#coding:utf-8

import ctypes
from ctypes import *
import os, platform
import fire

libc = CDLL('librt.so')
librt = libc

if platform.system() == 'Linux':
    if platform.architecture()[0] == '64bit':
        pthread_rwlock_t = c_byte * 56
    elif platform.architecture()[0] == '32bit':
        pthread_rwlock_t = c_byte * 32
    else:
        pthread_rwlock_t = c_byte * 44
else:
    raise Exception("Unsupported operating system.")

# https://renatocunha.com/2015/11/ctypes-mmap-rwlock/
# https://docs.oracle.com/cd/E19253-01/819-7051/sync-50/index.html
# https://github.com/renatolfc/prwlock/blob/master/prwlock/prwlock.py
# https://www.halolinux.us/kernel-reference/spin-locks.html

PTHREAD_PROCESS_SHARED = 1
pthread_rwlockattr_t = c_byte * 8

#define __SIZEOF_PTHREAD_RWLOCKATTR_T 8

# let max size
pthread_rwlock_t = c_byte * 128
pthread_rwlockattr_t = c_byte * 32

pthread_rwlockattr_t_p = POINTER(pthread_rwlockattr_t)
pthread_rwlock_t_p = POINTER(pthread_rwlock_t)
API = [
    ('pthread_rwlock_destroy', [pthread_rwlock_t_p]),
    ('pthread_rwlock_init', [pthread_rwlock_t_p, pthread_rwlockattr_t_p]),
    ('pthread_rwlock_unlock', [pthread_rwlock_t_p]),
    ('pthread_rwlock_wrlock', [pthread_rwlock_t_p]),
    ('pthread_rwlockattr_destroy', [pthread_rwlockattr_t_p]),
    ('pthread_rwlockattr_init', [pthread_rwlockattr_t_p]),
    ('pthread_rwlockattr_setpshared', [pthread_rwlockattr_t_p, c_int]),
]

def error_check(result, func, arguments):
    name = func.__name__
    if result != 0:
        error = os.strerror(result)
        raise OSError(result, '{} failed {}'.format(name, error))


def augment_function(library, name, argtypes):
    function = getattr(library, name)
    function.argtypes = argtypes
    function.errcheck = error_check


# At the global level we add argument types and error checking to the
# functions:
for function, argtypes in API:
    augment_function(libc, function, argtypes)


class RWLock(object):
    RWLOCK_DATA_SIZE = ctypes.sizeof(pthread_rwlock_t) + ctypes.sizeof(pthread_rwlockattr_t)

    def __init__(self,buf,initLock=True,offset=0):
        # Define these guards so we know which attribution has failed
        lock, lockattr =  None, None
        
        # [ rwlock_t , rwlockattr_t ]       
        
        # for mmap                
        tmplock = pthread_rwlock_t.from_buffer(buf,offset)
        offset = offset + ctypes.sizeof(pthread_rwlock_t)
        tmplockattr = pthread_rwlockattr_t.from_buffer(buf, offset)
        
        # for shmget                
        # tmplock = pthread_rwlock_t.from_address(buf)
        # tmplockattr = pthread_rwlockattr_t.from_address(buf+offset)
        
        
        lock_p = ctypes.byref(tmplock)
        lockattr_p = ctypes.byref(tmplockattr)
    
        # Initialize the rwlock attributes and make it process shared
        lockattr = tmplockattr
        
        if initLock:
            librt.pthread_rwlockattr_init(lockattr_p)        
            librt.pthread_rwlockattr_setpshared(lockattr_p,PTHREAD_PROCESS_SHARED)
            # Initialize the rwlock
            librt.pthread_rwlock_init(lock_p, lockattr_p)
        
        lock = tmplock
    
        # Finally initialize this instance's members
        self._buf = buf
        self._lock = lock
        self._lock_p = lock_p
        self._lockattr = lockattr
        self._lockattr_p = lockattr_p
        self._init_lock = initLock

    def acquire_read(self):
        librt.pthread_rwlock_rdlock(self._lock_p)

    def acquire_write(self):
        librt.pthread_rwlock_wrlock(self._lock_p)

    def release(self):
        librt.pthread_rwlock_unlock(self._lock_p)

    def __del__(self):
        # return 
        # 负责初始化lock的完成清除lock
        if self._init_lock: 
            librt.pthread_rwlockattr_destroy(self._lockattr_p)
            self._lockattr, self._lockattr_p = None, None
            librt.pthread_rwlock_destroy(self._lock_p)
            self._lock, self._lock_p = None, None
            # print('rwlock __del__')
        # self._buf.close()
      
__all__ = (RWLock,)


def test():
    buf = create_string_buffer(256)
    print(buf ,buf.raw)
    print(RWLock.RWLOCK_DATA_SIZE,ctypes.sizeof(pthread_rwlock_t) )
    return
    rw = RWLock(buf)
    rw.acquire_write()
    rw.release()

if __name__ == '__main__':
    fire.Fire()
