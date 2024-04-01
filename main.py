import os,os.path
import sys ,time , datetime ,traceback ,json
import random,threading
import fire
import gevent
from gevent import monkey
from gevent.fileobject import FileObject
# monkey.patch_all()
from shared_mem import SharedDataManager
from shared_file import MappingFile

FILE = './A.DATA'

def createDataFile(file=FILE,size=100):
    fp = open(file,'wb')
    data = b'\0'*1024*1024
    for n in range(size):
        fp.write(data)
    fp.close()


def readingRandom(repeat,n=0):
    # print(gevent.getcurrent(),n)
    print(n)
    f_raw = open(FILE,'rb')
    # fp = FileObject(f_raw, 'rb')
    fp = f_raw
    fp.seek(0,2)
    size = fp.tell()
    for _ in range(repeat):
        fp.seek( random.randint(0,size-2000))
        data = fp.read(2000)
    fp.close()


def startFileReading( num = 10000 , repeat=10000):
    lets = []
    for n in range(num):
        # lets.append(gevent.spawn(readingRandom,repeat,n))
        t = threading.Thread(target=readingRandom,args=(repeat,n))
        t.setDaemon(True)
        t.start()
        lets.append(t)
    # gevent.joinall(lets)
    for t in lets:
        t.join()


def lockRead():
    # sdm = SharedDataManager().init(shm_key=0x11)    
    sdm = MappingFile().init()
    print("try to lock for reading..")
    sdm.rwlock.acquire_read()
    print("got lock ")
    try:
        time.sleep(5)
    except: pass 
    sdm.rwlock.release()

def lockWrite():
    # sdm = SharedDataManager().init(shm_key=0x11)    
    sdm = MappingFile().init()
    
    print("try to lock for writing ..")
    sdm.rwlock.acquire_write()
    print("got lock ")
    try:
        time.sleep(10)
    except: pass 
    sdm.rwlock.release()
    print(" lock free ..")
    # del sdm.rwlock


if __name__ == '__main__':
    fire.Fire()

'''

hexdump -C -n 120 A.DATA 
od -t x4 -N 10 A.DATA 
'''
# https://renatocunha.com/2015/11/ctypes-mmap-rwlock/