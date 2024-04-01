#coding:utf-8

# 文件共享

"""
https://blog.csdn.net/AMDS123/article/details/80316781
https://github.com/omnisci/pymapd/blob/master/pymapd/ipc.py
https://www.cnblogs.com/52php/p/5861372.html
"""

import datetime 
import time 
from inspect import trace
import os,os.path
from  collections import OrderedDict
import datetime
import struct 
import json
from ctypes import *
import errno
import traceback
import copy
from dateutil.parser import parse
from elabs.dataset.core.basetype import DATAFILE_HEAD_SIZE
from elabs.dataset.core.shared_rwlock import RWLock
import mmap 
import fire 
import numpy as np
import pandas as pd 
from elabs.fundamental.utils.importutils import import_function
from elabs.dataset.core.basetype import * 

# IPC_CREAT = 512
# IPC_EXCL = 0x00002000
# IPC_RMID = 0
#
# # libc = CDLL("", use_errno=True, use_last_error=True)
# libc = CDLL('librt.so')
# # int shmget(key_t key, size_t size, int shmflg);
# shmget = libc.shmget
# shmget.restype = c_int
# shmget.argtypes = (c_int, c_size_t, c_int)
#
# # void* shmat(int shmid, const void *shmaddr, int shmflg);
# shmat = libc.shmat
# shmat.restype = c_void_p
# shmat.argtypes = (c_int, c_void_p, c_int)
#
# # int shmdt(const void *shmaddr);
# shmdt = libc.shmdt
# shmdt.restype = c_int
# shmdt.argtypes = (c_void_p,)
#
# # https://github.com/albertz/playground/blob/master/shared_mem.py
# # int shmctl(int shmid, int cmd, struct shmid_ds *buf);
# shmctl = libc.shmctl
# shmctl.restype = c_int
# shmctl.argtypes = (c_int, c_int, c_void_p)
#
# # void* memcpy( void *dest, const void *src, size_t count );
# memcpy = libc.memcpy
# memcpy.restype = c_void_p
# memcpy.argtypes = (c_void_p, c_void_p, c_size_t)
#
# # void *memset(void *s, int c, size_t n);
# memset = libc.memset
# memset.restype = c_void_p
# memset.argtypes = (c_void_p, c_void_p, c_size_t)


class DataSetBundle(object):
    def __init__(self):
        self.cfgs =  OrderedDict()
        self.shm_id = 0
        self.ptr = None
        self.all_size = 0
        self.start = None
        self.end = None
        # self.zones = [ None,None]
        # self.bucket:DataBucket = None
        self.used_space = 0
        self.exchange = ''
        self.tt = ''
        self.cols = {} # 
        self.handler = None
        self.profile = {}
        self.symbols = []
        # self.head = None

    def on_data(self,text):
        if not self.handler:
            self.handler = import_function(self.profile['handler'])
        try:
            symbol,ts,kvs = self.handler(text,self)
        except:
            traceback.print_exc()
            return 
        self.put_data(symbol,ts,**kvs)

    def get_period(self):
        return self.cfgs['period']
    
    def get_config(self,name):
        return self.cfgs.get(name)
    
    def get_symbols(self):
        dataset = self.cfgs['dataset']
        data_dir = os.path.join(self.cfgs['data_dir'] , dataset)
  
        symbols = []
        #lines = open(os.path.join( data_dir, f"symbol_{dataset}.txt")).readlines()
        lines = open(os.path.join( data_dir, "symbol_%s.txt"%dataset )).readlines()
        lines = [line.strip() for line in lines]
        lines = filter(lambda s: len(s) and s[0] != '#', lines)
        symbols = list(lines)
        return symbols

    def init(self,**kwargs):
        # https://www.ibm.com/docs/en/i/7.4?topic=functions-fopen-open-files
        
        self.cfgs.update(**kwargs)
        symbols = self.cfgs.get('symbols', [])
        dataset = self.cfgs.get('dataset')
        data_dir = self.cfgs.get('data_dir','./')
        access_mode = self.cfgs.get('access','read')
        init_lock = self.cfgs.get('init_lock',False)
        lock_enable = self.cfgs.get('lock_enable',False)
        data_dir = os.path.join( data_dir, dataset)
        
        self.lock_enable = lock_enable
        
        # 未指定symbols，则扫描目录下的文件
        if not symbols: 
            # print('data_dir:' , data_dir)
            # for fn in os.listdir( data_dir ):
            #     fp = open(os.path.join( data_dir ,fn ) , 'rb')
            #     magic = fp.read(len(MAGIC))
            #     if magic == MAGIC:                    
            #         symbols.append ( fn.split('.')[0].upper() )
            #     fp.close()
            symbols = self.get_symbols()
            
        self.symbols = symbols 
        
        fn = os.path.join(data_dir,'profile.json')
        text = open( fn ).read()
        data =  json.loads(text)
        data['children'].insert(0,{"name":"TS","type":"int"})
        
        self.profile = data 
        self.mmaps = {}
        
        start = self.profile['date_range'][0]
        end = self.profile['date_range'][1]
        start = parse(start)
        end = parse(end)
        period = data['period'].upper()
        period,type_ = period[:-1],period[-1]
        bitwide = data['bitwide']
        days = (end - start).days
        
        self.start = start.timestamp()
        self.end = end.timestamp()
        
        num = 0 
        if type_ == 'M':
            minutes = days * MinutesPerDay
            num = int(minutes / int(period))
        
        # offset = len(MAGIC) + RWLock.RWLOCK_DATA_SIZE
        offset = DATAFILE_HEAD_SIZE
        for n,child in enumerate( self.profile['children'] ):
            # self.cols[child['name']] = dict(index=n , offset = offset,type=child.get('type','float')) 
            self.cols[child['name']] = (n , offset,child.get('type','float') )
            offset += num * ValueBytes
            
        for symbol in symbols:
            fn = os.path.join(data_dir,"%s.dat"%symbol)
            fileno = os.open(fn,os.O_RDWR | os.O_SYNC )
            mptr = mmap.mmap( fileno,0)
            self.mmaps[symbol] = mptr # dict(mptr = mptr,  profile= data )
            
            # print(fn,mptr[:len(MAGIC)])

        initLock =  False 
        self.rwlock = None

        if init_lock:
            initLock = True                                 
            
        self.rwlock = RWLock( mptr,initLock=initLock ,offset=len(MAGIC))           

        return self 
        
    def get_data(self,symbol,cols=[],start=None,end=None,num=1, df = True):
        data = {}
            
        if self.lock_enable:
            self.rwlock.acquire_read()
        try:    
            
            data = self._get_data(symbol,cols,start,end,num)
            if 'TS' in data:
                data['DT'] = [ datetime.datetime.fromtimestamp(x) for x in data['TS'] ]
        except:
            traceback.print_exc()
        if self.lock_enable:
            self.rwlock.release()
            
        if df : 
            if data:   
                data = pd.DataFrame( data  )                              
                data = data[~(data['TS']==0)]       
                data.set_index('DT',inplace=True )  
            else:
                data = pd.DataFrame()        
        return data 
        
    def _get_data(self,symbol,cols=[],start=None,end=None,num=1):
        ret = {}
        mptr = self.mmaps.get(symbol)
        if not mptr:
            return ret 
        
        if isinstance(start, str):
            start = parse(start)
        if isinstance(start, datetime.datetime):
            start = start.timestamp()
            
        if start :
            if  start < self.start : 
                start = self.start                 
            elif start >= self.end:
                return ret
            
        if isinstance(end, str):
            end = parse(end)
        if isinstance(end, datetime.datetime):
            end = end.timestamp()
            
        if end :
            if  end < self.start : 
                end = self.start                 
            elif end >= self.end:
                end = self.end 
            
        if not cols:
            cols = list(self.cols.keys())
        cols.insert(0,'TS')
        
        if not start and not end and num:  
            # 仅仅指定数量num , 默认从最近latest记录的数据向后
            mptr.seek( FIELD_LATEST_OFFSET)            
            ts_latest = struct.unpack('<Q',mptr.read(8))[0] # 最近写入数据时间            
            ts = datetime.datetime.now().timestamp() 
            if ts > ts_latest:
                ts = ts_latest
                
            offset = self.get_offset(ts)
                        
            for name in cols:
                if name in self.cols:
                    index,start,type_ = self.cols[name] 
                    from_ = start + (offset - num ) * ValueBytes
                    to_ = from_ + (num+1) * ValueBytes 
                    buf = mptr[from_:to_]
                    if name == 'TS':
                        ar = np.frombuffer( buf, dtype=np.dtype(np.uint64).newbyteorder('<') )                        
                    else:
                        ar = np.frombuffer( buf, dtype=np.dtype(np.float64).newbyteorder('<') )
                    ret[name] = ar[-num:]
            
            return ret 
        
        if start :
            offset_start = self.get_offset( start )
            offset_end = offset_start
            
            if num:
                offset_end = offset_start + num 
            
            if end:
                offset_end = self.get_offset( end )
            
            for name in cols:
                if name in self.cols:
                    index,start,type_ = self.cols[name] 
                    from_ = start + offset_start * ValueBytes
                    to_ = start + offset_end * ValueBytes 
                    buf = mptr[from_:to_]
                    if name == 'TS':
                        ar = np.frombuffer( buf, dtype=np.dtype(np.uint64).newbyteorder('<') )
                    else:
                        ar = np.frombuffer( buf, dtype=np.dtype(np.float64).newbyteorder('<') )
                    ret[name] = ar    
            
            return ret 
        
        if end:
            offset_end = self.get_offset( end )
            if num :
                offset_start = offset_end - num 
            if start:
                offset_start = self.get_offset( start )
            
            for name in cols:
                if name in self.cols:
                    index,start,type_ = self.cols[name] 
                    from_ = start + offset_start * ValueBytes
                    to_ = start + offset_end * ValueBytes 
                    buf = mptr[from_:to_]
                    if name == 'TS':
                        ar = np.frombuffer( buf, dtype=np.dtype(np.uint64).newbyteorder('<') )
                    else:
                        ar = np.frombuffer( buf, dtype=np.dtype(np.float64).newbyteorder('<') )
                    ret[name] = ar
            return ret 
            
        
    def get_offset(self,ts):
        elapsed = int(ts) - self.start
        period = self.profile['period'].upper()
        n,unit = period[:-1],period[-1]            
        seconds = 1        
        if unit == 'M':
            seconds =  int(n) * 60
        offset = int( elapsed / seconds )
        return offset 

    def get_symbol_latest(self,symbol):
        """指定合约数据最近的时间"""
        ts = 0 
        if self.lock_enable:
            self.rwlock.acquire_write()
        try:
            mptr = self.mmaps.get(symbol)
            if mptr:
                mptr.seek( FIELD_LATEST_OFFSET)            
                ts = struct.unpack('<Q',mptr.read(8))[0]            
        except:
            traceback.print_exc()
        if self.lock_enable:
            self.rwlock.release()
        return ts 
            
    def put_data(self ,symbol,ts,  **kvs ):
        if self.lock_enable:
            self.rwlock.acquire_write()
        try:
            self._put_data(symbol,ts,**kvs)
        except:
            traceback.print_exc()
        if self.lock_enable:
            self.rwlock.release()
        
    def _put_data(self ,symbol,ts,  **kvs ):
        if isinstance(ts, str):
            ts = parse(ts)
        if isinstance(ts, datetime.datetime):
            ts = ts.timestamp()
            
        if  ts < self.start or  ts >= self.end:
            return  
        
        mptr = self.mmaps.get(symbol)
        if not mptr:
            return 
        
        offset = self.get_offset(ts)
        
        kvs['TS'] = ts 
        
        for k,v in kvs.items():            
            if k in self.cols:
                index,start ,type_ = self.cols[k]
                addr = start + offset * 8
                mptr.seek(addr)
                if type_ == 'int':                    
                    mptr.write(struct.pack("<Q", int(v)))
                    # mptr[addr: addr+ValueBytes] = struct.pack("<Q", int(v))
                else:    
                    mptr.write(struct.pack("<d", float(v)))
                    # mptr[addr: addr+ValueBytes] = struct.pack("<d", float(v))
        #记录最近一次行情时间,保持最长的数据时间
        if ts:
            mptr.seek( FIELD_LATEST_OFFSET )
            ts_last = struct.unpack('<Q',mptr.read(8))[0]
            if ts > ts_last:
                mptr.seek( FIELD_LATEST_OFFSET )
                mptr.write( struct.pack("<Q",int(ts)))


def for_write(symbols=[]):  
    DataSetBundle().init(dataset='ohlcv',access='write',symbols=symbols)

def for_read():  
    DataSetBundle().init(dataset='ohlcv',access='read')


if __name__ == '__main__':
    fire.Fire()

