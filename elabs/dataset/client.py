#coding:utf-8

import datetime , time 
import json
import sys,os,os.path
import traceback
from multiprocessing import Process
import fire

import numpy as np
from dateutil.parser import parse

# from elabs.app.core import kseeker
PWD = os.path.dirname(os.path.abspath(__file__))
FN = os.path.join( os.path.dirname( os.path.abspath(__file__) ) , 'dataset.json')
cfgs = json.loads( open(FN).read())


from elabs.dataset.core.shared_rwlock import RWLock
from elabs.dataset.core.shared_file2 import DataSetBundle
from elabs.dataset.core.basetype import MAGIC , RWLOCK_DATA_SIZE , DATAFILE_HEAD_SIZE
from elabs.dataset.dataset import pull_data , get_symbols
from elabs.utils.useful import flock_ex

def client(dataset):
    # 检测  dataset-serive 是否运行 ，未启动则返回错误
    if  flock_ex( os.path.join(PWD , "dataset-%s.lock"%dataset) ):
        #print(f"dataset service <{dataset}> not running..")
        print( "dataset service %s not running.."%dataset)
        return None 
    
    data_dir = cfgs['data_dir']
    c = cfgs['datasets'][dataset]
    fn = os.path.join( PWD, c['profile'] )
    profile = json.loads(open(fn).read()) 
    dsb = DataSetBundle().init( data_dir = data_dir, symbols = [], dataset= dataset,init_lock = False , lock_enable = False  )
    return dsb


def test():
    dsb = client('ohlcv')
    if dsb:
        df = dsb.get_data('AAVEUSDT',['DT','O','H','L','C'], num = 5 )
        df = dsb.get_data('AAVEUSDT', num = 150 )
        # df = dsb.get_data('AAVEUSDT',['DT','O','H','L','C'], start='2021-12-1',num = 50 )
        # df = dsb.get_data('AAVEUSDT',['DT','O','H','L','C'], start='2021-12-1', end='2021-12-20' )
        print(df)
        
        df.to_csv('test.csv')
        # return df 
        
def test_perf():
    dsb = client('ohlcv')
    if dsb:
        for _ in range(1000):
            print(_)
            df = dsb.get_data('AAVEUSDT',['DT','O','H','L','C'], start='2021-1-1', end='2022-12-20' )
    
if __name__ == '__main__':
    fire.Fire()