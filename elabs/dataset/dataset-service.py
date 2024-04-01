
"""
dataset-service.py 
数据集提供数据更新
"""

import datetime , time 
import json
import sys,os,os.path
import traceback
from multiprocessing import Process
import fire
import pymongo
import numpy as np
from dateutil.parser import parse
from elabs.utils.useful import open_file

from elabs.app.core import kseeker
PWD = os.path.dirname(os.path.abspath(__file__))
FN = os.path.join( os.path.dirname( os.path.abspath(__file__) ) , 'dataset.json')
cfgs = json.loads( open(FN).read())


from elabs.dataset.core.shared_rwlock import RWLock
from elabs.dataset.core.shared_file2 import DataSetBundle
from elabs.dataset.core.basetype import MAGIC , RWLOCK_DATA_SIZE , DATAFILE_HEAD_SIZE
from elabs.dataset.dataset import pull_data , get_symbols
from elabs.dataset.data_receiver import MarketReceiver 
from elabs.utils.useful import flock_ex 

def db_conn():
    conn = pymongo.MongoClient(**cfgs.get('mongodb'))
    return conn

def data_padding(dataset):
    """填充空缺的行情数据"""
    data_dir = cfgs['data_dir']
    c = cfgs['datasets'][dataset]
    fn = os.path.join( data_dir, dataset, 'profile.json' )
    profile = json.loads(open(fn).read()) 
    now = int( datetime.datetime.now().timestamp() ) + 1
    range_date = parse(profile['date_range'][0]), parse(profile['date_range'][1])
    conn = db_conn()
    db = conn[ c['db'] ]
    symbols = get_symbols(dataset)
    
    dsb = DataSetBundle().init( data_dir = data_dir, symbols = [], dataset= dataset,init_lock = False , lock_enable = False  )
    
    start = range_date[0].timestamp()
    end = range_date[1].timestamp()
    
    for symbol in symbols:
        
        ts = dsb.get_symbol_latest(symbol)
        print(symbol, ts , datetime.datetime.fromtimestamp(ts) , now)
        if ts == 0: 
            raise "Dataset is Empty!"
        if start <= now < end:
            if now > ts :
                print('padding :' , now - ts )
                pull_data(dataset,[symbol],ts,now )
                
    

def run(dataset):
    lock = flock_ex( os.path.join(PWD , f"dataset-{dataset}.lock") )
    if not lock:
        print(f"same dataset service <{dataset}> is running..")
        return None 
    
    fn = os.path.join(PWD,f"dataset-{dataset}.pid")
    fp = open(fn,'w')
    fp.write( str( os.getpid() ))
    fp.close()
    
    data_dir = cfgs['data_dir']
    c = cfgs['datasets'][dataset]
    fn = os.path.join( data_dir, dataset,'profile.json' )
    profile = json.loads(open(fn).read()) 
    dsb = DataSetBundle().init( data_dir = data_dir, symbols = [], dataset= dataset,init_lock = True , lock_enable = True  )

    data_padding(dataset) 
    mx_addr_list = c['mx_addr']
    topic = c['topic']
    receiver = MarketReceiver().init(mx_addr_list = mx_addr_list ,topic = topic ).addUser(dsb)
    print("Service: <%s> started.."%dataset)
    receiver.run()

if __name__ == '__main__':
    fire.Fire()