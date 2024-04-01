#coding:utf-8

import datetime , time 
import json
import sys,os,os.path
import traceback
from multiprocessing import Process
import fire
import pymongo
import numpy as np
from dateutil.parser import parse
from sympy import symbols
from typing  import List
from elabs.utils.useful import open_file
from elabs.utils.concurrency import multiprocess_task_split

from elabs.app.core import kseeker
PWD = os.path.dirname(os.path.abspath(__file__))
FN = os.path.join( os.path.dirname( os.path.abspath(__file__) ) , 'dataset.json')
cfgs = json.loads( open(FN).read())


from elabs.dataset.core.shared_rwlock import RWLock
from elabs.dataset.core.shared_file2 import DataSetBundle
from elabs.dataset.core.basetype import MAGIC , RWLOCK_DATA_SIZE , DATAFILE_HEAD_SIZE

# 创建定长行情数据文件
def init_file(name, symbols:List[str]=[]):
    data_dir = os.path.join(cfgs['data_dir'],name)
    c = cfgs['datasets'][name]
    if isinstance(symbols,str):
        symbols = symbols.split(',')
    if not symbols:
        lines = open( os.path.join( data_dir,c['symbols'])).readlines()
        lines = [ line.strip() for line in lines] 
        lines = filter(lambda s: len(s) and s[0] !='#', lines)
        symbols = list(lines)
    
    profile = c['profile']

    text = open( os.path.join(PWD,profile)).read()
    data =  json.loads(text)
    data['children'].insert(0,{"name":"TS","type":"int"})
    
    # name = os.path.join(PWD,data_dir,f"{data['name']}" )
    
    start,end = data['date_range']
    start = parse(start)
    end = parse(end)
    period = data['period'].upper()
    period,type_ = period[:-1],period[-1]
    bits = data['bitwide']
    days = (end - start).days
    num = 0 
    if type_ == 'M':
        minutes = days * 24 * 60 
        num = int(minutes / int(period))
    
    # if not os.path.exists( name ):
    #     os.makedirs( name )    
    
    with open(os.path.join(data_dir,'profile.json'),'w') as f :
        f.write( text )
    
    for symbol in symbols:
        symbol = symbol.upper()
        filename = os.path.join( data_dir , "%s.dat"%symbol )
        print('new data file:' , filename )
        # filesize = len(MAGIC) + RWLock.RWLOCK_DATA_SIZE +  int(bits/8) * num 
        filesize = DATAFILE_HEAD_SIZE + int(bits/8) * num 
        fp = open(filename,'wb')
        fp.write(MAGIC )
        fp.write(b'\0'* (DATAFILE_HEAD_SIZE - len(MAGIC) ))
        
        for n,child in enumerate(data['children']):
            newsize = int(bits/8) * num 
            while newsize > 0 :
                size = min(newsize, 1024*1024)
                fp.write(b'\0'*size)
                newsize -= size 
        fp.close()
        
    print('init okay !')
        

def db_conn():
    conn = pymongo.MongoClient(**cfgs.get('mongodb'))
    return conn

def list_symbols( dataset_name ):
    symbols = []
    dbname = cfgs['datasets'].get(dataset_name).get('db')
    if not dbname:
        return symbols
    conn = db_conn( )
    db = conn[dbname]
    
    names = db.list_collection_names()
    
    for name in sorted(names):
        symbol = name.split('_')[-1]
        symbols.append(symbol)
        # print(symbol)
    return symbols


def create_symbol_index(dataset_name):
    symbols = []
    dbname = cfgs['datasets'].get(dataset_name).get('db')
    if not dbname:
        return symbols
    conn = db_conn()
    db = conn[dbname]

    names = db.list_collection_names()

    for name in sorted(names):
        coll = db[name]
        print('indexing ',name,' ..')
        coll.create_index([('TS', 1), ('DT', 1)])
        coll.create_index([('DT', 1)])
        coll.create_index([('TS', 1)])



def init_dataset( dataset ):
    name = dataset 
    data_dir = os.path.join( cfgs['data_dir'] , dataset)
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    c = cfgs['datasets'][name]
    symbols = list_symbols(name)
    fp = open( os.path.join( data_dir,c['symbols']),'w')
    
    for symbol in symbols:
        fp.write(symbol)
        fp.write('\n')
    fp.close()
    
    

def pull_data( dataset, symbols=[] ,start='',end='' ,init_lock=False, lock_enable=False ):
    """
        更新本地行情数据集    
        symbols: 默认使用 symbol_ohlcv.txt 定义的合约清单 ，也可更新指定的合约
        start: 默认采用 ohlcv_profile.json 定义的时间 ，也可指定更新时间范围
        end :
        init_lock : 重置lock初始变量区
        lock_enable: 是否启用读写互斥
    """
    if isinstance(symbols,str):
        symbols = symbols.split(',')
    print("> pull_data: ",dataset,symbols,start,end , init_lock , lock_enable )
    
    data_dir = cfgs['data_dir']
    c = cfgs['datasets'][dataset]
    
    # if not symbols:
    #     lines = open( os.path.join(PWD, data_dir,c['symbols'])).readlines()
    #     lines = [ line.strip() for line in lines] 
    #     lines = filter(lambda s: len(s) and s[0] !='#', lines)
    #     symbols = list(lines)
    fn = os.path.join( PWD, c['profile'] )
    profile = json.loads(open(fn).read())
    
    
    if not start:
        start = profile['date_range'][0]
    if not end:
        end = profile['date_range'][1]
    if isinstance(start,str):
        start = parse(start)
    if isinstance( end , str) :
        end = parse(end )
    if isinstance(start, (int,float)):
        start = datetime.datetime.fromtimestamp(start)
    if isinstance(end, (int,float) ):
        end = datetime.datetime.fromtimestamp(end)
    range_date = parse(profile['date_range'][0]), parse(profile['date_range'][1])
    # 限制 start ,end 必须在数据集有效范围内
    if start < range_date[0]:
        start = range_date[0]
    if end > range_date[1]:
        end = range_date[1]
    
    
    conn = db_conn()
    db = conn[ c['db'] ]
    dsb = DataSetBundle().init( data_dir = data_dir, symbols = symbols, dataset= dataset,init_lock = init_lock , lock_enable = lock_enable  )
    if not symbols:
        # symbols = dsb.symbols
        symbols = get_symbols(dataset)

    for symbol in symbols:
        print("Pulling %s .."%symbol)
        
        prefix = c.get('collection_prefix','')
        name = "%s%s"%(prefix,symbol)
        coll = db[name]
        rs = coll.find({'DT':{'$gte':start,'$lt':end}},{'_id':False}).sort('DT',1)
        # fs = ['DT','TS'] + profile['children'].keys()
        latest = 0
        count = 0
        for data in list(rs):
            # print("write data:", symbol, data['TS'] , data)
            dsb.put_data( symbol ,data['TS'],**data )
            count +=1
            
        #print(" >> Pulled {symbol}: {count} Records  Finished!")
        print(" >> Pulled %s: %s Records  Finished!"%(symbol,count))
            

def get_symbols(dataset):
    data_dir = os.path.join(cfgs['data_dir'] , dataset)
    c = cfgs['datasets'][dataset]
    symbols = []
    lines = open(os.path.join( data_dir, c['symbols'])).readlines()
    lines = [line.strip() for line in lines]
    lines = filter(lambda s: len(s) and s[0] != '#', lines)
    symbols = list(lines)
    return symbols

def list_symbols_diff(dataset):
    """查询db中新增的合约"""
    ss = list_symbols(dataset)
    tt = get_symbols(dataset)
    symbols = []
    for s in ss:
        if s not in tt:
            symbols.append(s)
    return ','.join(symbols)


def pull_data_par(dataset, symbols=[] ,start='',end='',workers=1):
    """并行加载数据到本地文件"""
    
    if isinstance(symbols,str):
        symbols = symbols.split(',')
        
    if workers == 1:
        return pull_data( dataset ,symbols , start ,end )

    data_dir = cfgs['data_dir']
    c = cfgs['datasets'][dataset]
    symbols = []
    lines = open(os.path.join( data_dir, dataset, c['symbols'])).readlines()
    lines = [line.strip() for line in lines]
    lines = filter(lambda s: len(s) and s[0] != '#', lines)
    symbols = list(lines)
    groups = np.array_split(symbols, workers)
    processes = []
    # print(groups)

    for group in groups:
        symbols = list(group)
        if not symbols:
            continue 
        p = Process(target= pull_data,args=(dataset , symbols ,start,end))
        processes.append(p)
        p.start()
    for p in processes:
        p.join()


if __name__ == '__main__':
    fire.Fire()
