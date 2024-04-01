#coding:utf-8


"""
klinecache.py
继承到Market服务的kline持久化部件，在行情接入侧将kline数据缓存到本地文件，默认保持2天的line记录
marketmate服务将读取缓存的kline记录

配置项:
 - kline_cache_days = 2
 - kline_cache_dir ='./kline'   默认存储目录
 - kline_max_size = 200   一条kline最大长度
 - kline_cache_check_timeout = 5   一条kline最大长度

存储路径:
  yyyy-mm-dd/exchange/tt/symbol/hour

  2021-12-01 / ftx / swap / AXESusdt.dat
  2021-12-01 / ftx / swap / BTCusdt.dat

存储格式：
  每天每个symbol的kline记录被编排到不同文件中，为了加速访问或更新这些kline，采用预分配固定空间的方法实现。
  规定：
    每条kline长度 kline_max_size 字节，每个交易对 日占用固定空间 24* 60 * kline_max_size = sizeof(BTCusdt.dat)

"""
import os
import json
import time
import datetime
from threading import Thread
import signal
import sys
import fire
import cmd
from dateutil.parser import parse
from elabs.fundamental.utils.importutils import import_class
from elabs.fundamental.utils.useful import singleton,input_params,open_file
from elabs.fundamental.utils.timeutils import utctime2local
from elabs.app.core import logger
from elabs.app.core.message import Tick,KLine,OrderBook
from elabs.utils.useful import Timer

@singleton
class KlineLocalCache(object):
    def __init__(self):
        self.cfgs = {}
        self.symbol_fd = {}    # {symbol:[fd,create_time)]}
        self.symbol_file_size = 0
        self.kline_max_size = 0
        self.chk_timer:Timer = None

    def init(self,**kvs):
        self.cfgs.update(**kvs)
        self.kline_max_size = self.cfgs.get('kline_max_size',200)
        self.symbol_file_size =  self.kline_max_size * 24 * 60
        self.chk_timer = Timer(self.check,self.cfgs.get('kline_cache_check_timeout',5))

        return self

    def check(self):
        # 检查释放 超过一天的文件fd
        removes = []
        now = datetime.datetime.now()
        for s,v in self.symbol_fd.items():
            if (now - v[1]).days > 0 :
                removes.append(s)
        for r in removes:
            self.symbol_fd[r][0].close()
            del self.symbol_fd[r]

    def open(self):
        return self

    def close(self):
        pass

    def write(self,kline:KLine):
        if not self.cfgs.get('kline_cache_enable',0):
            return
        self.chk_timer.kick()

        path = self.cfgs.get('kline_cache_dir','./kline')
        now = datetime.datetime.fromtimestamp(kline.datetime/1000)
        # now  = utctime2local(now )
        tm_offset = now.hour * 60 + now.minute
        text = kline.marshall()

        # 固定长度填充补齐
        pad = self.kline_max_size - len(text)
        if pad > 0 :
            text = text + ' ' * pad
        text = text.encode()[:self.kline_max_size]

        data_offset = tm_offset * self.kline_max_size

        dn = str(now).split(' ')[0]
        fn = os.path.join(path,dn,kline.exchange,kline.tt,kline.symbol.replace('/','-')) +'.dat'
        fd = None
        if fn not in self.symbol_fd:
            init_content = False
            if not os.path.exists(fn):
                init_content = True
                fd = open_file(fn, 'wb')
            else:
                if os.stat(fn).st_size != self.symbol_file_size:
                    fd = open_file(fn,'wb')
                    init_content = True
                else:
                    fd = open_file(fn,'wb+')
            self.symbol_fd[fn] = (fd,datetime.datetime.now())

            if init_content:
                fd.write(b" "* self.symbol_file_size ) # 预写
                fd.flush()
                fd.seek(0,0)
        else:
            fd = self.symbol_fd[fn][0]
        fd.seek(data_offset,0)  # 跳跃到时间刻度
        fd.write(text)
        fd.flush()

    def read(self,exchange,tt,period,symbol,start,end):
        """从本地缓存文件中读取历史kline ， 此功能部署在 market mate 服务侧"""
        if isinstance(start,str):
            start = parse(start)
        if isinstance(end ,str):
            end = parse(end)
        path = self.cfgs.get('kline_cache_dir', './kline')
        symbol = symbol.replace('/','-')
        result =[]
        fnfd ={} # { fn:fd}
        while start < end:
            tm_offset = start.hour * 60 + start.minute
            data_offset = tm_offset * self.kline_max_size

            dn = str(start).split(' ')[0]
            fn = os.path.join(path, dn, exchange, tt, symbol) + '.dat'
            fd = None
            fd = fnfd.get(fn)
            if not fd:
                if not os.path.exists(fn):
                    start = start + datetime.timedelta(minutes=1)
                    continue
                fd = open(fn,'rb')
                fnfd[fn] = fd
            fd.seek(data_offset,0)
            bytes = fd.read(self.kline_max_size)
            text = bytes.decode().strip()
            if text:
                result.append(text)

            start = start + datetime.timedelta(minutes=1)
        for _,fd in fnfd.items():
            fd.close()
        return result


def t_read():
    kc = KlineLocalCache()
    kvs = {"kline_cache_days": 2,
          "kline_cache_dir": "./kline",
          "kline_max_size":200,
          "kline_cache_check_timeout": 5
           }
    kc.init(**kvs).open()
    rs = kc.read("ftx","spot",1,"btc/usdt",'2021-12-13 12:13','2021-12-13 12:55')
    return rs

if __name__ == '__main__':
    fire.Fire()

