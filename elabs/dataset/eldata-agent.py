#coding:utf-8

"""
eldata-agent.py
行情服务策略终端服务，负责与中心管理交互，控制dataset-service的部署、运行、管理

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
import pymongo
import pytz
from collections import defaultdict
from elabs.fundamental.utils.useful import singleton,input_params
from elabs.app.core.controller import Controller
from elabs.app.core.behavior import Behavior
from elabs.app.core import logger
from elabs.app.core.registry_client import RegistryClient
from elabs.app.core.message import Tick,KLine,OrderBook
# from elabs.app.core.market_receiver import MarketReceiver
from elabs.utils.useful import Timer
from elabs.app.core.command import KlineUpdateReport
from elabs.ctp.instrinfo import _instrinfo as instrinfo

from data_receiver import MarketReceiver
from shared_file import DataSetBundle

_instr ={}
for k,v in instrinfo.items():
    _instr[k.upper()] = v
instrinfo = _instr

PWD = os.path.dirname(os.path.abspath(__file__))

@singleton
class DataAgentService(Behavior,cmd.Cmd):

    prompt = 'elData-agent > '
    intro = 'welcome to elabs..'
    def __init__(self):
        Behavior.__init__(self)
        cmd.Cmd.__init__(self )
        self.running = False
        self.conn = None
        self.timer = None
        self.kline_latest = {}  #
        self.dsbs = {}  # { name: dsb }

    def init(self,**kvs):
        Behavior.init(self,**kvs)

        RegistryClient().init(**kvs)
        
        interval = self.cfgs.get('keep_alive_interval',5)
        self.timer = Timer(self.keep_alive,interval)
        
        mx_addr_list = defaultdict(list)
        
        # init dataset 
        for ds in self.cfgs['dataset_list']:            
            dsb = DataSetBundle().init(data_dir= ds['data_dir'])
            for addr in ds['mx_addr']:
                mx_addr_list[addr].append( dict(topic=dsb.profile['topic'], dsb = dsb) )            
            self.dsbs [ dsb.profile['name']] = dsb
            dsb.open() 
            
        MarketReceiver().init(mx_addr_list = mx_addr_list).addUser(self)

        return self

    def keep_alive(self,**kvs):
        """定时上报状态信息"""
        Controller().keep_alive()
        for name,t  in self.kline_latest.items():
            m = KlineUpdateReport()
            m.from_service = self.cfgs.get('service_type')
            m.from_id = self.cfgs.get('service_id')
            fs = name.split('_')
            fs.append(t)
            m.datas.append(fs)
            Controller().send_message(m)

    def open(self):
        RegistryClient().open()
        MarketReceiver().open()
        return self

    def close(self):
        self.running = False
        # self.thread.join()

    def valid_kline(self,kline):
        if kline.exchange.upper() != 'CTP':
            return True
        time = str(datetime.datetime.fromtimestamp( kline.datetime /1000)).split('.')[0]
        (datestr, timestr) = time.split(" ")
        datenum = int(datestr.replace('-', ''))
        timenum = int(timestr.replace(':', ''))
        symbol = kline.symbol
        valid = False
        if instrinfo.get(symbol):
            timerule = instrinfo[symbol]['get_timerule'](datenum, timenum)
            for ps, pe in timerule['trading_period']:
                if ps <= timenum < pe:
                    valid = True
                    break
        return valid
    

    def onKline(self, kline: KLine):
        """ serialize into file and nosql db """

        if not self.valid_kline(kline):
            return
        logger.debug("KLogger onKline :", kline.marshall())


        self.into_file(kline)
        self.into_nosql( kline )

        name = f"{kline.exchange}_{kline.tt}_{kline.period}_{kline.symbol}"
        self.kline_latest[name] = int(datetime.datetime.now().timestamp())*1000
        self.timer.kick()

    def into_file(self,kline: KLine):
        """写入文件系统"""
        pass



    def do_exit(self,*args):
        Controller().close()
        print('bye bye!')
        return True

    def do_show(self,line):
        args = input_params(line,['pos'])
        if args:
            pass

    def signal_handler(signal, frame):
        sys.exit(0)

#------------------------------------------

def signal_handler(signal,frame):
    Controller().close()
    print('bye bye!')
    sys.exit(0)

#------------------------------------------
FN = os.path.join(PWD,  'settings.json')

def run(id = '',fn='',noprompt=False):
    global FN
    if fn:
        FN = fn
    params = json.loads(open(FN).read())
    if id:
        params['service_id'] = id

    Controller().init(**params).addBehavior("market",DataAgentService()).open()
    if noprompt:
        signal.signal(signal.SIGINT, signal_handler)
        print("")
        print("~~ Press Ctrl+C to kill .. ~~")
        while True:
            time.sleep(1)
    else:
        DataAgentService().cmdloop()


if __name__ == '__main__':
    run()
    # fire.Fire()