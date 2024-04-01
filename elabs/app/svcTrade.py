#coding:utf-8

import os
import json
import threading
import time
import datetime
import traceback
from threading import Thread,Lock
import signal
import sys
import fire
import cmd

from elabs.fundamental.utils.useful import input_params
from elabs.fundamental.utils.useful import singleton,input_params
from elabs.app.core.controller import Controller
from elabs.app.core.behavior import Behavior
from elabs.app.core import logger
from elabs.app.core.registry_client import RegistryClient
from elabs.app.core.tradecmd import TradeCmd
from elabs.app.core.position_receiver import PosReceiver
from elabs.app.core.market_receiver import MarketReceiver
from elabs.app.core.message import Tick,KLine,OrderBook
from elabs.app.core.command import PositionSignal
from elabs.app.core.base import TradeBase
from elabs.fundamental.utils.importutils import import_class
from elabs.utils.useful import Timer

PWD = os.path.dirname(os.path.abspath(__file__))

@singleton
class TradeInstance(Behavior,cmd.Cmd):

  prompt = 'Trade > '
  intro = 'welcome to elabs..'
  def __init__(self):
    Behavior.__init__(self)
    cmd.Cmd.__init__(self )
    # TradeCmd.__init__(self)

    self.running = True
    self.trade_impl:TradeBase = None
    self.pos_cache_list = {}  # { exchange_account_tt_symbol: [pos,20211201 10:01] }
    self.timer = None
    self.thread = None
    self.timer_actions = {}

  def init(self,**kvs):
    Behavior.init(self,**kvs)
    RegistryClient().init(**kvs)
    PosReceiver().init(**kvs).addUser(self)
    MarketReceiver().init(**kvs).addUser(self)

    impl_cls = import_class( self.cfgs.get('class'))
    self.trade_impl = impl_cls()
    self.trade_impl.init(**kvs)

    self.timer = Timer(self.position_cache_save,self.cfgs.get('position_cache_interval',5))
    self.init_position()
    self.thread = threading.Thread(target=self.workTimer)
    self.thread.daemon = True

    for ta  in self.cfgs.get('timer_actions',[]):
      name,interval = ta
      timer = Timer(self.onTimerAction,interval,name=name)
      self.timer_actions[name] = timer

  def onTimerAction(self,**kwargs):
    self.trade_impl.onTimer(kwargs.get('name'))

  def init_position(self):
    fn = self.cfgs.get('position_cache_file')
    if not os.path.exists(fn):
      return

    f = open(fn, 'r')
    lines = f.readlines()
    lines = list(map(lambda x:x.strip(),lines))
    lines = list(filter(lambda x:x,lines))
    latest = []
    for line in lines:
      pos = PositionSignal()
      fs = line.split(',')
      k = fs[0]
      pos.exchange,pos.account,pos.tt,pos.symbol = k.split('_')
      pos.pos = int(fs[1])
      pos.timestamp = int(fs[2])
      self.pos_cache_list[ k ] = pos
      latest.append(pos)
    f.close()
    self.trade_impl.init_pos(latest)

  def open(self):
    RegistryClient().open()
    PosReceiver().open()
    MarketReceiver().open()
    self.trade_impl.open()

    self.thread.start()
    return self

  def workTimer(self):
    self.running = True
    while self.running:
      time.sleep(.1)
      for name,timer in self.timer_actions.items():
        try:
          timer.kick()
        except:
          traceback.print_exc()

  def close(self):
    self.trade_impl.close()
    self.running = False
    self.thread.join()

  def onTick(self, tick: Tick):
    """行情服务转发来的分时记录"""
    self.trade_impl.onTick(tick)

  def onKline(self, kline: KLine):
    self.trade_impl.onKline(kline)

  def onOrderBook(self, orderbook: OrderBook):
    self.trade_impl.onOrderBook(orderbook)

  def onPositionSignal(self, pos: PositionSignal):
    ## 过滤仓位信号，
    ## 持久化仓位信号
    ## 在初始化 trade_impl时加载历史仓位送入
    if self.cfgs.get('exchange'):
      if pos.exchange != self.cfgs.get('exchange'):
        logger.info(f"exchange unmatched! {pos.exchange} , {self.cfgs.get('exchange')}")
        return
    if self.cfgs.get('account'):
      if pos.account != self.cfgs.get('account'):
        logger.info(f"account unmatched! {pos.account} , {self.cfgs.get('account')}")
        return
    k = f"{pos.exchange}_{pos.account}_{pos.tt}_{pos.symbol}"
    with self.lock:
      self.pos_cache_list[k] = pos
    self.timer.kick() # 触发保存一下

    self.trade_impl.onPositionSignal(pos) # 收到发送到达的仓位信号转发给 报单服务模块


  def position_cache_save(self):
    f = open( self.cfgs.get('position_cache_file'),'w')
    with self.lock:
      for k,pos in self.pos_cache_list.items():
        v = f"{k},{pos.pos},{str(pos.timestamp)}"
        f.write(v)
        f.write("\n")
    f.flush()

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

def signal_handler(signal,frame):
  Controller().close()
  print('bye bye!')
  sys.exit(0)

#------------------------------------------
FN = os.path.join(PWD,  'trade.json')
FN = 'trade.json'

def run(id='',fn='',noprompt=False):
  global FN
  if fn:
    FN = fn

  if FN[0]!='/':
    FN = os.path.join(PWD,FN)

  params = json.loads(open(FN).read())
  if id:
    params['service_id'] = id

  Controller().init(**params).addBehavior("trade",TradeInstance()).open()
  if noprompt:
    signal.signal(signal.SIGINT, signal_handler)
    print("")
    print("~~ Press Ctrl+C to kill .. ~~")
    while True:
      time.sleep(1)
  else:
    TradeInstance().cmdloop()


if __name__ == '__main__':
  # run()
  fire.Fire()