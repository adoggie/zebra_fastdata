#coding:utf-8

import os
import json
import time
import datetime
from threading import Thread
import signal
import sys
import fire
import cmd

from elabs.fundamental.utils.importutils import import_class
from elabs.fundamental.utils.useful import singleton,input_params
from elabs.app.core.controller import Controller
from elabs.app.core.behavior import Behavior
from elabs.app.core import logger
from elabs.app.core.registry_client import RegistryClient
from elabs.app.core.position_receiver import PosReceiver
from elabs.app.core.tradecmd import TradeCmd
from elabs.app.core.market_publish import MarketPublisher
from elabs.app.core.position_receiver import PosReceiver
from elabs.app.core.message import Tick,KLine,OrderBook
from elabs.app.core.command import PositionSignal,ServiceKeepAlive,KlineAttach,KlinePull
from elabs.app.core.base import MarketBase
from elabs.app.core.klinecache import KlineLocalCache

PWD = os.path.dirname(os.path.abspath(__file__))

@singleton
class MarketInstance(Behavior,cmd.Cmd):

  prompt = 'Market > '
  intro = 'welcome to elabs..'
  def __init__(self):
    Behavior.__init__(self)
    cmd.Cmd.__init__(self )
    # TradeCmd.__init__(self)
    self.running = False
    self.market_impl:MarketBase = None
    self.master_keepalive_time = datetime.datetime.now()

  def init(self,**kvs):
    Behavior.init(self,**kvs)
    MarketPublisher().init(**kvs)
    RegistryClient().init(**kvs).addUser(self)
    KlineLocalCache().init(**kvs)

    impl_cls = import_class(self.cfgs.get('class'))
    self.market_impl = impl_cls()
    self.market_impl.init(**kvs)
    return self

  def open(self):
    ok = RegistryClient().open()
    if not ok:
      return self
    MarketPublisher().open()
    KlineLocalCache().open()
    self.market_impl.open()
    return self

  def close(self):
    self.market_impl.close()
    self.running = False
    # self.thread.join()

  def onTick(self, tick: Tick):
    if not self.iam_slave_and_master_online():
      MarketPublisher().publish_loc(tick)

  def onKline(self, kline: KLine):
    """
      slave 接收 master 心跳，超时则替代master发送kline
      ** 最佳方案：
           切换时，发送从 最近一次master ka 信号时间开始的所有kline数据
    """
    KlineLocalCache().write(kline)
    if not self.iam_slave_and_master_online():
      MarketPublisher().publish_remote(kline)

  def onKlinePull(self,kline: KLine):
    """market 实现历史kline的拉取推送"""
    if not self.iam_slave_and_master_online():
      MarketPublisher().publish_attach(kline)

  def iam_slave_and_master_online(self):
    if self.cfgs.get('ha_enable',0) and self.cfgs.get('ha_role','master') == 'slave':
      timeout = self.cfgs.get('ha_master_timout',120)
      delta = datetime.datetime.now() - self.master_keepalive_time
      if delta.total_seconds() < timeout:
        return True

      print("market slave switch on.")
    return False

  def onOrderBook(self, orderbook: OrderBook):
    if not self.iam_slave_and_master_online():
      MarketPublisher().publish_loc(orderbook)

  def onRegClientMessage(self,message):
    if isinstance(message,ServiceKeepAlive):
      # 拦截到master的keepalive
      self.master_keepalive_time = datetime.datetime.now()
    elif type(message) == KlineAttach:
      # 补缺 kline
      self.onKlineAttachHandler(message)
    elif type(message) == KlinePull:
      # 即刻从交易所拉去获得指定合约的kline记录 2021/12/25
      self.onKlinePullHandler(message)

  def onKlinePullHandler(self,m:KlinePull):
    if m.exchange != self.cfgs.get('exchange'):
      return
    if not self.iam_slave_and_master_online():
      self.market_impl.kline_pull(m.exchange,m.tt,m.symbol,m.start,m.end)

  def onKlineAttachHandler(self,message:KlineAttach):
    """接收到kline补尝消息，读取历史kline ，发送kline """
    if message.exchange != self.cfgs.get('exchange'):
      return
    if not self.iam_slave_and_master_online():
      start = datetime.datetime.fromtimestamp( int(message.start/1000))
      end = datetime.datetime.fromtimestamp( int(message.end/1000))
      lines = KlineLocalCache().read(message.exchange,message.tt,message.period,message.symbol,start,end)
      for line in lines:
        MarketPublisher().publish_attach(line)

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
# FN = os.path.join(PWD,  'market.json')
FN = 'market.json'

def run(id = '',fn='',noprompt=False):
  global FN
  if fn:
    FN = fn
  if FN[0]!='/':
    FN = os.path.join(PWD,FN)
  params = json.loads(open(FN).read())
  if id:
    params['service_id'] = id

  Controller().init(**params).addBehavior("market",MarketInstance()).open()
  if noprompt:
    signal.signal(signal.SIGINT, signal_handler)
    print("")
    print("~~ Press Ctrl+C to kill .. ~~")
    while True:
      time.sleep(1)
  else:
    MarketInstance().cmdloop()

if __name__ == '__main__':
  # run()
  fire.Fire()