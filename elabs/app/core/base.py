#coding:utf-8

from typing import List
from elabs.app.core.message import Tick,KLine,OrderBook
from elabs.app.core.command import PositionSignal

class MarketBase(object):
    def __init__(self):
        self.cfgs = {}

    def init(self,**kvs):
        self.cfgs.update(**kvs)

    def open(self):
        pass

    def close(self):
        pass

    def kline_pull(self,exchange,tt,symbol,start,end):
        """查询获取指定时间段的kline记录并返回
            exchange : 交易所 ftx
            tt： 交易类型  swap/spot
            symbol: 交易对
            start , end :  utc-timestamp  (ms)
        """
        pass

class TradeBase(object):
    def __init__(self):
        pass

    def init(self,**kvs):
        pass

    def open(self):
        pass

    def close(self):
        pass

    def onTick(self, tick: Tick):
        pass

    def onKline(self, kline: KLine):
        pass

    def onOrderBook(self, orderbook: OrderBook):
        pass

    def onPositionSignal(self, pos: PositionSignal):
        pass

    def init_pos(self, positions: List[PositionSignal]):
        """启动时带入最近的仓位信息"""

        pass

    def onTimer(self,name):
        pass