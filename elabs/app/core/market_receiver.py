#coding:utf-8

"""
MarketReceiver
1.行情消息接收服务
2. tick, kline, orderbook,order ,trade
"""

import threading
import time, traceback
import json
from elabs.fundamental.utils.useful import object_assign, singleton
from elabs.app.core.message import  *
from elabs.utils.zmqex import init_keepalive

import zmq

@singleton
class MarketReceiver(object):
    """"""
    def __init__(self):
        self.cfgs = {}
        self.users = []
        self.broker = None
        self.ctx = zmq.Context()
        self.sock = None

        self.running = False
        self.sock_list = []

    def init(self,**cfgs):
        '''
         market_topic :
         market_broker_addr :
        '''
        self.cfgs.update(**cfgs)

        recv_list = self.cfgs.get('market_receivers',[])
        if self.cfgs.get('market_broker_addr'):
            recv_list.append([self.cfgs.get('market_topic', ''),self.cfgs.get('market_broker_addr','') ])

        # addr = self.cfgs.get('market_broker_addr','')
        # if addr :
        #     self.sock = self.ctx.socket(zmq.SUB)
        #     init_keepalive(self.sock)
        #     self.sock.connect( addr )
        #
        # topic = self.cfgs.get('market_topic', '')
        # if isinstance(topic, (tuple, list)):
        #     for tp in topic:
        #         tp = tp.encode()
        #         self.sock.setsockopt(zmq.SUBSCRIBE, tp)
        # else:
        #     topic = topic.encode()
        #     self.sock.setsockopt(zmq.SUBSCRIBE, topic)  # 订阅所有

        self.sock_list =[]
        for r in recv_list:
            topic,addr = r
            sock = self.ctx.socket(zmq.SUB)
            init_keepalive(sock)
            if isinstance(topic, (tuple, list)):
                for tp in topic:
                    tp = tp.encode()
                    sock.setsockopt(zmq.SUBSCRIBE, tp)
            else:
                topic = topic.encode()
                sock.setsockopt(zmq.SUBSCRIBE, topic)  # 订阅所有
                print("--topic sub:",topic)
            sock.connect(addr)
            print("--connect:",addr)
            self.sock_list.append(sock )

        return self

    def addUser(self,user):
        self.users.append(user)
        return self

    def _recv_thread(self):
        self.running = True
        poller = zmq.Poller()
        for sock in self.sock_list:
            poller.register(sock, zmq.POLLIN)

        while self.running:
            text = ''
            try:
                events = dict(poller.poll(1000))
                for sock in self.sock_list:
                    if sock in events:
                        text = sock.recv_string()
                        self.parse(text)
            except:
                traceback.print_exc()
                print(text)
                time.sleep(.1)

    def parse(self,text):
        message = parseMessage(text)
        for user in self.users:
            if isinstance(message,Tick):
                user.onTick(message)
            elif isinstance(message,KLine):
                user.onKline(message)
            elif isinstance(message,OrderBook):
                user.onOrderBook(message)

    def open(self):
        self.thread = threading.Thread(target=self._recv_thread)
        self.thread.daemon = True
        self.thread.start()
        return self

    def close(self):
        self.running = False
        self.sock.close()
        #self.thread.join()

    def join(self):
        self.thread.join()

