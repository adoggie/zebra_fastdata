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
        self.sock_back = {}

    def init(self,**cfgs):

        self.cfgs.update(**cfgs)
        mx_addr_list = self.cfgs['mx_addr_list']
        topic = self.cfgs['topic']
        # userback = self.cfgs['userback']

        for addr in mx_addr_list:
            sock = self.ctx.socket(zmq.SUB)
            init_keepalive(sock)
            sock.setsockopt(zmq.SUBSCRIBE, topic.encode())
            sock.connect(addr)
            print(" connect:",addr , "topic:",topic)    
            self.sock_list.append( sock )        
            # self.sock_back[sock] = userback
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
                        for user in self.users:
                            user.on_data(text)
            except:
                traceback.print_exc()
                print(text)
                time.sleep(.1)

    def run(self):
        return self._recv_thread() 
    
    def open(self , noThread = False):    
        self.thread = threading.Thread(target=self._recv_thread)
        self.thread.daemon = True
        self.thread.start()
        return self

    def close(self):
        self.running = False
        self.sock.close()

    def join(self):
        self.thread.join()

