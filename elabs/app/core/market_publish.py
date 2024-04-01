#coding:utf-8

"""
MessagePublisher
为 委托报单服务提供 Tick，OrderBook，信息


"""
import fire
import json
import zmq
from elabs.fundamental.utils.useful import singleton, hash_object, object_assign
from elabs.app.core.message import RapidMessage
from elabs.utils.zmqex import init_keepalive

@singleton
class MarketPublisher(object):
  def __init__(self):
    self.cfgs = {}
    self.sock_remote = None
    self.sock_loc = None
    self.sock_attch = None  # kline补充发布

  def init(self,**kvs):
    self.cfgs.update(**kvs)

    self.ctx = zmq.Context()

    # 远程发布
    addr = self.cfgs.get('market_public_broker_addr','').strip()
    if addr:
      self.sock_remote = self.ctx.socket(zmq.PUB)
      init_keepalive(self.sock_remote)
      self.sock_remote.connect(addr)

    #本地发布
    addr = self.cfgs.get('market_local_broker_addr','').strip()
    if addr:
      self.sock_loc = self.ctx.socket(zmq.PUB)
      init_keepalive(self.sock_loc)
      self.sock_loc.bind(addr)

    #attach 发布 kline 补偿
    addr = self.cfgs.get('market_attach_broker_addr','').strip()
    if addr:
      self.sock_attch = self.ctx.socket(zmq.PUB)
      init_keepalive(self.sock_attch)
      self.sock_attch.connect(addr)

    return self

  def open(self):
    return self

  def close(self):
    self.sock.close()
    return self

  def publish_remote_rawdata(self,text):
    if self.sock_remote:
      self.sock_remote.send(text.encode())

  def publish_attach(self,message:RapidMessage):
    text = message.marshall()
    if self.sock_attch:
      self.sock_attch.send(text.encode())

  def publish_remote(self,message:RapidMessage):
    text = message.marshall()
    if self.sock_remote:
      self.sock_remote.send(text.encode())

  def publish_loc(self,message:RapidMessage):
    text = message.marshall()
    if self.sock_loc:
      self.sock_loc.send(text.encode())
