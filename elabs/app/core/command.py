#coding:utf-8


"""

ver,dest_service:dest_id,from_service:from_id,msg_type

1.0,market:001,manager:m01,status_query,plain,text
1.0,market:001,manager:m01,status_query,base64:json,

"""

import datetime,json,base64
import os
import time
import traceback
# from elabs.fundamental.utils.useful import utc_timestap
from elabs.fundamental.utils.timeutils import localtime2utc

from elabs.app.core.message import TradeType,ExchangeType

class CommandBase(object):
  Type = ''
  def __init__(self):
    self.ver = '2.0'
    self.dest_service = 'manager' # 中心管理服务
    self.dest_id = 'manager'
    self.from_service = ''
    self.from_id = ''
    self.timestamp = int(datetime.datetime.now().timestamp()*1000)
    self.msg_type = self.Type
    self.encode = 'plain'

  def body(self):
    return ''

  def marshall(self):
    signature = 'nosig'
    text = f"{self.ver},{self.dest_service}:{self.dest_id},{self.msg_type}," \
           f"{self.from_service}:{self.from_id},{self.timestamp}," \
           f"{signature},{self.encode},{self.body()}"
    return text


class PositionSignal(CommandBase):
  """仓位信号"""
  Type = 'position_signal'
  def __init__(self):
    CommandBase.__init__(self)
    self.exchange = ''
    self.account = ''
    self.tt = TradeType.SPOT[1]
    self.symbol = ''
    self.pos = 0
    self.datetime = int(datetime.datetime.now().timestamp()*1000)
    self.timestamp = int(datetime.datetime.now().timestamp()*1000)
    self.encode = 'base64:json'

  def body(self):
    data = dict(exchange = self.exchange,
                account = self.account,
                tt = self.tt,
                symbol = self.symbol,
                pos = self.pos,
                datetime = int(float(self.datetime)))
    text = base64.b64encode(json.dumps(data).encode()).decode()
    return text

  @classmethod
  def parse(cls,data):
    m = cls()
    m.exchange = data.get('exchange')
    m.account = data.get('account')
    m.tt = data.get('tt')
    m.symbol = data.get('symbol')
    m.pos = data.get('pos')
    m.datetime = data.get('datetime')
    return m

  @staticmethod
  def rand_one():
    ps = PositionSignal()
    ps.exchange = ExchangeType.Binance[1]
    ps.account = "acc001"
    ps.tt = TradeType.SPOT[1]
    ps.symbol = 'btc/ustd'
    ps.pos = 11
    ps.datetime = datetime.datetime.now().timestamp()
    return ps

class ExchangeSymbolUp(CommandBase):
  """交易所交易币上报"""
  Type = 'exg_symbol_up'
  def __init__(self):
    CommandBase.__init__(self)
    self.exchange = ''
    self.tt = {}    # { spot: [...] , swap:[...] }

    self.timestamp = int(datetime.datetime.now().timestamp()*1000)
    self.encode = 'base64:json'

  def body(self):
    data = dict(exchange = self.exchange,
                tt = self.tt)
    text = base64.b64encode(json.dumps(data).encode()).decode()
    return text

  @classmethod
  def parse(cls,data):
    m = cls()
    m.exchange = data.get('exchange')
    m.tt = data.get('tt')
    return m

  @staticmethod
  def rand_one():
    m = ExchangeSymbolUp()
    m.exchange = ExchangeType.Binance[1]
    m.tt = {
     "spot":{"btc/usdt": [1, 0.0001, 100000, 10000, 100, 0.0001],
         "eth/usdt": [1, 0.001, 5000, 1000, 1000, 0.001]},
     }
    return m

class ServiceLogText(CommandBase):
  """服务消息"""
  Type = 'svc_log'
  def __init__(self):
    CommandBase.__init__(self)
    self.timestamp = 0
    self.level = 'D'
    self.text = ''
    self.encode = 'base64:json'

  def body(self):
    data = dict(timestamp = self.timestamp,
                level = self.level,
                text = self.text
               )
    text = base64.b64encode(json.dumps(data).encode()).decode()
    return text

  @classmethod
  def parse(cls,data):
    m = cls()
    m.timestamp = data.get('timestamp',0)
    m.level = data.get('level','')
    m.text = data.get('text','')
    return m

  @classmethod
  def rand_one(cls):
    ps = cls()
    ps.timestamp = int(datetime.datetime.now().timestamp()*1000)
    ps.level = "I"
    ps.text = "system busy!"
    return ps

class ServiceAlarmData(CommandBase):
  """服务消息"""
  Type = 'alarm'
  def __init__(self):
    CommandBase.__init__(self)
    self.type = 'app'
    self.level = 0
    self.tag = ''
    self.detail = ''
    self.data = {}
    self.encode = 'base64:json'

  def body(self):
    data = dict(type = self.type,
                level = self.level,
                tag = self.tag,
                detail = self.detail,
                data = self.data
               )
    text = base64.b64encode(json.dumps(data).encode()).decode()
    return text

  @classmethod
  def parse(cls,data):
    m = cls()
    m.type = data.get('type',0)
    m.level = data.get('level',0)
    m.tag = data.get('tag','')
    m.detail = data.get('detail','')
    m.data = data.get('data',{})
    return m

  @classmethod
  def rand_one(cls):
    ps = cls()
    # ps.type =
    ps.level = 1
    ps.tag = "red"
    ps.detail = "wwwwww..."
    ps.data = dict(name='abc',water=99)
    return ps



class ServiceStatusRequest(CommandBase):
  """服务状态查询"""
  Type = 'service_status_request'
  def __init__(self):
    CommandBase.__init__(self)

  @classmethod
  def parse(cls,data):
    m = cls()
    return m

  @staticmethod
  def rand_one():
    m = ServiceStatusRequest()
    return m


class ServiceStatusReport(CommandBase):
  """上报本服务状态信息"""
  Type = 'service_status_report'
  def __init__(self):
    CommandBase.__init__(self)
    self.service_type = ''
    self.service_id = ''
    self.pid = 0
    self.start = 0
    self.params = {}    # 附加的服务状态信息
    self.encode = 'base64:json'
    self.now = 0
    self.ip = ''
    self.tag = ''

  def body(self):
    data = dict(service_type= self.service_type,
                service_id = self.service_id,
                now = self.now,
                pid = self.pid,
                start = self.start,
                params = self.params,
                ip = self.ip,
                tag = self.tag
                )
    text = base64.b64encode(json.dumps(data).encode()).decode()
    return text

  @classmethod
  def parse(cls,data):
    m = cls()

    m.service_type = data.get('service_type')
    m.service_id = data.get('service_id')
    m.pid = data.get('pid')
    m.start = data.get('start')
    m.now = data.get('now')
    m.params = data.get('params',{})
    m.ip = data.get('ip','')
    m.tag = data.get('tag','')
    return m

  @classmethod
  def rand_one(cls):
    m = cls()
    m.service_type = 'market'
    m.service_id = 'market01'
    m.pid = 10021
    m.start = int(datetime.datetime.now().timestamp()*1000)
    m.now = m.start
    m.params = {'k':1001}
    return m


class ServiceKeepAlive(ServiceStatusReport):
  Type = 'service_keep_alive'
  def __init__(self):
    ServiceStatusReport.__init__(self)

class KlineAttach(CommandBase):
  """请求kline补偿,本地缓存查询"""
  Type = 'kline_attach'
  def __init__(self):
    CommandBase.__init__(self)
    self.exchange = ''
    self.tt = ''
    self.symbol = ''
    self.period = 1
    self.start = 0
    self.end = 0

    self.timestamp = int(datetime.datetime.now().timestamp()*1000)
    self.encode = 'base64:json'
    self.dest_service = "market"
    self.dest_id = "market"

  def body(self):
    data = dict(exchange = self.exchange,
                tt = self.tt,
                symbol = self.symbol,
                period = self.period,
                start = self.start,
                end = self.end)
    text = base64.b64encode(json.dumps(data).encode()).decode()
    return text

  @classmethod
  def parse(cls,data):
    m = cls()
    m.exchange = data.get('exchange','')
    m.tt = data.get('tt','')
    m.symbol = data.get('symbol','')
    m.period = data.get('period',1)
    m.start = int(float(data.get('start',0)))
    m.end = int( float(data.get('end',0)))
    if m.start < 1e+10 :
      m.start = m.start * 1000
    if m.end  < 1e+10:
      m.end = m.end * 1000

    return m

  @classmethod
  def rand_one(cls):
    m = cls()
    m.exchange = ExchangeType.FTX[1]
    m.tt = 'swap'
    m.symbol = 'btc/usdt'
    m.start = int( (datetime.datetime.now().timestamp() - 100) * 1000)
    m.end = int( datetime.datetime.now().timestamp() * 1000)

    return m

class KlinePull(KlineAttach):
  """请求kline补偿(从交易所拉取）"""
  Type = 'kline_pull'
  def __init__(self):
    KlineAttach.__init__(self)
    self.dest_service ="market"
    self.dest_id = "market"


class SystemMxAliveBroadcast(CommandBase):
  """上报本服务状态信息"""
  Type = 'sys_mx_alive_broadcast'
  def __init__(self):
    CommandBase.__init__(self)
    self.from_id = 'manager'
    self.from_service = 'manager'

  @classmethod
  def parse(cls,data):
    m = cls()
    return m

  @classmethod
  def rand_one(cls):
    m = cls()
    return m

class TradePosReport(CommandBase):
  """上报账户仓位信息"""
  Type = 'pos_report'
  def __init__(self):
    CommandBase.__init__(self)
    self.from_id = 'trader'
    self.from_service = 'trader'
    self.timestamp = int(datetime.datetime.now().timestamp() * 1000)
    self.encode = 'base64:json'
    self.account = ''
    self.symbol = ''
    self.tt = ''          # 交易类型
    self.real_pos = 0     # 实际仓位
    self.real_pending = 0 #	挂单仓位
    self.db_pos = 0       #数据库仓位

  @classmethod
  def parse(cls,data):
    m = cls()
    m.account = data.get('account','')
    m.symbol = data.get('symbol','')
    m.tt = data.get('tt','')
    m.real_pos = data.get('real_pos',0)
    m.real_pending = data.get('real_pending',0)
    m.db_pos = data.get('db_pos',0)
    return m

  @classmethod
  def rand_one(cls):
    m = cls()
    return m

  def body(self):
    data = dict(account = self.account,
                symbol = self.symbol,
                tt = self.tt,
                real_pos = self.real_pos,
                real_pending = self.real_pending,
                db_pos = self.db_pos
                )
    text = base64.b64encode(json.dumps(data).encode()).decode()
    return text


class TradeEquityReport(CommandBase):
  """上报账户权益信息"""
  Type = 'equity_report'
  def __init__(self):
    CommandBase.__init__(self)
    self.from_id = 'trader'
    self.from_service = 'trader'
    self.timestamp = int(datetime.datetime.now().timestamp() * 1000)
    self.encode = 'base64:json'
    self.account = ''
    self.collateral = 0             # 帐户实际资产
    self.free_collateral = 0          # 空闲可用资产
    self.position_value = 0     # 开仓市值

  @classmethod
  def parse(cls,data):
    m = cls()
    m.account = data.get('account','')
    m.collateral = data.get('collateral',0)
    m.free_collateral = data.get('free_collateral',0)
    m.position_value = data.get('position_value',0)
    return m

  @classmethod
  def rand_one(cls):
    m = cls()
    return m

  def body(self):
    data = dict(account = self.account,
                collateral = self.collateral,
                free_collateral = self.free_collateral,
                position_value = self.position_value)
    text = base64.b64encode(json.dumps(data).encode()).decode()
    return text

class KlineUpdateReport(CommandBase):
  """上报行情最新接收状态（klogger负责定时上报）"""
  Type = 'klineupdate_report'
  def __init__(self):
    CommandBase.__init__(self)
    self.from_id = 'klogger'
    self.from_service = 'klogger'
    self.timestamp = int(datetime.datetime.now().timestamp() * 1000)
    self.encode = 'base64:json'
    self.datas = [] # [ [ exchange,tt,period,symbol,uptime ] ]


  @classmethod
  def parse(cls,data):
    m = cls()
    m.datas = data.get('datas',[])
    return m

  @classmethod
  def rand_one(cls):
    m = cls()
    return m

  def body(self):
    data = dict(datas = self.datas)
    text = base64.b64encode(json.dumps(data).encode()).decode()
    return text


class HostRunningStatus(CommandBase):
  """上报主机运行状态"""
  Type = 'host_run_status'
  def __init__(self):
    CommandBase.__init__(self)
    self.data = {}
    self.encode = 'base64:json'

  def body(self):
    data = dict(
                data = self.data
               )
    text = base64.b64encode(json.dumps(data).encode()).decode()
    return text

  @classmethod
  def parse(cls,data):
    m = cls()
    m.data = data.get('data',{})
    return m


MessageDefinedList = [
  ServiceStatusRequest,
  ServiceStatusReport,
  PositionSignal,
  ServiceKeepAlive,
  ServiceLogText,
  ServiceAlarmData,
  ExchangeSymbolUp,
  KlineAttach,
  SystemMxAliveBroadcast  , # 2021/12/20
  KlinePull,   # 2021/12/25
  TradePosReport,
  TradeEquityReport,
  KlineUpdateReport,
  HostRunningStatus
  
]

def parseMessage(text):
  """解析消息报文"""
  if isinstance(text,bytes):
    text = text.decode()
  fs = text.split(',')
  if len(fs) < 8:
    return None
  ver ,dest,msg_type,from_,timestamp,signature,encode,body,*others = fs
  if ver !='2.0':
    return None
  m = None
  for md in MessageDefinedList:
    m = None
    try:
      if md.Type == msg_type:
        encs= encode.split(':')
        for enc in encs:
          if enc == 'base64':
            body = base64.b64decode(body)
          elif enc == 'json':
            body = json.loads(body)

        m = md.parse(body)
        m.ver = ver
        m.dest_service, m.dest_id = dest.split(':')
        m.msg_type = msg_type
        m.from_service, m.from_id = from_.split(':')
        m.timestamp = int(float(timestamp))
        m.signature = signature
    except:
      traceback.print_exc()
      m = None
    if m:
      break
  return m

def test_serde():
  print()
  text = PositionSignal.rand_one().marshall()
  print(text)
  m = parseMessage(text)
  text2 = m.marshall()
  print(text2)
  assert (text == text2)

  text = ServiceStatusRequest.rand_one().marshall()
  print(text)
  m = parseMessage(text)
  text2 = m.marshall()
  print(text2)
  assert (text == text2)

  text = ServiceStatusReport.rand_one().marshall()
  print(text)
  m = parseMessage(text)
  text2 = m.marshall()
  print(text2)
  assert (text == text2)

  text = ServiceKeepAlive.rand_one().marshall()
  print(text)
  m = parseMessage(text)
  text2 = m.marshall()
  print(text2)
  assert (text == text2)

  text = ServiceLogText.rand_one().marshall()
  print(text)
  m = parseMessage(text)
  text2 = m.marshall()
  print(text2)
  assert (text == text2)
