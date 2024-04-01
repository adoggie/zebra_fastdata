#coding:utf-8

"""
kseeker.py

client for KSeeker
"""

import os,os.path,time,datetime,traceback,sys,json
from dateutil.parser import parse

import fire
import requests
import pandas as pd

PWD = os.path.dirname(os.path.abspath(__file__))
FN = os.path.join( os.path.dirname( os.path.abspath(__file__) ) , 'settings.json')
cfgs = json.loads( open(FN).read())


base_url = ''

def get_kline(symbol,start='', end='',num=0, exchange='ftx',
              tt='spot',period='1m',pdf=True,url='',fs='',fill=1,reverse=0):
  base_url = cfgs.get("kseeker.base_url",'')
  if url:
    base_url = url
  if base_url.find('/api/kseeker/kline') == -1:
    base_url = base_url+'/api/kseeker/kline'

  params = dict(exchange = exchange,tt = tt,symbol = symbol,period=period,num = num ,
                start=start,end=end ,reverse=reverse, fill = fill , fs=fs)

  data = requests.get(base_url,params=params,timeout=10000).json()
  result = data['result']
  if pdf :
    result = pd.DataFrame(result)
  return result


def test_get_kline():
  global base_url
  base_url = "http://f4:17028/api/kseeker/kline"
  df = get_kline('BTC-PERP',exchange='ftx',tt='swap',num=10,fs='TS,DT,O,H,L,C,AMT,BV')
  print(df )


if __name__ == '__main__':
    fire.Fire()