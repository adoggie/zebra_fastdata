
1. 初始化数据集 ohlcv , 读取 `dataset.json` 获取交易合约列表，写入 `symbol_ohlcv.txt`.

---
```sh
python -m elabs.dataset.dataset init_dataset <ohlcv>
```
---
  选择交易合约

2. 初始化本地行情数据集文件

---
```sh
python -m elabs.dataset.dataset init_file <ohlcv>

vim ~/.bash_profile
umask 001
```
---

3. 同步数据集 
从db直接拉取行情数据到本地数据集文件

---
```sh
python -m elabs.dataset.dataset pull_data <ohlcv>
python -m elabs.dataset.dataset pull_data <ohlcv> --symbols="MA,RB" --start="2021-1-1" -- end="2021-3-1"
python -m elabs.dataset.dataset pull_data_par <ohlcv> --workers=50

```
---

4. 启动数据集同步服务

---
```sh
python -m elabs.dataset.dataset-service run <ohlcv>
```
---

5. 客户访问数据集测试

---
```sh
python -m elabs.dataset.client test 
```
---

6. 新上合约
   
   停止 `dataset-service.py`服务 ， 同步新增 symbol 到本地，创建数据文件并进行pull同步。

```bash
python -m elabs.dataset.dataset list_symbols_diff <ohlcv>  返回 新symbol列表
>> symbols: 'BTSUSDT,ACKUSDT'

python -m elabs.dataset.dataset init_dataset <ohlcv>
python -m elabs.dataset.dataset init_file <ohlcv> 'BTSUSDT,ACKUSDT'
python -m elabs.dataset.dataset pull_data <ohlcv> 'BTSUSDT,ACKUSDT'

1000SHIBBUSD,CVXBUSD,FILBUSD,LEVERBUSD,LINKBUSD,LTCBUSD,MATICBUSD,SANDBUSD
```

## 设置 mongodb 正向代理访问

---
```bash
autossh -M 0 -g -CNL 27017:127.0.0.1:27017 -p 22 scott@172.16.30.3 -o TCPKeepAlive=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=9999
```
---

## 测试

bsc 173个交易合约 
python dataset.py pull_data_par ohlcv --workers=50

网络IO : 50MB/s

* SSD 写入：  13分23秒
* /dev/shm 写入： 
* 非持久化: 13分23秒



