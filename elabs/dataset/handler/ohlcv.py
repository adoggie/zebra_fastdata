


from re import L


def on_data(text , dsb):
    """ 
        text: message from mx
        dsb: datasetbundle 
    return:  
        symbol,ts, values 
    """
    # topic = dsb.profile['topic']
    print(f"on_data: <{dsb.cfgs['dataset']}> " ,text)
    fs = text.split(',')
    if len(fs) <= 3:
        return None
    
    return parse( fs )

def parse(fs):    
    ver,type,exchange,tt,period,symbol,ts,\
    open,high,low,close,vol,amt,transactions,is_maker,\
    buy_vol,buy_amt, *others = fs
    period = period.replace('m','')
    period = int(period)
    ts = int(float(ts)/1000)
    kvs = dict(
        O = float(open),
        H = float(high),
        L = float(low),
        C = float(close),
        V = float(vol),
        AMT = float(amt),
        TRAN = float(transactions),
        MKR = int(is_maker),
        BV = float(buy_vol),
        BAMT = float(buy_amt)
    )
    return symbol,ts,kvs