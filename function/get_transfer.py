from .transforms import json2df_tron,json2df_eth

import pandas
import datetime
import math


# start,end=UTC Time
def get_transfer_eth(ethObj,addr,start=datetime.datetime(2010,1,1),
                     end=datetime.datetime.now(),
                     transType='ERC20',batchnum=10000,limit=100000,
                     endblock=999999999,startblock=0,sort='desc',
                     debugMode=False):
    addr = addr.lower()

    collects = []

    pageNo = 0
    pageLimit = 10000/batchnum
    while True:
        if pageNo >= pageLimit:
            if sort == 'desc':
                endblock = eval(collects[-1]['blockNumber'])
            elif sort == 'asc':
                startblock = eval(collects[-1]['blockNumber'])
            pageNo = 0
        pageNo += 1
        if debugMode:
            print('page',pageNo,sort,startblock,endblock)
        if transType in ['Normal','Internal','ERC20']:
            res = ethObj.get_transfer_once(addr,batchnum=batchnum,
                                        endblock=endblock,
                                        startblock=startblock,
                                        sort=sort,page=pageNo,
                                          transType=transType)
        ##目前尚未支援ERC721
        elif transType in ['ERC721']:
            res = ethObj.get_transfer_once(addr,batchnum=batchnum,
                                        sort=sort,page=pageNo,
                                          transType=transType)
        
        collects += res

        if debugMode:
            if len(res) == 0:
                print(len(res),batchnum)
            else:
                print(len(res),batchnum,datetime.datetime.fromtimestamp(eval(res[-1]['timeStamp'])))
        if len(res) < batchnum:
            break
        if sort == 'desc' and datetime.datetime.fromtimestamp(eval(res[-1]['timeStamp'])) < start:
            break
        if sort == 'asc' and datetime.datetime.fromtimestamp(eval(res[-1]['timeStamp'])) > end:
            break
        if len(collects) >= limit:
            break

    dfCollect = json2df_eth(collects)
    dfCollect = dfCollect.drop_duplicates()
    dfCollect = dfCollect[(dfCollect['Date(UTC+8)']>=start+datetime.timedelta(hours=8))&(dfCollect['Date(UTC+8)']<=end+datetime.timedelta(hours=8))]
    return dfCollect

# start,end=UTC Time
def get_transfer_tron_desc(tronObj,addr,start=datetime.datetime(2010,1,1),
                           end=datetime.datetime.now(),
                           transType='TRC20',limit=100000,debugMode=False):
    collects = []

    pageNo = 0
    pageLimit = 200 #10000/50
    starttime = start
    endtime = end
    time_colname = 'timestamp' if transType != 'TRC20' else 'block_ts'
    while True:
        if debugMode:
            print('page',pageNo,starttime,endtime)
        res = tronObj.get_transfer_once(addr,startnum=pageNo*50,
                                        start=starttime,end=endtime,
                                          transType=transType)
        if transType == 'TRC10':
            txs = res['data']
        elif transType == 'Internal':
            txs = res['data']
        else:
            txs = res['token_transfers']
        
        collects += txs

        if debugMode:
            if len(txs) == 0:
                print(len(txs),pageNo*50,res['rangeTotal'])
            else:
                print(len(txs),pageNo*50,res['rangeTotal'],datetime.datetime.fromtimestamp(txs[-1][time_colname]/1000))
        if len(txs) < 50 or pageNo*50 > res['rangeTotal']:
            break
        if datetime.datetime.fromtimestamp(txs[-1][time_colname]/1000) < start:
            break
        if len(collects) >= limit:
            break
        pageNo += 1
        
        if pageNo >= pageLimit:
            endtime = datetime.datetime.fromtimestamp(txs[-1][time_colname]/1000)
            pageNo = 0

    dfCollect = json2df_tron(collects,transType=transType)
    dfCollect = dfCollect.drop_duplicates()
    dfCollect = dfCollect[(dfCollect['Date(UTC+8)']>=start+datetime.timedelta(hours=8))&(dfCollect['Date(UTC+8)']<=end+datetime.timedelta(hours=8))]
    return dfCollect

# start,end=UTC Time
def get_transfer_tron_asc(tronObj,addr,start=datetime.datetime(2010,1,1),
                          end=datetime.datetime.now(),
                          transType='TRC20',limit=100000,
                          upLimit=100000,debugMode=False):
    starttime = start
    endtime = end
    count = 0
    time_colname = 'timestamp' if transType != 'TRC20' else 'block_ts'
    while True:
        res = tronObj.get_transfer_once(addr,startnum=0,
                                        start=starttime,end=endtime,
                                          transType=transType)
        count += 1
        if debugMode:
            print('count',count,res['rangeTotal'],starttime,endtime)
        if res['rangeTotal'] > upLimit:
            if debugMode:
                print(f'***此錢包交易數量={res["rangeTotal"]}，疑似為交易所考慮程式效能所以不追')
            return json2df_tron([],transType=transType)
        if res['rangeTotal'] > 10000:
            startnum = min(limit,10000)-50
            res2 = tronObj.get_transfer_once(addr,startnum=startnum,
                                             start=starttime,end=endtime,
                                             transType=transType)
            if transType == 'TRC10':
                txs = res2['data']
            elif transType == 'Internal':
                txs = res2['data']
            else:
                txs = res2['token_transfers']
            if len(txs) == 0:
                print('No transaction records',res2)
                break
            tmptime = datetime.datetime.fromtimestamp(txs[-1][time_colname]/1000)
            if tmptime > endtime:
                #交易數量過大會導致API有問題，反正不追
                if debugMode:
                    print('***疑似交易量過大，無法使用API取得更準確的下一筆交易紀錄(API僅能指定時間)')
                    print(res2.url)
                    print('-----------------------------------------')
                return json2df_tron([],transType=transType)
            elif tmptime == endtime:
                endtime = tmptime-datetime.timedelta(seconds=1)
            else:
                endtime = tmptime
        else:
            break
    

    dfCollect = get_transfer_tron_desc(tronObj,addr,start=starttime,
                 end=endtime,transType=transType,limit=limit,
                 debugMode=debugMode)
    dfCollect = dfCollect.sort_values(by='Date(UTC+8)')
    return dfCollect