from .transforms import json2df_tron
from .get_transfer import get_transfer_tron_desc
import datetime
import pandas

columns = ['BlockNo','TxID','Date(UTC+8)','From','To',
           'Value','TxFee','Token','Contract',
           'TXType','FromContract','ToContract','FromLabel','ToLabel']

def get_tx_by_hash(tronObj,txid,columns=columns):
    res = tronObj.get_txinfo_by_hash(txid)
    for key,value in res['contract_map'].items():
        if value:
            if res['addressTag'].get(key) is None:
                res['addressTag'][key] = res['contractInfo'][key]['tag1']
        else:
             if res['addressTag'].get(key) is None:
                res['addressTag'][key] = ''
    
    if 'transfersAllList' in res.keys():
        txlist = pandas.json_normalize(res['transfersAllList'])
        txlist['Value'] = txlist.apply(lambda tx:eval(tx['amount_str'][:-tx['decimals']]+'.'+tx['amount_str'][-tx['decimals']:]),axis=1)
        txlist['FromContract'] = txlist.apply(lambda tx:res['contract_map'][tx['from_address']],axis=1)
        txlist['ToContract'] = txlist.apply(lambda tx:res['contract_map'][tx['to_address']],axis=1)
        txlist[['BlockNo','TxID','Date(UTC+8)','TxFee']] = [[res['block'],res['hash'],
                                                             datetime.datetime.fromtimestamp(res['timestamp']/1000.0)+datetime.timedelta(hours=8),
                                                             res['cost']['energy_fee']/(10**6)] for _ in range(len(txlist))]
        txlist['FromLabel'] = txlist.apply(lambda tx:'' if res['addressTag'].get(tx['from_address']) is None else res['addressTag'][tx['from_address']],axis=1)
        txlist['ToLabel'] = txlist.apply(lambda tx:'' if res['addressTag'].get(tx['to_address']) is None else res['addressTag'][tx['to_address']],axis=1)
        txlist = txlist.rename(columns={'symbol':'Token','from_address':'From',
                                        'to_address':'To','contract_address':'Contract',
                                        'tokenType':'TXType'})
        txlist = txlist[columns]
    else:
        blockNo,txid,date,fee = res['block'],res['hash'],datetime.datetime.fromtimestamp(res['timestamp']/1000.0),res['cost']['energy_fee']/(10**6)
        from_,to_ = res['contractData']['owner_address'],res['contractData']['to_address']
        from_contract = res['contract_map'][from_]
        from_label = '' if res['addressTag'].get(from_) is None else res['addressTag'][from_]
        to_contract = res['contract_map'][to_]
        to_label = '' if res['addressTag'].get(to_) is None else res['addressTag'][to_]
        if len(res['contractInfo']) == 0:
            value = res['contractData']['amount']/(10**6)
            data = [[blockNo,txid,date,from_,to_,value,fee,
                    'TRX','trc10','trc10',from_contract,
                     to_contract,from_label,to_label]]
            txlist = pandas.DataFrame(data=data,columns=columns)
    return txlist

def get_transfer_tron(tronObj,addr,start=datetime.datetime(2010,1,1),
                          end=datetime.datetime.now(),
                          transType='TRC20',limit=500,sort='desc',
                          totalLimit=100000,debugMode=False):
    def set_starttime():
        res = tronObj.get_transfer_once(addr,startnum=0,
                                        start=start,end=end,
                                            transType=transType)
        if res['rangeTotal'] > totalLimit:
            if debugMode:
                print(f'***查詢期間交易總量超過容許值={res["rangeTotal"]}，可能為交易所')
            return False,"疑似交易所"
        if res['rangeTotal'] == 0:
            if debugMode:
                print(f'***查詢期間無交易，請確認設定值')
            return False,"查無交易"
        return True,start
    def set_endtime():
        starttime = start
        endtime = end
        count = 0
        time_colname = 'timestamp' if transType != 'TRC20' else 'block_ts'
        res = tronObj.get_transfer_once(addr,startnum=0,
                                        start=starttime,end=endtime,
                                            transType=transType)
        if res['rangeTotal'] > totalLimit:
            if debugMode:
                print(f'***查詢期間交易總量超過容許值={res["rangeTotal"]}，可能為交易所')
            return False,"疑似交易所"
        if debugMode:
            print('count',0,res['rangeTotal'],0,starttime,endtime)
        count = 0
        while True:
            #先改startnum查一次
            lastTotal = res['rangeTotal']
            if lastTotal <= 10000:
                startnum = max(lastTotal-limit,0)
            else:
                startnum = 10000-limit
            count += 1
            res = tronObj.get_transfer_once(addr,startnum=startnum,
                                                 start=starttime,end=endtime,
                                                 transType=transType)
            if transType == 'TRC10':
                txs = res['data']
            elif transType == 'Internal':
                txs = res['data']
            else:
                txs = res['token_transfers']
            if debugMode:
                print('count1',count,lastTotal,startnum,starttime,endtime,res['rangeTotal'],len(txs))
            
            if len(txs) == 0:
                if res["rangeTotal"] > 0:
                    print(f'***查詢期間交易總量={res["rangeTotal"]}，但API未回傳任何交易')
                    return False,"limit可以調整看看"
                else:
                    return False,"查無交易"

            if lastTotal <= 10000:
                endtime = datetime.datetime.fromtimestamp(txs[0][time_colname]/1000)
                break
            
            tmptime = datetime.datetime.fromtimestamp(txs[-1][time_colname]/1000)
            if tmptime > endtime:
                #交易數量過大會導致API有問題，反正不追
                if debugMode:
                    print('***疑似查詢期間交易總量過大，導致API回傳的交易時間有問題',tmptime,endtime)
                    print(res.url)
                    print('-----------------------------------------')
                return False,"API異常"
            elif tmptime == endtime:
                endtime = tmptime-datetime.timedelta(seconds=1)
            else:
                endtime = tmptime
            count += 1
            res = tronObj.get_transfer_once(addr,startnum=startnum,
                                                 start=starttime,end=endtime,
                                                 transType=transType)
            if debugMode:
                print('count2',count,lastTotal,startnum,starttime,endtime,res['rangeTotal'])
            
        return True,endtime

    if sort == 'asc':
        setTrue,setInfo = set_endtime()
        if setTrue:
            dfCollect = get_transfer_tron_desc(tronObj,addr,start=start,
                     end=setInfo,transType=transType,limit=limit,
                     debugMode=debugMode)
            dfCollect = dfCollect.sort_values(by='Date(UTC+8)')
        else:
            dfCollect = json2df_tron([],transType=transType)
    if sort == 'desc':
        setTrue,setInfo = set_starttime()
        if setTrue:
            dfCollect = get_transfer_tron_desc(tronObj,addr,start=start,
                         end=end,transType=transType,limit=limit,
                         debugMode=debugMode)
        else:
            dfCollect = json2df_tron([],transType=transType)
    return setTrue,setInfo,dfCollect
