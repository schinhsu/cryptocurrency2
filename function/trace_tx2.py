import datetime
import pandas
from .get_transfer2 import get_transfer_tron
from .get_transfer2 import columns

addrLabels = {}

def lookup_addrs(tronObj,addrs):
    global addrLabels
    for addr in addrs:
        if not addrLabels.get(addr) is None:
            continue
        addrInfo = tronObj.get_account_info(addr)
        tag = '' if addrInfo.get('addressTag') is None else addrInfo['addressTag']
        type_ = False
        if addrInfo['accountType'] == 2:
            type_ = True
        addrLabels[addr] = {}
        addrLabels[addr]['IsContract'] = type_
        addrLabels[addr]['Label'] = tag

def trace_tx_tron(tronObj,txinfo,traceType='From',traceLimit=1000,totalLimit=100000,
                  traceTolerance=1,ignoreAmount=1,debugMode=False):
    #如果給的資料有'TXType'的欄位，transType值='TXType'欄位值
    if 'TXType' in txinfo.index:
        transType = txinfo['TXType'].upper()
        if not transType in ['TRC10','TRC20']:
            token_info = tronObj.get_contract_info(txinfo['Contract'])
            transType = token_info['data'][0]['tokenInfo']['tokenType'].upper()
    #如果給的資料沒有'TXType'的欄位，依token類型自動判斷使用哪個transType
    else:
        if txinfo['Token'] == 'TRX':
            transType = 'TRC10'
        elif txinfo['Token'] == 'USDT':
            transType = 'TRC20'
        else:
            token_info = tronObj.get_contract_info(txinfo['Contract'])
            transType = token_info['data'][0]['tokenInfo']['tokenType'].upper()

    targetUTCTime = txinfo['Date(UTC+8)']-datetime.timedelta(hours=8)
    # 下載追蹤錢包資訊(正常交易和Internal交易都下載)
    sort = 'desc' if traceType == 'From' else 'asc'
    if traceType == 'From':
        tmp = get_transfer_tron(tronObj,txinfo[traceType],transType=transType,sort=sort,
                                limit=traceLimit,totalLimit=totalLimit,end=targetUTCTime,debugMode=debugMode)
        if debugMode:
            print(f'***已下載{len(tmp[2])}筆{transType}交易紀錄')
        tmp2 = get_transfer_tron(tronObj,txinfo[traceType],transType='Internal',sort=sort,
                                limit=traceLimit,totalLimit=totalLimit,end=targetUTCTime,debugMode=debugMode)
        if debugMode:
            print(f'***已下載{len(tmp2[2])}筆Internal交易紀錄')
    else:
        tmp = get_transfer_tron(tronObj,txinfo[traceType],transType=transType,sort=sort,
                                limit=traceLimit,totalLimit=totalLimit,start=targetUTCTime,debugMode=debugMode)
        if debugMode:
            print(f'***已下載{len(tmp[2])}筆{transType}交易紀錄')
        tmp2 = get_transfer_tron(tronObj,txinfo[traceType],transType='Internal',sort=sort,
                                limit=traceLimit,totalLimit=totalLimit,start=targetUTCTime,debugMode=debugMode)
        if debugMode:
            print(f'***已下載{len(tmp2[2])}筆Internal交易紀錄')
    tmp[2].loc[:,'TXType'] = [transType.lower() for _ in range(len(tmp[2]))]
    tmp2[2].loc[:,'TXType'] = ['Internal' for _ in range(len(tmp2[2]))]
    check = pandas.concat([tmp[2],tmp2[2]])
    check = check.sort_values(by=['Date(UTC+8)'],ascending=(traceType=='To'))

    start = False
    amount = 0
    result = pandas.DataFrame(columns=columns)
    for _,row in check.iterrows():
        if row['TxID'] == txinfo['TxID']:
            start = True
            continue
        # 過濾幣別、合約
        if row['Token'] != txinfo['Token']:
            #print('pass1')
            continue
        #internal交易的合約位址會寫觸發的智能合約(和一般trc10交易不同)
        if row['TXType'] != 'Internal' and row['Contract'] != txinfo['Contract'] and txinfo['Token'] != 'TRX':
            #print('pass2')
            continue
        if start:
            if row[traceType] != txinfo[traceType]:
                amount += row['Value']
                #加入手續費
                if traceType == 'To' and transType == 'TRC10':
                    amount += row['TxFee']
                #result.loc[len(result)] = row.tolist()[:len(columns)]
                result = pandas.concat([result,pandas.DataFrame([row.values],columns=row.index)])
                if amount >= txinfo['Value']:
                    break
                if txinfo['Value'] - amount < traceTolerance:
                    break
    check_len1 = len(result)
    result = result[result['Value']>=ignoreAmount]
    check_len2 = len(result)
    #確認錢包位址是否為合約及標記
    lookup_addrs(tronObj,result['From'].tolist()+result['To'].tolist())
    prefix = 'From'
    for addr in result[prefix].drop_duplicates():
        lookup = addrLabels[addr]
        result.loc[result[prefix]==addr,[prefix+'Contract',prefix+'Label']] = [lookup['IsContract'],lookup['Label']]
    prefix = 'To'    
    for addr in result[prefix].drop_duplicates():
        lookup = addrLabels[addr]
        result.loc[result[prefix]==addr,[prefix+'Contract',prefix+'Label']] = [lookup['IsContract'],lookup['Label']]

    errType = ''
    if not tmp[0]:
        errType += f'{tmp[1]}類型{transType}'
    if not tmp2[0]:
        if len(errType) > 0:
            errType += '; '
        errType += f'{tmp2[1]}類型Internal'
    if check_len2 == 0 and check_len1 > 0:
        errType = f'追查結果均小於忽略金額={ignoreAmount}，請增加ignoreAmount'
    if check_len1 == 0:
        errType = f'可能須調整traceTolerance(原設定值={traceTolerance})，或尚未花掉'
    if len(errType) == 0 and not start:
        errType = f'下載交易資料未包含原始追蹤交易，請增加traceLimit(原設定值={traceLimit})'
    if start and txinfo['Value'] - amount >= traceTolerance:
        errType = f'追查結果總金額={amount}未達原始追蹤交易額={txinfo["Value"]}，請增加traceLimit(原設定值={traceLimit})' 
    if len(errType) == 0:
        errType = '無異常'
    return result,errType