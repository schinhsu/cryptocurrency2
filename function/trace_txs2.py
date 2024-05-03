import pandas
import os
import datetime
from .get_transfer2 import get_tx_by_hash
from .trace_tx2 import trace_tx_tron,lookup_addrs,addrLabels
from .get_transfer2 import columns

def trace_layer(tronObj,traceResults,layer,traceType='From',traceLimit=500,traceTolerance=1,tracePrintNum=10,totalLimit=100000,ignoreAmount=1,debugMode=True):
    toTrace = traceResults[traceResults['Layer'] == layer]
    traceRes = pandas.DataFrame(columns=columns)

    traceLen = len(toTrace)
    count = 0
    for i,row in toTrace.iterrows():
        count += 1
        if count % tracePrintNum == 0 or count == traceLen:
            print(f'Tracing Layer {layer} - TXNO {count}/{traceLen} ...')
            
        if '追查情形' in traceResults.columns:
            traceCheck = traceResults[(traceResults['TxID']==row['TxID'])&(traceResults['TxNo']!=row['TxNo'])&(~traceResults['追查情形'].isna())]
            if len(traceCheck) > 0:
                tracedTXNO = traceCheck['TxNo'].iloc[0]
                print(f'{row["TxNo"]} 追查結果同{tracedTXNO}')
                traceResults.loc[i,'追查情形'] = f'追查結果同{tracedTXNO}'
                print(f'Tracing Layer {layer} - TXNO {count}/{traceLen} Result: {traceResults.loc[i,"追查情形"]}')
                continue
        
        traceTXRes = pandas.DataFrame(columns=columns)
        if row['TXType'] == 'Internal' or row[traceType+'Contract']:
            check_internal = get_tx_by_hash(tronObj,row['TxID'])
            check_internal.loc[:,['Layer','TxNo']] = [[row['Layer'],row['TxNo']] for _ in range(len(check_internal))]
            check_internal.index = [idx for idx in range(len(traceResults),len(traceResults)+len(check_internal))]
            check = check_internal[~check_internal[traceType+'Contract']]
            if len(check) == 0:
                print(f'{row["TxID"]} 無法確認要追蹤哪一筆')
                traceResults.loc[i,'追查情形'] = '無法確認要追蹤哪一筆'
                traceResults = pandas.concat([traceResults,check_internal])
                print(f'Tracing Layer {layer} - TXNO {count}/{traceLen} Result: {traceResults.loc[i,"追查情形"]}')
                continue
            else:
                traceResults.loc[i,'追查情形'] = f'疑似有{len(check)}筆'
                print(f'Tracing Layer {layer} - TXNO {count}/{traceLen} Result: {traceResults.loc[i,"追查情形"]}')
                for j,row_check in check.iterrows():
                    try:
                        if len(row_check[traceType+'Label']) > 0:
                            check_internal.loc[j,'追查情形'] = f"標記內容: {row_check[traceType+'Label']}"
                            continue
                    except TypeError:
                        pass
                    r1,r2 = trace_tx_tron(tronObj,row_check,traceType=traceType,
                                          traceLimit=traceLimit,
                                          totalLimit=totalLimit,
                                          ignoreAmount=ignoreAmount,
                                          traceTolerance=traceTolerance,
                                          debugMode=debugMode)
                    traceTXRes = pandas.concat([traceTXRes,r1])
                    check_internal.loc[j,'追查情形'] = r2
                    #print(f'Internal交易第{j}筆追查情形: {r2}')
            traceResults = pandas.concat([traceResults,check_internal])
        else:
            try:
                if len(row[traceType+'Label']) > 0:
                    traceResults.loc[i,'追查情形'] = f"標記內容: {row[traceType+'Label']}"
                    print(f'Tracing Layer {layer} - TXNO {count}/{traceLen} Result: {traceResults.loc[i,"追查情形"]}')
                    continue
            except TypeError:
                pass
            r1,r2 = trace_tx_tron(tronObj,row,traceType=traceType,traceLimit=traceLimit,
                                  totalLimit=totalLimit,ignoreAmount=ignoreAmount,
                                  traceTolerance=traceTolerance,debugMode=debugMode)
            traceTXRes = pandas.concat([traceTXRes,r1])
            traceResults.loc[i,'追查情形'] = r2
            print(f'Tracing Layer {layer} - TXNO {count}/{traceLen} Result: {traceResults.loc[i,"追查情形"]}')
        traceTXRes.reset_index(drop=True,inplace=True)
        traceTXRes.loc[:,'Layer'] = [row['Layer']+1 for _ in range(len(traceTXRes))]
        traceTXRes.loc[:,'TxNo'] = [f"{row['TxNo']}_{idx+1}" for idx in range(len(traceTXRes))]
        traceRes = pandas.concat([traceRes,traceTXRes])
    return traceRes,traceResults

def trace_txs_tron(tronObj,txs,layerNum=3,traceType='From',totalLimit=100000,
                   ignoreAmount=1,traceLimit=500,traceTolerance=1,tracePrintNum=10,
                   storeEachLayer=False,storePath='tmp//',debugMode=False):
    if storeEachLayer and not os.path.exists(storePath):
        os.makedirs(storePath)

    txs.reset_index(drop=True,inplace=True)
    if not 'FromLabel' in txs.columns or not 'ToLabel' in txs.columns:
        addrs_from = txs['From'].drop_duplicates().tolist()
        addrs_to = txs['To'].drop_duplicates().tolist()
        lookup_addrs(tronObj,addrs_from+addrs_to)
        prefix = 'From'
        for addr in addrs_from:
            lookup = addrLabels[addr]
            txs.loc[txs[prefix]==addr,[prefix+'Contract',prefix+'Label']] = [lookup['IsContract'],lookup['Label']]
        prefix = 'To'    
        for addr in addrs_to:
            lookup = addrLabels[addr]
            txs.loc[txs[prefix]==addr,[prefix+'Contract',prefix+'Label']] = [lookup['IsContract'],lookup['Label']]
    
    if 'Layer' in txs.columns and 'TxNo' in txs.columns and '追查情形' in txs.columns:
        traceResults = txs[columns+['Layer','TxNo','追查情形']].copy()
        layerCount = int(txs['Layer'].max())
    elif 'Layer' in txs.columns and 'TxNo' in txs.columns:
        traceResults = txs[columns+['Layer','TxNo']].copy()
        layerCount = int(txs['Layer'].max())
    else:
        traceResults = txs[columns].copy()
        traceResults['Layer'] = 0
        traceResults['TxNo'] = [f'{i+1}' for i in range(len(traceResults))]
        layerCount = 0

    while layerCount < layerNum:
        print(f'...Start Tracing Layer {layerCount}...')
        traceLayerResults,traceResults = trace_layer(tronObj,traceResults,layer=layerCount,traceType=traceType,traceLimit=traceLimit,traceTolerance=traceTolerance,tracePrintNum=tracePrintNum,totalLimit=totalLimit,ignoreAmount=ignoreAmount,debugMode=debugMode)
        traceResults = pandas.concat([traceResults,traceLayerResults])
        traceResults.reset_index(drop=True,inplace=True)
        layerCount += 1

        if storeEachLayer:
            try:
                traceResults.to_excel(f'{storePath}layer{layerCount}.xlsx',index=False)
            except PermissionError:
                nowtime = datetime.datetime.now()
                nowtime_str = nowtime.strftime('%Y%m%d%H%M%S')
                traceResults.to_excel(f'{storePath}layer{layerCount}_{nowtime_str}.xlsx',index=False)
    return traceResults