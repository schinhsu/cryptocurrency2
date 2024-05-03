import pandas
import datetime

# 統一輸出欄位
columns = ['BlockNo','TxID','Date(UTC+8)','From','To','Value','TxFee','Token','Contract','TXType']

def transform_balance(balance,decimalLen=18):
    decimalLen = int(decimalLen)
    balance = str(balance)
    balance = balance.replace('nan','')
    if balance.find('.') >= 0:
        return eval(balance)
    if len(balance) == 0:
        return 0
    try:
        integer = '0' if len(balance) <= decimalLen else balance[:len(balance)-decimalLen]
    except TypeError:
        print(type(balance),balance)
        print(len(balance),decimalLen)
        
    decimal = balance[len(integer):] if len(balance) > decimalLen else balance.zfill(decimalLen)

    balance = integer+'.'+decimal
    
    return eval(balance)

def json2df_eth(entries):
    global columns
    if len(entries) == 0:
        return pandas.DataFrame(columns=columns)
    
    df = pandas.json_normalize(entries)
    df['Date(UTC+8)'] = pandas.to_datetime(df['timeStamp'].astype(int),unit='s')+datetime.timedelta(hours=8)
    
    # Normal交易不會特別寫symbol和tokendecimal
    if not 'tokenSymbol' in df.columns:
        df['tokenSymbol'] = 'ETH'
    if not 'tokenDecimal' in df.columns:
        df['tokenDecimal'] = 18
    # Internal交易不會寫gasPrice
    if not 'gasPrice' in df.columns:
        df['gasPrice'] = 0
    
    df['tokenDecimal'] = pandas.to_numeric(df['tokenDecimal'])
    df['Value'] = df[['value','tokenDecimal']].apply(lambda x: transform_balance(x['value'], x['tokenDecimal']), axis=1)
    df['TxFee'] = df[['gasUsed','gasPrice']].apply(lambda x: eval(x['gasUsed'])*transform_balance(x['gasPrice']), axis=1)
    
    dfTrim = df[['blockNumber','hash','Date(UTC+8)','from','to','Value','TxFee','tokenSymbol','contractAddress']]
    dfTrim.columns = columns
    return dfTrim

def json2df_tron(entries,transType):
    global columns
    
    if len(entries) == 0:
        return pandas.DataFrame(columns=columns)

    df = pandas.json_normalize(entries)
    if transType == 'TRC10':
        #有些交易類型會沒有token(ex:vote...)
        df.dropna(subset=['tokenInfo.tokenId'],inplace=True)
        #展開toAddressList欄位
        df = df.explode('toAddressList')
        #展開後如果為空值需要去除(ex:update permission...)
        df.dropna(subset=['toAddressList'],inplace=True)
    
        #token名稱統一大寫
        df['tokenAbbr'] = df['tokenInfo.tokenAbbr'].str.upper()
        df['amount'] = df['amount'].apply(transform_balance,decimalLen=6)
        df['txfee'] = df['cost.fee'].apply(transform_balance,decimalLen=6)
        df['time'] = pandas.to_datetime(df['timestamp'],unit='ms')+datetime.timedelta(hours=8)
    
        dfTrim = df[['block','hash','time','ownerAddress','toAddressList','amount','txfee','tokenAbbr','tokenInfo.tokenType']]
        
    elif transType == 'Internal':
        #token名稱統一大寫
        df['tokenAbbr'] = df['token_list.tokenInfo.tokenAbbr'].str.upper()
        df['amount'] = df[['call_value','token_list.tokenInfo.tokenDecimal']].apply(lambda x: transform_balance(x['call_value'], x['token_list.tokenInfo.tokenDecimal']), axis=1)
        #此API沒有回覆手續費資訊
        df['txfee'] = 0
        df['time'] = pandas.to_datetime(df['timestamp'],unit='ms')+datetime.timedelta(hours=8)
        
        dfTrim = df[['block','hash','time','from','to','amount','txfee','tokenAbbr','token_list.tokenInfo.tokenType']]
    
    elif transType == 'TRC20':
        #不確定會不會有 沒有這兩個欄位的交易
        df.dropna(subset=['to_address','tokenInfo.tokenId'],inplace=True)
    
        #token名稱統一大寫
        df['tokenAbbr'] = df['tokenInfo.tokenAbbr'].str.upper()
        df['amount'] = df[['quant','tokenInfo.tokenDecimal']].apply(lambda x: transform_balance(x['quant'], x['tokenInfo.tokenDecimal']), axis=1)
        #此API沒有回覆手續費資訊
        df['txfee'] = 0
        df['time'] = pandas.to_datetime(df['block_ts'],unit='ms')+datetime.timedelta(hours=8)
        
        dfTrim = df[['block','transaction_id','time','from_address','to_address','amount','txfee','tokenAbbr','contract_address']]
    
    dfTrim.columns = ['BlockNo','TxID','Date(UTC+8)','From','To','Value','TxFee','Token','Contract']
    dfTrim.loc[:,['TXType']] = [transType for _ in range(len(dfTrim))]
    return dfTrim