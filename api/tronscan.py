from .req_management import ReqManagement

import datetime

class Tronscan:
    def __init__(self,debugMode=False,sessionVerify=False):
        self.req_manage = ReqManagement(excludes=['message','Error'],debugMode=debugMode,sessionVerify=sessionVerify)
        
        self.url = 'https://apilist.tronscan.org/api/'

    def get_txinfo_by_hash(self,txid):
        params = {'hash':txid}
        res = self.req_manage.send_requests('get',self.url+'transaction-info',params=params)
        return res.json() #dict

    #API每次回覆50筆(limit)的資料量
    #transType=['TRC10','Internal','TRC20']
    def get_transfer_once(self,addr,start=datetime.datetime(2010,1,1),end=datetime.datetime.now(),transType='TRC10',startnum=0):
        params = {
            'count':'true','limit':50,'start':startnum,
            'start_timestamp':int(start.timestamp()*1000),
            'end_timestamp':int(end.timestamp()*1000)
        }
        if transType == 'TRC10':
            url = self.url+'transaction'
            params['address'] = addr
        elif transType == 'TRC20':
            url = self.url+'token_trc20/transfers'
            params['relatedAddress'] = addr
        elif transType == 'Internal':
            url = self.url+'internal-transaction'
            params['address'] = addr
        else:
            print(f'!!Undefined TransType:{transType}!!')
        res = self.req_manage.send_requests('get',url,params=params)
        return res.json() #dict

    def get_account_info(self,addr):
        params = {'address':addr}
        res = self.req_manage.send_requests('get',self.url+'account',params=params)
        return res.json() #dict

    def get_account_detailed_info(self,addr):
        params = {'address':addr}
        res = self.req_manage.send_requests('get',self.url+'accountv2',params=params)
        return res.json() #dict

    def get_contract_info(self,contractAddr):
        params = {'contract':contractAddr}
        res = self.req_manage.send_requests('get',self.url+'contract',params=params)
        return res.json() #dict