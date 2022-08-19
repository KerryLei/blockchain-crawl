#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug  2 15:36:05 2022

@author: geed
"""
import requests
import json
from bs4 import BeautifulSoup
import pandas as pd
import traceback
import numpy as np
from datetime import datetime
import time
import pytz

class cmc_info(object):

    def __init__(self):
        self.coin_columns = ['time','lastUpdated','symbol','name','price','percentChange24h','volume24h',
                             'turnover','dominance','cmcRank','marketCap','circulatingSupply']
        self.ex_columns = ['time','lastUpdated','rank','name','score',
                           'spotVol24h','derivativesVol24h','totalVol24h','totalVolChgPct24h',
                           'numMarkets','marketSharePct','numCoins','liquidity',
                           'derivativesMarketPairs','derivativesOpenInterests','makerFee','takerFee',
                           'visits','fiats']
        self.coins = pd.DataFrame(columns=self.coin_columns)
        self.exchanges = pd.DataFrame(columns=self.ex_columns)        
        
    
    def get_crypto(self):
        url = 'https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing?'
        parameters = {
          'start':'1',
          'limit':'400',
          'convert':'USD',
          'sortBy':'rank',
          'sortType':'desc',
          'aux':'high24h,low24h,num_market_pairs,cmc_rank,date_added,circulating_supply'
        }
        
        # headers = {
        #   'Accepts': 'application/json',
        #   'X-CMC_PRO_API_KEY': '6bac0bdb-5bcd-410e-8fc6-677332ff6f59',
        # }
        
        page = requests.get(url,params=parameters)
        info = json.loads(page.content)
        
        time = pd.to_datetime(datetime.now(tz=pytz.UTC).strftime('%Y-%m-%d %H:%M:%S'),utc=True)
        
        for coin in info['data']['cryptoCurrencyList']:
            values = [time]
            for k in self.coin_columns[1:]:
                try:
                    values.append(coin[k])
                except KeyError:
                    try:
                        values.append(coin['quotes'][0][k])
                    except Exception as e:
                        traceback.print_exc()
            self.coins.loc[len(self.coins)] = values
            
        self.coins['lastUpdated'] = pd.to_datetime(self.coins['lastUpdated'],utc=True)
        
        self.coins.to_csv('Coins.csv',index=False)

        
        
    
    def get_exchanges(self):
        url = 'https://coinmarketcap.com/rankings/exchanges/'
        page = requests.get(url)
        soup = BeautifulSoup(page.content,'lxml')
        exs = json.loads(soup.find('script',id='__NEXT_DATA__').text)['props']['pageProps']['exchange']
        
        time = pd.to_datetime(datetime.now(tz=pytz.UTC).strftime('%Y-%m-%d %H:%M:%S'),utc=True)
        
        for ex in exs:
            values = [time]
            for k in self.ex_columns[1:]:
                try:
                    if ex[k] is None:
                        raise KeyError
                    if type(ex[k]) == list:
                        ex[k] = ','.join(ex[k])
                    values.append(ex[k])
                except KeyError:
                    values.append(np.nan)
            self.exchanges.loc[len(self.exchanges)] = values
    
        self.exchanges['lastUpdated'] = pd.to_datetime(self.exchanges['lastUpdated'],utc=True)
        
        self.exchanges.to_csv('Exchanges.csv',index=False)



if __name__ == '__main__':
    cmc = cmc_info()
    while True:
        cmc.get_crypto()
        cmc.get_exchanges()
        time.sleep(60)
















