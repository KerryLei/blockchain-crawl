#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug  3 09:41:17 2022

@author: geed
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re
from datetime import datetime
import traceback
import pytz
import time

class coingecko:
    
    def __init__(self):
        
        self.headers = {"cookie":"",
                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36"
                       }
        self.columns = ['stime','rank','name','type','trust_score','norm_volume(BTC)','volume(BTC)','visits','coins','pairs']
        
        self.exchanges = pd.DataFrame(columns=self.columns)

    def get_exc_info(self):
        try:
            stime = pd.to_datetime(datetime.now(tz=pytz.UTC).strftime('%Y-%m-%d %H:%M:%S'),utc=True)
            exchanges = pd.DataFrame(columns=self.columns)
            
            for i in range(1,6):
                link = 'https://www.coingecko.com/en/exchanges?page=' + str(i)
            
                page = requests.get(link,headers=self.headers).content
                soup = BeautifulSoup(page,'lxml')
                    
                record = soup.find('tbody').findAll('tr')
            
                for r in record:
                    t = re.sub(' ','',r.text).split()
                    t = [re.sub('[^0-9A-Za-z\.]','',x) for x in t]
                    if len(t) == 10:
                        t = t[:6] + t[7:]
                    exchanges.loc[len(exchanges.index),self.columns[1:]] = t
        
            exchanges['stime'] = stime
            exchanges = pd.DataFrame(np.where(exchanges=='NA',np.nan,exchanges),columns=self.columns)
            
            self.exchanges = pd.concat([self.exchanges,exchanges],ignore_index=True)
            self.exchanges.to_csv('/Users/geed/Desktop/DF/CMC/exchanges.csv',index=False)
            
        except:
            traceback.print_exc()
            pass
        

if __name__ == '__main__':
    gec = coingecko()
    gec.get_exc_info()
    
        















