#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日期    : 2022/7/11 9:39 AM
作者    : geed
功能    : 爬取连上充值到账时间
参数    :
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sys
import re
import time
import json
import datetime
import os

filepath = os.path.abspath(__file__)
filename = os.path.basename(filepath)
curr_dir = os.path.dirname(filepath)
bi_path = os.path.dirname(curr_dir)
if sys.path[sys.path.__len__() - 1] != bi_path:
    sys.path.append(bi_path)  # 引入新的模块路径
from common.util_clickhouse import ClickHouseDb
from common.util import Util


class DataProcess:
    def __init__(self, stime, log=None):
        self.__dict_type = {'ERC20': 'https://etherscan.io/tx/',
                            'ETH': 'https://etherscan.io/tx/',
                            'BEP20': 'https://bscxplorer.com/tx/',
                            'BNB': 'https://bscxplorer.com/tx/',
                            'TRC20': 'https://apilist.tronscanapi.com/api/transaction-info?hash=',
                            'TRX': 'https://apilist.tronscanapi.com/api/transaction-info?hash=',
                            'XGP': 'https://api.luniverse.io/scan/v1.0/chains/7806468005210300226/transactions/',
                            'XRP': 'https://api.xrpscan.com/api/v1/tx/',
                            'Polygon': 'https://polygonscan.com/tx/',
                            'EVMOS': 'https://evm.evmos.org/tx/',
                            'COSMOS': 'https://evm.evmos.org/tx/',
                            'AOK': 'https://api.aok.network/v2/transaction/',
                            'PLCU': 'https://api.plcultima.info/v2/public/tx?id=',
                            'SOL': 'https://api.solscan.io/transaction?tx=',
                            'BTC': 'https://blockstream.info/api/tx/',
                            'ATOM':'https://cosmos.lcd.atomscan.com/cosmos/tx/v1beta1/txs/'
                            }
        self.__ch_conn = ClickHouseDb('hw307')
        self.__ch_conn.connect()
        self.stime = datetime.datetime.strptime(stime, '%Y-%m-%d %H:%M:%S')
        if log is None:
            self.log = Util.get_logger("crawl_deposit_reachtime_%s" % (os.getpid()))

    @classmethod
    def epoch_convert(cls, epoch):
        if len(str(epoch)) >= 13:
            epoch = epoch / 1000
        reach_time = pd.to_datetime(datetime.datetime.fromtimestamp(epoch)).tz_localize('Asia/Shanghai',
                                                                                        nonexistent='NaT').tz_convert(
            'UTC')
        return reach_time

    @classmethod
    def find_reach_time(cls, content, type):
        if type == 'ETH' or type == 'ERC20' or type == 'Polygon':
            soup = BeautifulSoup(content, 'lxml')
            text = soup.findAll('div', class_='col-md-9')[3].text
            reach_time = re.findall(r'\(.*\)', text)[0][1:-9]
            reach_time = pd.to_datetime(reach_time, utc=True)
            if 'PM' in text and reach_time.hour != 12:
                reach_time = reach_time + datetime.timedelta(hours=12)
            elif 'AM' in text and reach_time.hour == 12:
                reach_time = reach_time - datetime.timedelta(hours=12)
            else:
                pass
        elif type == 'BEP20' or type == 'BNB':
            soup = BeautifulSoup(content, 'lxml')
            reach_time = pd.to_datetime(soup.findAll('p',class_='title is-6')[9].text)
        elif type == 'EVMOS' or type == 'COSMOS':
            soup = BeautifulSoup(content, 'lxml')
            reach_time = pd.to_datetime(
                soup.find('i', class_='fa-regular fa-clock').next.next.span.attrs['data-from-now'][:-1], utc=True)
        elif type == 'XRP':
            reach_time = pd.to_datetime(json.loads(content)['date'], utc=True)
        elif type == 'TRC20' or type == 'TRX':
            reach_time = cls.epoch_convert(json.loads(content)['timestamp'])
        elif type == 'AOK':
            reach_time = cls.epoch_convert(json.loads(content)['result']['timestamp'])
        elif type == 'XGP':
            reach_time = cls.epoch_convert(json.loads(content)['data']['transaction']['timestamp'])
        elif type == 'SOL':
            reach_time = cls.epoch_convert(json.loads(content)['blockTime'])
        elif type == 'BTC':
            reach_time = cls.epoch_convert(json.loads(content)['status']['block_time'])
        elif type == 'PLCU':
            reach_time = cls.epoch_convert(json.loads(content)['data']['time'])
        elif type == 'ATOM':
            reach_time = pd.to_datetime(json.loads(content)['tx_response']['timestamp'],utc=True)
        else:
            print('Invalid chain type!')
            sys.exit(1)
        return reach_time

    def get_time(self, df):
        reach_time_list = []
        x = 0
        headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.53 Safari/537.36',
            'cookie': ''}

        for i in range(df.shape[0]):
            t = 0
            type = df.loc[i, 'chain_type']
            if type in self.__dict_type:
                base_url = self.__dict_type[type]
                h = df.loc[i, 'hash_id']
            else:
                reach_time_list.append(pd.to_datetime('1970-01-01 08:00:00'))
                x += 1
                self.time_estimate(x, df.shape[0])
                continue

            while True:
                try:
                    url = base_url + h
                    content = requests.get(url, headers=headers)
                    reach_time = self.find_reach_time(content.content, type)
                    reach_time = pd.to_datetime(reach_time).tz_convert('Asia/Shanghai')
                    reach_time_list.append(reach_time)
                    break
                except IndexError as err:
                    self.log.error(f"url request error: {err}")
                    if t <= 5:
                        time.sleep(2)
                        t += 1
                    else:
                        reach_time_list.append(pd.to_datetime('1970-01-01 08:00:00'))
                        break
                except Exception as ex:
                    self.log.error(f"url request error: {ex}")
                    reach_time_list.append(pd.to_datetime('1970-01-01 08:00:00'))
                    break
            x += 1
            self.time_estimate(x, df.shape[0])
        df['reach_time'] = pd.Series(reach_time_list)
        return df

    @classmethod
    def time_estimate(cls, x, y):
        t = 0.5 * (y - x)
        h = int(t // 3600)
        m = int((t - 3600 * h) // 60)
        s = int(t - 3600 * h - 60 * m)
        str_t = '{:0>2d}'.format(h) + 'h ' + '{:0>2d}'.format(m) + 'm ' + '{:0>2d}'.format(s) + 's'
        str_p = '{:.2%}'.format(x / y)
        print('\r', end='')
        print(str_p + ' completed  Estimated Remaining Time: ' + str_t, end='')
        sys.stdout.flush()

    @classmethod
    def read_data(cls, df):
        df['check_time'] = pd.to_datetime(df['check_time']).dt.tz_localize('Asia/Shanghai')
        df['check_time'] = np.where(df['check_time'] == pd.to_datetime('1970-01-01 08:00:00'), pd.NaT,
                                    df['check_time'])
        df['if_check'] = np.where(df['check_time'].isna(), 1, 0)
        return df

    @classmethod
    def cb_generate(cls, df):
        df2 = df.loc[:, ['hash_id', 'if_check', 'chain_type', 'currency', 'num']]
        df2['time1'] = df['add_time'] - df['reach_time']
        df2['time2'] = df['check_time'] - df['add_time']
        df2['time3'] = np.where(df['check_time'].isna() == False, df['update_time'] - df['check_time'],
                                df['update_time'] - df['add_time'])
        df2['total'] = np.where(df2['time2'].isna() == False, df2['time1'] + df2['time2'] + df2['time3'],
                                df2['time1'] + df2['time3'])
        return df2

    @classmethod
    def tb_generate(cls, df):
        df2 = df.loc[:, ['hash_id', 'if_check', 'chain_type', 'currency', 'num']]
        df2['time1'] = df['check_time'] - df['add_time']
        df2['time2'] = np.where(df['check_time'].isna() == False, df['update_time'] - df['check_time'],
                                df['update_time'] - df['add_time'])
        df2['total'] = np.where(df2['time1'].isna() == False, df2['time1'] + df2['time2'], df2['time2'])
        return df2

    @classmethod
    def finalize(cls, df):
        df['time_range'] = np.where(df['total'] <= '5m', '000-005',
                                    np.where((df['total'] > '5m') & (df['total'] <= '10m'), '005-010',
                                             np.where((df['total'] > '10m') & (df['total'] <= '30m'), '010-030',
                                                      np.where((df['total'] > '30m') & (df['total'] <= '1h'), '030-060',
                                                               np.where((df['total'] > '1h') & (df['total'] < '2h'),
                                                                        '060-120',
                                                                        np.where(df['total'].isna(), '',
                                                                                 '120+'))))))
        return df

    def get_deposit_data(self):
        sql = """
                select hash_id, currency, num, add_time, check_time, update_time, chain_type, if_deposit_or_withdraw
                from df02.dwd_deposit_withdraw_chain 
                where toDate(update_time) = toDate('{}')""".format(self.stime)
        rst = self.__ch_conn.execute(sql)
        return rst

    def load_data_ch(self, df):
        sql = "insert into df02.dwd_deposit_withdraw_time VALUES"
        self.__ch_conn.batch_insert(sql, df.to_dict('records'))

    def clean_data(self):
        clean_sql = "alter table df02.dwd_deposit_withdraw_time delete where toDate(update_time) = toDate('{}')".\
            format(self.stime)
        self.log.info("clean sql:" + clean_sql)
        self.__ch_conn.execute(clean_sql)


def main():
    if len(sys.argv) < 2:
        print("参数个数错误")
        exit(1)
    stime = sys.argv[1]
    dp = DataProcess(stime)
    dp.clean_data()
    rst = dp.get_deposit_data()
    if rst['code']:
        df = pd.DataFrame(columns=['hash_id', 'currency', 'num', 'add_time', 'check_time', 'update_time', 'chain_type',
                                   'if_deposit_or_withdraw'],
                     data=rst['data'])
        cb = dp.read_data(df)
        cb_new = dp.get_time(cb)
        dp.load_data_ch(cb_new)
    else:
        dp.log.info("获取充值记录失败")


if __name__ == '__main__':
    main()
