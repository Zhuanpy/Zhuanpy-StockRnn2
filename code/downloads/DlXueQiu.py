# -*- coding: utf-8 -*-
import json
from download_utils import page_source
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 5000)


def setting_cookies():
    cookies = {}
    f = open(r'E:\Python\Project\Stock\Project_01\code\Dl_Strategy\plug\cookie_xueqiu.txt', 'r')
    for line in f.read().split(';'):
        name, value = line.strip().split('=', 1)
        cookies[name] = value
    return cookies


def market_code(stock_code):
    if stock_code[0] == '6':
        mk = 'SH'

    elif stock_code[0] == '0' or stock_code[0] == '3':
        mk = 'SZ'

    else:
        mk = None
        print(f'雪球无市场类股票：{stock_code};')

    return mk


headers = {'Accept': 'application/json, text/plain, */*',
           'Accept-Encoding': 'gzip, deflate, br',
           'Accept-Language': 'en-US,en;q=0.9',
           'Connection': 'keep-alive',
           'Host': 'stock.xueqiu.com',
           'Origin': 'https://xueqiu.com',
           'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) '
                         'AppleWebKit/537.36 (KHTML, like Gecko) '
                         'Chrome/89.0.4389.90 Safari/537.36'}

cookies = setting_cookies()


class DownloadData:

    @classmethod
    def stock_1m_data(cls, stock_name, stock_code, days=1):
        web_title = 'https://stock.xueqiu.com/v5/stock/chart/minute.json?'
        stock_market = market_code(stock_code)
        web_symbol = f'symbol={stock_market}{stock_code}'  # SZ002475
        web_days = '&period={}d'.format(days)
        web_site = f'{web_title}{web_symbol}{web_days}'

        pagesource = page_source(web_site, headers, cookies)
        json_data = json.loads(pagesource.text)
        data = pd.DataFrame(json_data['data']['items'])

        data.loc[:, 'timestamp'] = pd.to_datetime(data['timestamp'].values, unit='ms', utc=True
                                                  ).tz_convert('Asia/Shanghai').strftime('%Y-%m-%d %H:%M:%S')

        data.loc[:, 'timestamp'] = pd.to_datetime(data['timestamp'])

        data = data[['timestamp', 'avg_price', 'current', 'high', 'low', 'volume', 'amount']]

        columns_list = ['date', 'open', 'close', 'high', 'low', 'volume', 'money']
        data.columns = columns_list

        flt = ['open', 'close', 'high', 'low', 'volume']
        data[flt] = data[flt].astype(float)

        data['volume'] = data['volume'] / 100

        data[['volume', 'money']] = data[['volume', 'money']].astype('int64')
        data['volume'] = data['volume'] * 100
        # 删除 09：30 时间数据
        data.loc[1, 'volume'] = data.loc[0, 'volume'] + data.loc[1, 'volume']
        data.loc[1, 'money'] = data.loc[0, 'money'] + data.loc[1, 'money']

        data = data.iloc[1:].reset_index(drop=True)

        print(f'雪球下载1m数据成功: {stock_name};')
        return data

    @classmethod
    def stock_daily_data(cls, stock_name, stock_code, count=100):
        title = 'https://stock.xueqiu.com/v5/stock/chart/kline.json?'
        market = market_code(stock_code)
        web_name = f'symbol={market}{stock_code}'

        begin = round((pd.Timestamp('today') + pd.Timedelta(days=1)).timestamp() * 1000)
        web_begin = f'&begin={begin}&period=day&type=before&count=-{count}'

        web_end = '&indicator=kline,pe,pb,ps,pcf,market_capital,agt,ggt,balance'
        web_site = f'{title}{web_name}{web_begin}{web_end}'

        pagesource = page_source(web_site, headers, cookies)
        json_data = json.loads(pagesource.text)
        data = pd.DataFrame(json_data['data']['item'], columns=json_data['data']['column'])

        data.loc[:, 'timestamp'] = pd.to_datetime(data['timestamp'].values,
                                                  unit='ms', utc=True).tz_convert('Asia/Shanghai').date

        data = data.rename(columns={'timestamp': 'date', 'amount': 'money'})

        data = data[['stock_code', 'stock_name', 'date', 'open',
                     'close', 'high', 'low', 'volume', 'money',
                     'chg', 'percent', 'turnoverrate', 'pe', 'pb', 'ps', 'pcf',
                     'market_capital', 'balance', 'hold_volume_cn', 'hold_ratio_cn', 'net_volume_cn']]

        flt = ['open', 'close', 'high', 'low', 'chg', 'percent', 'turnoverrate', 'pe', 'pb', 'ps', 'pcf',
               'market_capital', 'balance', 'hold_volume_cn', 'hold_ratio_cn', 'net_volume_cn']

        data[flt] = data[flt].astype(float)
        data[['volume', 'money']] = data[['volume', 'money']].astype(int)

        print(f'雪球下载daily数据成功: {stock_name};')

        return data

    @classmethod
    def stock_120m_data(cls, stock_name, stock_code, count):
        mk = market_code(stock_code)
        title = f'https://stock.xueqiu.com/v5/stock/chart/kline.json?symbol={mk}{stock_code}&'

        begin = round((pd.Timestamp('today') + pd.Timedelta(days=1)).timestamp() * 1000)

        begin_date = f'begin={begin}&period=120m&type=before&count=-{count}&'

        web_end = 'indicator=kline,pe,pb,ps,pcf,market_capital,agt,ggt,balance'

        web_site = f'{title}{begin_date}{web_end}'

        source = page_source(web_site, headers, cookies)
        json_data = json.loads(source.text)
        data = pd.DataFrame(json_data['data']['item'], columns=json_data['data']['column'])
        data.loc[:, 'timestamp'] = pd.to_datetime(data['timestamp'], unit='ms') + pd.Timedelta(hours=8)

        data = data.rename(columns={'timestamp': 'trade_date', 'amount': 'money'})
        data = data[['trade_date', 'open', 'close', 'high', 'low', 'volume', 'money']]
        flt = ['open', 'close', 'high', 'low', 'volume', 'money']
        data[flt] = data[flt].astype(float)

        print(f'雪球下载120m数据成功: {stock_name};')
        return data


if __name__ == '__main__':
    st = DownloadData()
