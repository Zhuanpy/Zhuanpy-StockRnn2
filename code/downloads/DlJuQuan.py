# -*- coding: utf-8 -*-
# from jqdatasdk import *

# auth('13975124715', '651748264Zz')

import pandas as pd


def JQ_code(code):
    if code[0] == '6':
        code = f'{code}.XSHG'

    elif code[0] == '0' or code[0] == '3':
        code = f'{code}.XSHE'

    else:
        print(f'股票:{code}JQ无市场分类；')

    return code


def fuquan_value(fq):
    if fq == '前复权':
        fq = 'pre'

    if fq == '后复权':
        fq = 'post'

    if fq == '不复权':
        fq = None

    return fq


class DownloadData:

    @classmethod
    def download_history_data(cls, code, start_date, end_date, frequency, fq_value):
        code = JQ_code(code)
        fq = fuquan_value(fq_value)
        download = get_price(code, start_date=start_date, end_date=end_date, frequency=frequency, fq=fq)
        if len(download):
            download = download.reset_index()
            download = download.rename(columns={'index': 'date'})
            # 标准化数据
            download.loc[:, 'date'] = pd.to_datetime(download['date'])
            float_column = ['open', 'close', 'high', 'low', 'volume', 'money']
            download[float_column] = download[float_column].astype(float)
            download[['volume', 'money']] = download[['volume', 'money']].astype('int64')
            st_col = ['date', 'open', 'close', 'high', 'low', 'volume', 'money']
            download = download[st_col]
        return download

    @classmethod
    def get_index_weight(cls, code, date='2021-08-01'):
        wh = get_index_weights(index_id=code, date=date).reset_index().sort_values(by=['index'])
        wh = wh.rename(columns={'index': 'stock_code', 'date': 'check_date', 'display_name': 'stock_name'})
        wh = wh[['stock_code', 'stock_name', 'weight', 'check_date']]
        return wh

    @classmethod
    def get_sw_lervel1_weight(cls, code, name):
        l1 = get_industry_stocks(code)
        l1 = pd.DataFrame(data=l1, columns=['stock_code'])
        l1.loc[:, 'industry_code'] = code
        l1.loc[:, 'industry_name'] = name
        return l1

    @classmethod
    def get_sw_lervel2_weight(cls, code, name):
        # code = '801011'
        # name = '林业II'
        l2 = get_industry_stocks(code)
        l2 = pd.DataFrame(data=l2, columns=['stock_code'])
        l2.loc[:, 'industry_code'] = code
        l2.loc[:, 'industry_name'] = name
        return l2

    @classmethod
    def get_all_securities(cls):
        data = get_all_securities(types=['stock'], date=None)
        return data


if __name__ == '__main__':
    st = DownloadData()
