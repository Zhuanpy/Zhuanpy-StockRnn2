# -*- coding: utf-8 -*-
import pandas as pd
from DB_MySql import MysqlAlchemy as alc
from DB_MySql import execute_sql

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)


class StockData15m:
    db_15m = 'stock_15m_data'

    @classmethod
    def load_15m(cls, code_: str):
        df = alc.pd_read(cls.db_15m, code_)
        return df

    @classmethod
    def append_15m(cls, code_: str, data):
        alc.pd_append(data, cls.db_15m, code_)

    @classmethod
    def replace_15m(cls, code_: str, data):
        alc.pd_replace(data, cls.db_15m, code_)

    @classmethod
    def data15m_execute_sql(cls, sql):
        execute_sql(cls.db_15m, sql)


class StockData1m:
    """
    _year: data start year ;
    year_: data end year;
    """

    @classmethod
    def load_1m(cls, code_: str, _year):

        y = int(pd.Timestamp('today').year)

        if len(_year) == 4:
            _year = f'{_year}-01-01'

        _year = int(pd.to_datetime(_year).year)

        df = pd.DataFrame()

        for i in range(y - _year + 1):
            db = f'data1m{_year + i}'
            tb = code_.lower()
            data_ = alc.pd_read(db, tb)
            df = pd.concat([df, data_], ignore_index=True)

        return df

    @classmethod
    def append_1m(cls, code_: str, year_: str, data):
        db = f'data1m{year_}'
        tb = code_.lower()
        alc.pd_append(data, db, tb)

    @classmethod
    def replace_1m(cls, code_: str, year_: str, data):
        db = f'data1m{year_}'
        tb = code_.lower()
        alc.pd_replace(data, db, tb)


class StockPoolData:
    db_pool = 'stockpool'

    tb_pool = 'stockpool'
    tb_poolCount = 'poolcount'
    tb_trade_record = 'traderecord'
    tb_board = 'board'

    @classmethod
    def load_StockPool(cls):
        df = alc.pd_read(cls.db_pool, cls.tb_pool)
        df['RecordDate'] = pd.to_datetime(df['RecordDate'])
        return df

    @classmethod
    def load_board(cls):
        df = alc.pd_read(cls.db_pool, cls.tb_board)
        df['RecordDate'] = pd.to_datetime(df['RecordDate'])
        return df

    @classmethod
    def load_poolCount(cls):
        df = alc.pd_read(cls.db_pool, cls.tb_poolCount)
        df['date'] = pd.to_datetime(df['date'])
        return df

    @classmethod
    def load_tradeRecord(cls):
        df = alc.pd_read(cls.db_pool, cls.tb_trade_record)
        return df

    @classmethod
    def append_tradeRecord(cls, data):
        alc.pd_append(data, cls.db_pool, cls.tb_trade_record)

    @classmethod
    def append_poolCount(cls, data):
        alc.pd_append(data, cls.db_pool, cls.tb_poolCount)

    @classmethod
    def pool_execute_sql(cls, sql):
        execute_sql(cls.db_pool, sql)

    @classmethod
    def replace_stock_pool(cls, data):
        alc.pd_replace(data=data, database=cls.db_pool, table=cls.tb_pool)

    @classmethod
    def replace_board(cls, data):
        alc.pd_replace(data=data, database=cls.db_pool, table=cls.tb_board)


class LoadNortFunds:

    db_funds = 'northfunds'
    tb_amount = 'amount'
    tb_toboard = 'toboard'
    tb_tostock = 'tostock'

    @classmethod
    def load_funds2board(cls):
        df = alc.pd_read(cls.db_funds, cls.tb_toboard)
        df['TRADE_DATE'] = pd.to_datetime(df['TRADE_DATE'])
        return df

    @classmethod
    def load_amount(cls):
        df = alc.pd_read(cls.db_funds, cls.tb_amount)
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        return df

    @classmethod
    def load_funds2stock(cls):
        df = alc.pd_read(cls.db_funds, cls.tb_tostock)
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        return df

    @classmethod
    def append_funds2board(cls, data):
        alc.pd_append(data, cls.db_funds, cls.tb_toboard)

    @classmethod
    def append_amount(cls, data):
        alc.pd_append(data, cls.db_funds, cls.tb_amount)

    @classmethod
    def append_funds2stock(cls, data):
        alc.pd_append(data, cls.db_funds, cls.tb_tostock)

    @classmethod
    def replace_funds2board(cls, data):
        alc.pd_replace(data, cls.db_funds, cls.tb_toboard)


class LoadRnnModel:

    db_rnn = 'rnn_model'
    tb_train_record = 'trainrecord'
    tb_run_record = 'runrecord'

    @classmethod
    def load_train_record(cls):
        data = alc.pd_read(cls.db_rnn, cls.tb_train_record)
        return data

    @classmethod
    def load_run_record(cls):
        data = alc.pd_read(cls.db_rnn, cls.tb_run_record)
        return data

    @classmethod
    def rnn_execute_sql(cls, sql):
        execute_sql(cls.db_rnn, sql)


class LoadFundsAwkward:

    db_funds_awkward = 'funds_awkward_stock'
    tb_funds_500 = 'topfunds500'
    tb_awkwardNormalization = 'awkward_normalization'
    tb_fundsAwkward = 'fundsawkward'

    @classmethod
    def load_awkwardNormalization(cls):
        df = alc.pd_read(cls.db_funds_awkward, cls.tb_awkwardNormalization)
        df['Date'] = pd.to_datetime(df['Date'])
        return df

    @classmethod
    def append_awkwardNormalization(cls, data):
        alc.pd_append(data, cls.db_funds_awkward, cls.tb_awkwardNormalization)

    @classmethod
    def load_top500(cls):
        df = alc.pd_read(cls.db_funds_awkward, cls.tb_funds_500)
        df['Date'] = pd.to_datetime(df['Date'])
        return df

    @classmethod
    def load_fundsAwkward(cls):
        df = alc.pd_read(cls.db_funds_awkward, cls.tb_fundsAwkward)
        df['Date'] = pd.to_datetime(df['Date'])
        return df

    @classmethod
    def append_fundsAwkward(cls, data):
        alc.pd_append(data, cls.db_funds_awkward, cls.tb_fundsAwkward)

    @classmethod
    def awkward_execute_sql(cls, sql):
        execute_sql(cls.db_funds_awkward, sql)


class LoadBasicInform:

    db_basic = 'stock_basic_information'
    tb_minute = 'record_stock_minute'
    tb_record_north_funds = 'recordnorthfunds'

    @classmethod
    def load_minute(cls):
        data = alc.pd_read(cls.db_basic, cls.tb_minute)
        return data

    @classmethod
    def load_record_north_funds(cls):
        data = alc.pd_read(cls.db_basic, cls.tb_record_north_funds)
        return data

    @classmethod
    def append_record_north_funds(cls, data):
        alc.pd_append(data, cls.db_basic, cls.tb_record_north_funds)

    @classmethod
    def basic_execute_sql(cls, sql):
        execute_sql(cls.db_basic, sql)


class LoadBasic:

    # stockbasic.basic

    db = 'stockbasic'
    tb_basic = 'basic'

    @classmethod
    def load_basic(cls):
        df = alc.pd_read(cls.db, cls.tb_basic)
        return df


if __name__ == '__main__':
    lf = LoadBasic()
    data = lf.load_basic()
    print(data)
