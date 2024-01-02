import time
from DlEastMoney import DownloadData as dle
import pandas as pd
from code.MySql.LoadMysql import StockData1m, LoadBasicInform, LoadNortFunds
import datetime

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 5000)


def stock_1m(code, days):
    """
        download stock data 1m data , return download data & data end date ;
    """

    try:
        data = dle.stock_1m_days(code, days=days)
        date_ = data.iloc[-1]['date'].date()

    except Exception as ex:
        print(f'从东方财富下载{code}异常：{ex};')
        return pd.DataFrame(), None

    return data, date_  # date_:  data end date;


def board_1m(code, days):
    """
    download board 1m data , return download data & data end date ;
    """

    try:
        data = dle.board_1m_multiple(code, days=days)
        date_ = data.iloc[-1]['date'].date()

    except Exception as ex:
        print(f'从东方财富下载{code}异常：{ex};')
        return pd.DataFrame(), None

    return data, date_  # date_:  data  end date;


class DataDailyRenew:
    """
    近期数据更新

    """

    @classmethod
    def download_1mData(cls):

        """
        download 1m data , every day running method;
        """
        today = datetime.date.today()

        shapes = 1
        current = pd.to_datetime(today)  # .date()
        # print(type(current))
        # exit()
        while shapes:

            record = LoadBasicInform.load_minute()  # alc.pd_read(database=db_basic, table=tb_basic)

            record['StartDate'] = pd.to_datetime(record['StartDate'])
            record['EndDate'] = pd.to_datetime(record['EndDate'])
            record['RecordDate'] = pd.to_datetime(record['RecordDate'])

            dl1 = record[(record['Classification'] == '行业板块') &
                         (record['EndDate'] < current)]

            dl2 = record[(record['Classification'] != '行业板块') &
                         (record['StartDate'] < pd.to_datetime('2020-01-01')) &
                         (record['EndDate'] < current)]

            dl = pd.concat([dl1, dl2], ignore_index=True).sort_values(by=['EndDate']).reset_index(drop=True)

            shapes = dl.shape[0]

            if not shapes:
                print('已是最新数据')
                break

            for i in dl.index:
                id_ = dl.loc[i, 'id']
                name = dl.loc[i, 'name']
                escode = dl.loc[i, 'EsCode']
                code_ = dl.loc[i, 'code']
                classification = dl.loc[i, 'Classification']

                _ending = dl.loc[i, 'EndDate']

                ending = None  # 下载数据的日期

                days = current - _ending

                days = days.days

                if not days:
                    print(f'无最新1m数据: {name}, {code_};')
                    continue

                days = min(5, days)

                data = pd.DataFrame()  # 1m数据

                print(f'\n下载进度：\n总股票数: {dl.shape[0]}个; 剩余股票: {dl.shape[0] - i}个;')

                if classification != '行业板块':  # 下载个股1m数据
                    data, ending = stock_1m(escode, days)

                if classification == '行业板块':
                    data, ending = board_1m(escode, days)

                    if data.shape[0]:
                        data.loc[:, 'money'] = 0

                if data.shape[0]:
                    select = pd.to_datetime(_ending + pd.Timedelta(days=1))
                    data = data[data['date'] > select]

                # 判断是否保存数据 及 更新记录表格
                ending = pd.to_datetime(ending)

                if data.shape[0] and ending > _ending:

                    try:
                        # 有时保存数据会出现未知错误;
                        # 保存数据， 保存 1m数据;
                        year_ = ending.year
                        StockData1m.append_1m(code_=code_, year_=str(year_), data=data)

                        sql = f'''update {LoadBasicInform.db_basic}.{LoadBasicInform.tb_minute} 
                        set EndDate='{ending}', RecordDate = '{current}', 
                        EsDownload = 'success' where id={id_}; '''
                        LoadBasicInform.basic_execute_sql(sql)

                    except Exception as ex:
                        sql = f'''update {LoadBasicInform.db_basic}.{LoadBasicInform.tb_minute} 
                        set RecordDate = '{current}', EsDownload = 'failed' where id={id_}; '''
                        LoadBasicInform.basic_execute_sql(sql)
                        print(f'股票：{name}, {code_}存储数据异常: {ex}')

                if data.shape[0] and ending == _ending:
                    # data record date equal download end date ,just renew record date
                    sql = f'''update {LoadBasicInform.db_basic}.{LoadBasicInform.tb_minute} set 
                    EndDate = '{ending}', RecordDate = '{current}' where id = {id_}; '''
                    LoadBasicInform.basic_execute_sql(sql)

                    print(f'{LoadBasicInform.tb_minute} 数据更新成功: {name}, {code_}')

                time.sleep(2)

    @classmethod
    def renew_NorthFunds(cls):
        """
        更新北向资金数据
        renew North funds data;
        """

        current = pd.Timestamp('today').date()
        tables = ['tostock', 'amount', 'toboard']

        record = LoadBasicInform.load_record_north_funds()
        print(record)

        for index in record.index:

            table = record.loc[index, 'name']
            id_ = record.loc[index, 'id']

            _ending = record.loc[index, 'ending_date']
            _current = record.loc[index, 'renew_date']

            ending = None

            if current <= _current:
                print(f'无新数据:{table}')
                continue

            try:

                if table == tables[0]:  # 北向资金流入个股数据；

                    data = dle.funds_to_stock()  # 下载数据

                    if not data.shape[0]:
                        break

                    ending = data.iloc[-1]['trade_date']
                    LoadNortFunds.append_funds2stock(data)

                if table == tables[1]:  # 北向资金日常数据

                    data = dle.funds_daily_data()  # 下载数据

                    if not data.shape[0]:
                        break

                    ending = data.iloc[-1]['trade_date']
                    LoadNortFunds.append_amount(data)

                if table == tables[2]:  # 北向资金流入板块数据
                    days = current - _ending
                    days = days.days
                    print(days)

                    data = pd.DataFrame()

                    for i in range(int(days)):
                        i = i + 1
                        date_ = _ending + pd.Timedelta(days=i)
                        date_ = date_.strftime('%Y-%m-%d')
                        dl = dle.funds_to_sectors(date_)  # 下载无最新数据时

                        if not dl.shape[0]:
                            continue

                        data = pd.concat([data, dl], ignore_index=True)

                    # 判断是否有下载数据，无下载数据跳出循环，有下载数据继续更新
                    if not data.shape[0]:
                        break

                    ending = data.iloc[-1]['TRADE_DATE']
                    LoadNortFunds.append_funds2board(data)

                # 更新 保存 record 数据
                if not ending or ending <= _ending:
                    print(f'{table}无最新数据;')
                    continue

                sql = f'''update {LoadBasicInform.db_basic}.{LoadBasicInform.tb_record_north_funds} set 
                ending_date = '{ending}', renew_date='{current}' where id={id_}; '''
                LoadBasicInform.basic_execute_sql(sql=sql)
                print(f'{table}数据更新成功;')

            except Exception as ex:
                print(f'{table} 数据更新异常:\n{ex}')


class RMDownloadData(DataDailyRenew):
    """
    daily running
    """

    def __init__(self):
        super().__init__()
        # DataDailyRenew.__init__(self)

    def daily_renew_data(self):

        current_time = pd.Timestamp('today')
        market_open = pd.to_datetime('09:30')
        market_close = pd.to_datetime('15:30')

        if current_time < market_open:
            self.renew_NorthFunds()  # 北向资金信息

        elif current_time > market_close:
            self.download_1mData()  # 更新股票当天1m信息；
            self.renew_NorthFunds()  # 北向资金信息


if __name__ == '__main__':
    rn = DataDailyRenew()
    # rn.download_1mData()
    rn.renew_NorthFunds()
