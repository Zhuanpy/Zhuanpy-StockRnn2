import multiprocessing
import time
import pandas as pd
from code.MySql.LoadMysql import LoadFundsAwkward as aw
from code.MySql.LoadMysql import StockPoolData as pl
from DlEastMoney import DownloadData as dle


class DownloadFundsAwkward:
    """
    # 函数说明
    # 下载数据 DownloadFundsAwkwardData
    # 1.下载排名前近3年收益排名前300的基金数据。
    # 2.下载此基金数据重仓前10的股票数据。
    """

    def __init__(self, Dl_Date):

        self.DlDate = pd.to_datetime(Dl_Date).date()
        self.pending = None
        # self.db = 'funds_awkward_stock'

    def pending_data(self):
        record = aw.load_top500()  # alc.pd_read(database=self.db, table=tb)

        pending = record[(record['Date'] == self.DlDate)]

        if not pending.shape[0]:
            st = 'pending'
            record.loc[:, 'Date'] = self.DlDate
            record.loc[:, 'Status'] = st
            id_ = record.sort_values(by=['id']).iloc[0]['id']

            sql = f''' update {aw.db_funds_awkward}.{aw.tb_funds_500} 
            set Date = '{self.DlDate}', Status='{st}' where id >= '{id_}';'''
            aw.awkward_execute_sql(sql=sql)

        pending = record[(record['Date'] == self.DlDate) & (record['Status'] != 'success')].reset_index(drop=True)
        return pending

    def awkward_top10(self, start: int, end: int):

        num = 0

        for index in range(start, end):
            funds_name = self.pending.loc[index, 'Name']
            funds_code = self.pending.loc[index, 'Code']
            id_ = self.pending.loc[index, 'id']
            select = self.pending.loc[index, 'Selection']

            print(f'回测进度：\n总股票数:{end - start}个; 剩余股票: {end - start - num}个;\n当前股票：{funds_name},{funds_code};')
            try:
                data = dle.funds_awkward(funds_code)

            except Exception as ex:
                print(f'Dl EastMoney funds_awkward error: {ex}')
                data = dle.funds_awkward_by_driver(funds_code)

            if data.shape[0]:
                data.loc[:, 'funds_name'] = funds_name
                data.loc[:, 'funds_code'] = funds_code
                data.loc[:, 'Date'] = self.DlDate
                data.loc[:, 'Selection'] = select

                data = data[['stock_name', 'funds_name', 'funds_code', 'Date', 'Selection']]
                aw.append_fundsAwkward(data)

                sql = f'''update {aw.db_funds_awkward}.{aw.tb_funds_500} set Status = 'success' where id = '{id_}';'''
                print(f'{funds_name} data download success;\n')

            else:
                sql = f'''update {aw.db_funds_awkward}.{aw.tb_funds_500} set Status = 'failed' where id = '{id_}';'''
                print(f'{funds_name} data download failed;\n')

            aw.awkward_execute_sql(sql=sql)

            num += 1
            time.sleep(5)

    def multi_processing(self):
        self.pending = self.pending_data()
        print(self.pending.tail())
        indexes = self.pending.shape[0]
        if indexes:

            if indexes > 3:
                index1 = indexes // 3
                index2 = indexes // 3 * 2

                p1 = multiprocessing.Process(target=self.awkward_top10, args=(0, index1,))
                p2 = multiprocessing.Process(target=self.awkward_top10, args=(index1, index2,))
                p3 = multiprocessing.Process(target=self.awkward_top10, args=(index2, indexes,))

                p1.start()
                p2.start()
                p3.start()

            else:
                p1 = multiprocessing.Process(target=self.awkward_top10, args=(0, indexes,))
                p1.start()


class AnalysisFundsAwkward:
    """
    # 函数说明
    # 分析统计数据 AnalysisFundsAwkwardData
    # 1.统计出股票池；
    # 2.找出基金增持股票；
    # 3.找出基金减持股票；
    # 4.找出板块变动数据；
    """

    def __init__(self, dl_date):

        self.DlDate = pd.to_datetime(dl_date)  # .date()
        self.pool = pl.load_StockPool()  # database='StockPool', table='StockPool'

        self.awkward = aw.load_fundsAwkward()  # database='funds_awkward_stock', table='FundsAwkward'
        self.awkward = self.awkward[self.awkward['Selection'] == 1]

        self.num_max = 200
        self.num_min = 1

        self.count_dic = {}

    def normalization_all_data(self):

        for index in self.pool.index:
            stock_name = self.pool.loc[index, 'name']
            id_ = self.pool.loc[index, 'id']
            data_ = self.awkward[self.awkward['stock_name'] == stock_name].groupby('Date').count().reset_index()
            data_.loc[:, 'stock_name'] = stock_name

            data_ = data_.rename(columns={'Selection': 'count'})
            data_['TrendCount'] = data_['count'] - data_['count'].shift(1)
            data_['score'] = round((data_['count'] - self.num_min) / (self.num_max - self.num_min), 4)
            data_ = data_[['stock_name', 'count', 'TrendCount', 'score', 'Date']]

            if data_.shape[0]:
                score = data_.iloc[0]['score']
                aw.append_awkwardNormalization(data_)

            else:
                score = 0

            # 更新股票池基金得分
            sql = f'''update {pl.db_pool}.{pl.tb_pool} set FundsAwkward = {score} where id = '{id_}' ;'''
            pl.pool_execute_sql(sql)
            self.count_dic[stock_name] = score

        print(f'Success count: {self.count_dic}')

    def normalization_last(self):

        awkward = self.awkward[self.awkward['Date'] == self.DlDate]
        print(self.pool.head())

        if awkward.shape[0]:
            for index in self.pool.index:
                stock_name = self.pool.loc[index, 'name']
                stock_id = self.pool.loc[index, 'id']

                data_ = self.awkward[self.awkward['stock_name'] == stock_name
                                     ].groupby('Date').count().tail(3).reset_index()

                data_.loc[:, 'stock_name'] = stock_name
                data_ = data_.rename(columns={'Selection': 'count'})
                data_.loc[:, 'TrendCount'] = data_['count'] - data_['count'].shift(1)
                data_.loc[:, 'score'] = round((data_['count'] - self.num_min) / (self.num_max - self.num_min), 4)
                data_ = data_[['stock_name', 'count', 'TrendCount', 'score', 'Date']].tail(1)

                if data_.shape[0]:
                    score = data_.iloc[0]['score']
                    aw.append_awkwardNormalization(data_)

                else:
                    score = 0

                # 更新股票池基金得分
                sql = f'''update {pl.db_pool}.{pl.tb_pool} set FundsAwkward={score} where id='{stock_id}';'''
                pl.pool_execute_sql(sql)
                self.count_dic[stock_name] = score

            print(f'Success count: {self.count_dic}')


if __name__ == '__main__':
    DlDate = '2022-04-09'
    aly = AnalysisFundsAwkward(dl_date=DlDate)
    aly.normalization_last()
