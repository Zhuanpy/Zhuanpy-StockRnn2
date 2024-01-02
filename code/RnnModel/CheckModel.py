# -*- coding: utf-8 -*-
import pandas as pd
from RnnRunModel import PredictionCommon
from code.MySql.LoadMysql import LoadRnnModel, StockPoolData
from code.MySql.sql_utils import Stocks
import matplotlib.pyplot as plt
import multiprocessing
from code.Evaluation.CountPool import PoolCount
from Rnn_utils import reset_id_time, reset_record_time, date_range

plt.rcParams['font.sans-serif'] = ['FangSong']
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)


def stock_evaluate(day_, _num, num_, data, months, check_model):
    count = 0

    for index in range(_num, num_):

        db = LoadRnnModel.db_rnn
        tb = LoadRnnModel.tb_train_record

        print(f'当前进度，剩余{num_ - _num - count}；')

        stock_ = data.loc[index, 'code']
        id_ = data.loc[index, 'id']
        check_date = pd.Timestamp('now').date()

        try:

            run = PredictionCommon(Stock=stock_, months=months, monitor=False, check_date=day_)
            run.single_stock()

            if check_model:
                sql2 = f'''update {db}.{tb} set 
                ModelCheckTiming = '{pd.Timestamp.now()}',
                ModelCheck = 'success',
                ModelError = 'success',
                ModelCheckTiming = '{check_date}' where id={id_};'''
                LoadRnnModel.rnn_execute_sql(sql2)

        except Exception as ex:

            print(f'Error: {ex}')

            if check_model:
                sql2 = f'''update {db}.{tb} set 
                ModelCheck = 'error',
                ModelError = 'error',
                ModelCheckTiming = '{check_date}' where id={id_};'''
                LoadRnnModel.rnn_execute_sql(sql2)

        count += 1


def multiprocessing_count_pool(day_, months='2022-02', check_model=False):
    data = StockPoolData.load_StockPool()

    shape_ = data.shape[0]

    print(f'处理日期{day_}， 处理个数：{shape_}')

    l1 = shape_ // 3
    l2 = shape_ // 3 * 2

    p1 = multiprocessing.Process(target=stock_evaluate, args=(day_, 0, l1, data, months, check_model,))
    p2 = multiprocessing.Process(target=stock_evaluate, args=(day_, l1, l2, data, months, check_model,))
    p3 = multiprocessing.Process(target=stock_evaluate, args=(day_, l2, shape_, data, months, check_model,))

    p1.start()
    p2.start()
    p3.start()

    p1.join()
    p2.join()
    p3.join()


class RMHistoryCheck:

    def __init__(self, _date=None, date_=None, months='2022-02'):

        self.months = months

        if not date_:
            self.date_ = pd.Timestamp.now().date()

        else:
            self.date_ = pd.to_datetime(date_).date()

        if not _date:
            self._date = self.date_

        else:
            self._date = pd.to_datetime(_date).date()

    def check1stock(self, Stock, reset_record=False):

        name, code, id_ = Stocks(Stock)

        if reset_record:
            reset_id_time(id_, self._date)

        dates = date_range(self._date, self.date_)
        for d_ in dates:
            run = PredictionCommon(Stock=name, months=self.months, monitor=False, check_date=d_)
            run.single_stock()

    def loop_by_date(self):
        list_day = date_range(self._date, self.date_)
        reset_record_time(list_day[0])

        for day_ in list_day:
            multiprocessing_count_pool(day_, check_model=False)
            PoolCount.count_trend()
            print(f'日期{day_}完成;')

    def loop_by_check_model(self):
        list_day = date_range(self._date, self.date_)
        reset_record_time(list_day[0])

        for day_ in list_day:
            multiprocessing_count_pool(day_, check_model=True)
            PoolCount.count_trend()
            print(f'日期{day_}完成;')


if __name__ == '__main__':
    start_ = '2022-10-26'
    # end_ = '2022-10-26'
    months = '2022-02'
    rm = RMHistoryCheck(_date=start_)
    rm.loop_by_date()
