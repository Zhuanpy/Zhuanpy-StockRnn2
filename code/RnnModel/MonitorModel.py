# -*- coding: utf-8 -*-
import pandas as pd
from code.MySql.LoadMysql import StockPoolData
from RnnRunModel import PredictionCommon
import multiprocessing
from code.Normal import Useful


class RMMonitor:

    def __init__(self, months='2022-02'):
        self.lines, self.line = Useful.dashed_line(50)
        self.pool_data = None
        self.months = months
        self.db, self.tb = 'StockPool', 'StockPool'

    def show_results(self):
        pool = StockPoolData.load_StockPool()
        positions = pool[pool['Position'] == 1]
        reTrend = pool[(pool['RecordDate'] == pd.Timestamp.now().date()) &
                       (pool['Trends'] == -1) &
                       pool['ReTrend'] == 1]

        trading = pool[(pool['RecordDate'] == pd.Timestamp.now().date()) &
                       (pool['Trends'] == -1) &
                       pool['RnnModel'] < -5.5]

        results = pd.concat([positions, reTrend, trading], ignore_index=True)
        print(results)

    def monitor_buy_stock(self, start_, end_):
        i = 0

        for index in range(start_, end_):
            Stock = self.pool_data.loc[index, 'code']
            print(f'{self.lines}\n回测进度：\n总股票数:{end_ - start_}个;'
                  f'剩余股票: {(end_ - start_ - i)}个;\n当前股票：{Stock};\n')

            try:
                run = PredictionCommon(Stock=Stock, months=self.months, monitor=True, check_date=None)
                run.single_stock()

            except Exception as ex:
                print(f'{Stock}: {ex}；')
                # pass

            i += 1

    def monitor_multiple_process(self):

        self.pool_data = StockPoolData.load_StockPool()

        print(self.pool_data)

        shapes = self.pool_data.shape[0]

        if shapes > 4:

            index1 = int(shapes * 0.25)
            index2 = int(shapes * 0.5)
            index3 = int(shapes * 0.75)

            p1 = multiprocessing.Process(target=self.monitor_buy_stock, args=(0, index1,))
            p2 = multiprocessing.Process(target=self.monitor_buy_stock, args=(index1, index2,))
            p3 = multiprocessing.Process(target=self.monitor_buy_stock, args=(index2, index3,))
            p4 = multiprocessing.Process(target=self.monitor_buy_stock, args=(index3, shapes,))

            p1.start()
            p2.start()
            p3.start()
            p4.start()

            p1.join()
            p2.join()
            p3.join()
            p4.join()

        else:
            p1 = multiprocessing.Process(target=self.monitor_buy_stock, args=(0, shapes,))
            p1.start()
            p1.join()

    def monitor_position_stock(self):

        pool = StockPoolData.load_StockPool()
        data = pool[pool['Position'] == 1]

        for index in data.index:

            Stock = data.loc[index, 'code']
            stop_loss = data.loc[index, 'StopLoss']

            run = PredictionCommon(Stock=Stock, months=self.months, monitor=True,
                                   check_date=None, stopLoss=stop_loss, position=1)
            run.single_stock()

        self.show_results()


if __name__ == '__main__':
    rm = RMMonitor()
    rm.monitor_multiple_process()
