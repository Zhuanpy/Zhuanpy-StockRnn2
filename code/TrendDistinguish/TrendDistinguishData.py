import pandas as pd
import numpy as np
from code.Normal import ResampleData
from code.MySql.sql_utils import Stocks
from code.Signals.StatisticsMacd import SignalMethod
from code.MySql.LoadMysql import StockData1m, StockPoolData
from Distinguish_utils import array_data
from root_ import file_root


pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)


class TrendDistinguishData:
    """
    判断趋势 数据处理
    """

    def __init__(self, stock):
        self.data_15m, self.data_1m = None, None
        self.stock_name, self.stock_code, self.stock_id = Stocks(stock)

    def load_1m(self, _date):
        """
        load 1m data
        """
        data = StockData1m.load_1m(self.stock_code, _date)
        data = data[data['date'] >= pd.to_datetime(_date)]
        return data

    def calculates(self, _date):
        self.data_1m = self.load_1m(_date)
        self.data_15m = ResampleData.resample_1m_data(data=self.data_1m, freq='15m')

        self.data_15m = SignalMethod.trend_3ema_MACDBoll(self.data_15m)
        self.data_15m = self.data_15m.dropna(subset=['SignalTimes'])

        return self.data_15m


class CountTrendData(TrendDistinguishData):

    """
    count trend data
    """

    def __init__(self, stock, _date='2019-01-01'):
        TrendDistinguishData.__init__(self, stock)
        self.data = self.calculates(_date)

    def save_data(self, data, file):
        data.shape = (1, 150, 200, 4)

        try:
            _data = np.load(file, allow_pickle=True)
            data = np.append(_data, data, axis=0)

        except FileNotFoundError:
            pass

        np.save(file, data)

    def count_trend(self):

        df = self.data[200:].dropna(subset=['SignalChoice'])

        df.loc[:, 'maxChange'] = (df['EndPrice'] - df['StartPrice']) / df['StartPrice']

        df1 = df[df['maxChange'] > 0.1]
        df2 = df[df['maxChange'] < -0.1]

        df12 = pd.concat([df1, df2]).sort_index()

        for y in df12.index:

            last2SignalTimes = list(df.loc[:y]['SignalTimes'].tail(3))

            if len(last2SignalTimes) <= 2:
                continue

            data_SignalTimes = self.data[self.data['SignalTimes'].isin(last2SignalTimes)]

            signal_times = df.loc[y, 'SignalTimes']
            signal_ = df.loc[y, 'Signal']

            start_time = df.loc[y, 'SignalStartTime']
            end_time = df.loc[y, 'EndPriceIndex']

            print(f'{self.stock_code}: {signal_times}')
            _index = self.data[self.data['date'] <= start_time].index[-1] + 5  # start index
            index_ = self.data[self.data['date'] <= end_time].index[-1]  # end index

            signal_data = self.data[self.data['SignalTimes'] == signal_times]
            shapes = signal_data.shape[0] // 6
            shapes = min(shapes, 5)  # one signal max 5 pictures.

            # plot_ = PlotTrend()
            for s in range(shapes):
                path_ = file_root()
                path_ = f'{path_}/data/output/MacdTrend/train/'

                if signal_ == 1:
                    _figName = f'{path_}_up/{self.stock_code}_{signal_times}.jpg'
                    figName_ = f'{path_}up_/{self.stock_code}_{signal_times}.jpg'

                    _file = f'{path_}_up/{self.stock_code}.npy'
                    file_ = f'{path_}up_/{self.stock_code}.npy'

                else:
                    _figName = f'{path_}_down/{self.stock_code}_{signal_times}.jpg'
                    figName_ = f'{path_}down_/{self.stock_code}_{signal_times}.jpg'

                    _file = f'{path_}_down/{self.stock_code}.npy'
                    file_ = f'{path_}down_/{self.stock_code}.npy'

                _num = _index + s
                _data = data_SignalTimes.loc[:_num].tail(100)  # _up = # _up : 上涨前期
                _array = array_data(data=_data, name_=_figName)

                num_ = index_ - s
                data_ = data_SignalTimes.loc[:num_].tail(100)
                array_ = array_data(data=data_, name_=figName_)

                self.save_data(_array, _file)
                self.save_data(array_, file_)


if __name__ == '__main__':

    pool = StockPoolData.load_StockPool()
    pool = pool.sort_values(by=['CycleAmplitude'])
    pool = pool.tail(40).head(20)
    print(pool['name'])
    # exit()
    for stock in pool['name']:
        count = CountTrendData(stock)
        count.count_trend()
        print(f'{stock} successfully;')
