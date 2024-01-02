# -*- coding: utf-8 -*-
import pandas as pd
from MacdSignal import calculate_MACD

from BollingerSignal import Bollinger
from code.parsers.MacdParser import *


class CountMACD:

    @classmethod
    def remark_MACD(cls, data):
        # '下跌'
        con01 = (data[macd_] > 0) & (data[macd_].shift(-1) < 0)
        data.loc[con01, Signal] = downInt

        # '上涨'
        con02 = (data[macd_] < 0) & (data[macd_].shift(-1) > 0)
        data.loc[con02, Signal] = upInt

        return data

    # 找出 MACD 次数
    @classmethod
    def find_MACD_times(cls, data):
        con01 = data[Signal] == downInt  # 下跌
        con02 = data[Signal] == upInt  # 上涨
        data.loc[con01, SignalChoice] = down
        data.loc[con02, SignalChoice] = up

        con03 = ~data[Signal].isnull()
        data.loc[con03, SignalTimes] = data.loc[con03, 'date'].dt.strftime("%Y%m%d%H%M")
        data[SignalTimes] = data[SignalTimes].fillna(method='ffill')

        return data

    @classmethod
    def find_effect_MACD(cls, data):

        df = data.dropna(subset=[Signal])

        if df.shape[0]:

            for index in df.index:
                signal_times = data.loc[index, SignalTimes]
                signal = data.loc[index, Signal]
                condition = data[SignalTimes] == signal_times

                if signal == upInt:
                    diffs = data[condition & (data[DifMl] > 0) & (data[DifSm] > 0)].shape[0]

                    if diffs < 7:
                        data.loc[index, Signal] = None

                if signal == downInt:
                    diffs = data[condition & (data[DifMl] < 0) & (data[DifSm] < 0)].shape[0]

                    if diffs < 7:
                        data.loc[index, Signal] = None

            drop = data.dropna(subset=[Signal])

            if drop.shape[0]:

                for i, index in zip(range(drop.shape[0]), drop.index):

                    try:
                        if drop.iloc[i][Signal] == drop.iloc[i - 1][Signal]:
                            data.loc[index, Signal] = None

                    except ValueError:
                        pass

        data.loc[:, SignalTimes] = None  # 第2次统计出 涨跌次数
        data.loc[:, SignalChoice] = None  # 第2次统计出 涨跌次数

        return data

    @classmethod
    def count_MACD(cls, data):
        """
        统计出 macd 信号
        """

        data = calculate_MACD(data)
        data = cls.remark_MACD(data)  # 统计出 macd 信号
        data = cls.find_MACD_times(data)  # 统计出 涨跌次数 signal_times
        data = cls.find_effect_MACD(data)  # 删除无效振幅波段， 方法： by dif and bars 10 数

        data = cls.find_MACD_times(data)  # 第2次统计出 涨跌次数 signal_times

        con1 = data[SignalChoice].isin([up, down])
        data.loc[con1, SignalStartIndex] = data[con1]['date']

        fills = [Signal, SignalStartIndex]
        data[fills] = data[fills].fillna(method='ffill')
        return data


class StatisticsMACD:

    @classmethod
    def find_start_end_index(cls, data):
        data = data.set_index('date', drop=True)
        drops = data.dropna(subset=[SignalChoice])

        if drops.shape[0]:

            # find end price & end price id; 
            for i, index in zip(range(drops.shape[0]), drops.index):
                if i > 0:
                    choice_ = data.loc[index, SignalChoice]
                    times_ = data.loc[index, SignalTimes]  # signal_times

                    if choice_ == down:
                        end_price = data[data[SignalTimes] == times_]['low'].min()
                        end_price_id = data[data[SignalTimes] == times_]['low'].idxmin()

                    else:
                        end_price = data[data[SignalTimes] == times_]['high'].max()
                        end_price_id = data[data[SignalTimes] == times_]['high'].idxmax()

                    data.loc[index, EndPrice] = end_price  # '结束价'
                    data.loc[index, EndPriceIndex] = end_price_id  # '结束价_index'

            # find start price & start price id; 
            con1 = (~data[SignalChoice].isnull())
            data.loc[con1, StartPrice] = data[con1][EndPrice].shift(1)
            data.loc[con1, StartPriceIndex] = data[con1][EndPriceIndex].shift(1)

            # fill nan 
            fills = [EndPrice, EndPriceIndex, StartPrice, StartPriceIndex]
            data[fills] = data[fills].fillna(method='ffill')

            # reset 'date' to column 
            data = data.reset_index()

        return data

    @classmethod
    def s_StartEndIndex(cls, data):
        data = data.set_index('date', drop=True)
        drops = data.dropna(subset=[SignalChoice])
        # print(data)
        # exit()
        for i, index in zip(range(len(drops)), drops.index):

            if i > 0:
                signal_ = data.loc[index, SignalChoice]
                times_ = data.loc[index, SignalTimes]  # signal_times

                if signal_ == down:
                    end_price = data[data[SignalTimes] == times_]['low'].min()
                    end_price_id = data[data[SignalTimes] == times_]['low'].idxmin()

                else:
                    end_price = data[data[SignalTimes] == times_]['high'].max()
                    end_price_id = data[data[SignalTimes] == times_]['high'].idxmax()

                data.loc[index, EndPrice] = end_price  # '结束价'
                data.loc[index, EndPriceIndex] = end_price_id  # '结束价_index'
                data.loc[index, 'EndDaily1mVolMax5'] = data.loc[end_price_id, 'Daily1mVolMax5']

        condition = (~data[SignalChoice].isnull())
        data.loc[condition, StartPrice] = data[condition][EndPrice].shift(1)
        data.loc[condition, StartPriceIndex] = data[condition][EndPriceIndex].shift(1)

        fills = [EndPrice, EndPriceIndex, StartPrice, StartPriceIndex, 'EndDaily1mVolMax5']
        data[fills] = data[fills].fillna(method='ffill')
        data = data.reset_index()
        return data

        # 思路：下跌趋势中，找出信号区间，上涨区间的最大值，下跌区间最小值，用于算出振幅;
        # 此处和价格监视模块的思路不一样，价格监视模块，

    @classmethod
    def s_CycleAmplitude(cls, data):  # 找出最大的振幅
        con01 = data[Signal] == upInt
        con02 = data[Signal] == downInt

        data.loc[con01, CycleAmplitudePerBar] = round((data['high'] - data[StartPrice]) / data[StartPrice], 3)
        data.loc[con02, CycleAmplitudePerBar] = round((data['low'] - data[StartPrice]) / data[StartPrice], 3)
        data.loc[:, CycleAmplitudeMax] = round((data[EndPrice] - data[StartPrice]) / data[StartPrice], 3)

        return data

    @classmethod
    def s_Cycle1mVolumeMax(cls, data, data1m):
        conditions = (~data[SignalChoice].isnull())

        for index in data[conditions].index:
            st = data.loc[index, SignalTimes]

            ed_time = data.loc[index, EndPriceIndex]

            selects = data[(data[SignalTimes] == st) & (data['date'] < ed_time)]

            if len(selects) > 5:
                st_time = data[(data[SignalTimes] == st) &
                               (data['date'] < ed_time)].iloc[-5]['date']
            else:
                st_time = pd.to_datetime(ed_time.date())

            max1 = data1m[(data1m['date'] > st_time) &
                          (data1m['date'] <= ed_time)].sort_values(by=['volume']).tail(1)['volume'].mean()

            max5 = data1m[(data1m['date'] > st_time) &
                          (data1m['date'] <= ed_time)].sort_values(by=['volume']).tail(5)['volume'].mean()

            data.loc[(data[SignalTimes] == st), Cycle1mVolMax1] = max1
            data.loc[(data[SignalTimes] == st), Cycle1mVolMax5] = max5

        return data

    @classmethod
    def s_CycleLength(cls, data):

        for index in data[~data[SignalChoice].isnull()].index:
            st = data.loc[index, SignalTimes]
            conditions = data[SignalTimes] == st

            st_index = data[conditions].index[0]
            ed_index = data[conditions].index[-1]

            data.loc[conditions, CycleLengthMax] = ed_index - st_index
            data.loc[conditions, CycleLengthPerBar] = data[conditions].index - st_index

        return data

    @classmethod
    def find_Daily1mMax(cls, x, num, data1m):
        st_date = pd.to_datetime(x.date())
        ed_date = st_date + pd.Timedelta(days=1)

        select = data1m[(data1m['date'] > st_date) & (data1m['date'] < ed_date)]
        max_volume = select[(select['date'] > st_date) &
                            (select['date'] < ed_date)].sort_values(by=['volume'])['volume'].tail(num).mean()

        max_volume = int(max_volume)
        return max_volume

    @classmethod
    def s_Daily1mMax(cls, data, data1m):  # 找出每天最大的 1根，5根，15根 1分钟成交量
        fills = [Daily1mVolMax1, Daily1mVolMax5, Daily1mVolMax15]
        data.loc[:, 'minute_date'] = data['date'].dt.time
        con = data['minute_date'] == pd.to_datetime('09:45:00').time()
        data.loc[con, Daily1mVolMax1] = data.loc[con, 'date'].apply(cls.find_Daily1mMax, args=(1, data1m,))
        data.loc[con, Daily1mVolMax5] = data.loc[con, 'date'].apply(cls.find_Daily1mMax, args=(5, data1m,))
        data.loc[con, Daily1mVolMax15] = data.loc[con, 'date'].apply(cls.find_Daily1mMax, args=(15, data1m,))

        data[fills] = data[fills].fillna(method='ffill')

        return data

    @classmethod
    def find_Bar1mMax(cls, x, num, data1m):
        st = pd.to_datetime(x) + pd.Timedelta(minutes=-15)
        ed = pd.to_datetime(x)

        max_volume = data1m[(data1m['date'] > st) &
                            (data1m['date'] < ed)].sort_values(by=['volume'])['volume'].tail(num).mean()

        try:
            max_volume = int(max_volume)  # volume 1m is 'nan'

        except ValueError:
            max_volume = 0

        return max_volume

    @classmethod
    def s_BarMax1mVolume(cls, data, data1m):
        data.loc[:, Cycle1mVolMax1] = data['date'].apply(cls.find_Bar1mMax, args=(1, data1m,))
        data.loc[:, Cycle1mVolMax5] = data['date'].apply(cls.find_Bar1mMax, args=(5, data1m,))
        return data
        # 思路：找出每根bar中 最大的 1m volume 和前5的 1m volume


class SignalMethod:

    @classmethod
    def signal_by_MACD_3ema(cls, data, data1m):
        data = CountMACD.count_MACD(data)
        data = StatisticsMACD.s_Daily1mMax(data, data1m)

        data = StatisticsMACD.s_StartEndIndex(data)

        data = StatisticsMACD.s_CycleAmplitude(data)
        data = StatisticsMACD.s_Cycle1mVolumeMax(data, data1m)

        data = StatisticsMACD.s_CycleLength(data)

        data = data.drop(columns=['minute_date'])
        data = Bollinger(data=data)  # 找出boll通道价格
        return data

    @classmethod
    def trend_3ema_MACDBoll(cls, data):
        data = CountMACD.count_MACD(data)
        data = StatisticsMACD.find_start_end_index(data)
        data = Bollinger(data=data)  # 找出boll通道价格
        return data

    @classmethod
    def ema3_MACDBoll(cls, data):
        data = CountMACD.count_MACD(data)
        data = Bollinger(data=data)  # 找出boll通道价格
        return data

    @classmethod
    def trend_MACD(cls, data):
        data = CountMACD.count_MACD(data)
        data = data[[Signal, SignalChoice, SignalTimes, 'date']]
        return data


if __name__ == '__main__':
    pass
    # data1m = alc.pd_read(database='stock_1m_data', table='sz002475')
    # print(count)
