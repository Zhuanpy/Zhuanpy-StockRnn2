# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
from code.MySql.LoadMysql import StockData1m, StockData15m, LoadRnnModel
from code.MySql.sql_utils import Stocks
from code.parsers.RnnParser import *
from code.Normal import ReadSaveFile, ResampleData
from code.Signals.StatisticsMacd import SignalMethod
from root_ import file_root

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 5000)


def transfer_data():

    month_p = ['2021-11', '2021-12', '2022-01']
    stock_list = ['600309']

    for stock_code in stock_list:

        try:
            path1 = month_p[0]
            data1 = pd.read_csv(f'data/{path1}/15m/{stock_code}.csv')
            end1 = data1.iloc[-1]['date']

        except FileNotFoundError:
            data1 = pd.DataFrame()
            end1 = None

        try:
            path2 = month_p[1]
            data2 = pd.read_csv(f'data/{path2}/15m/{stock_code}.csv')
            end2 = data2.iloc[-1]['date']

        except FileNotFoundError:
            data2 = pd.DataFrame()
            end2 = None

        try:
            path3 = month_p[2]
            data3 = pd.read_csv(f'data/{path3}/15m/{stock_code}.csv')

        except FileNotFoundError:
            data3 = pd.DataFrame()

        if len(data1):
            data2 = data2[data2['date'] > end1]

        if len(data2):
            data3 = data3[data3['date'] > end2]

        data = pd.concat([data1, data2, data3], ignore_index=True)

        data = data.replace([np.inf, -np.inf], np.nan)

        StockData15m.replace_15m(stock_code, data)

        return data


class ModelData:

    """
    1. 处理模型数据
    2. 模型数据准备
    """

    def __init__(self):

        self.root = file_root()
        self.months = None
        self._month = None
        self.stock_code = None
        self.data_15m = None

        self.X = XColumn()
        self.Y = YColumn()
        self.model_name = ModelName

    def data_common(self, model_name: str, con_x, con_y, height=30, width=30):  # width=w2, height=h1
        file_ = f'{model_name}_{self.stock_code}'
        data_x = np.zeros([0])
        data_y = np.empty([0])

        # 前数据读取
        if self._month:
            data_x = np.load(f'{self.root}/data/{self._month}/train_data/{file_}_x.npy', allow_pickle=True)
            data_y = np.load(f'{self.root}/data/{self._month}/train_data/{file_}_y.npy', allow_pickle=True)

        # 整理数据
        data_ = self.data_15m.dropna(subset=[SignalChoice])

        for st in data_[SignalTimes]:

            x = self.data_15m[self.data_15m[SignalTimes] == st][con_x].dropna(how='any').tail(height)
            y = self.data_15m[self.data_15m[SignalTimes] == st][con_y].dropna(how='any').tail(1)

            if not x.shape[0] or not y.shape[0]:
                continue

            x = pd.concat([x[[Signal]], x], axis=1)
            x = x.to_numpy()

            # 计算填充数据  30*30 矩阵， 数据不足0补充
            h = height - x.shape[0]
            w = width - x.shape[1]

            ht = h // 2  # height top
            hl = h - ht  # height bottom

            wl = w // 2  # width left
            wr = w - wl  # width right

            x = np.pad(x, ((ht, hl), (wr, wl)), 'constant', constant_values=(0, 0))
            x.shape = (1, height, width, 1)
            y = y.to_numpy()

            # 判断储存 x y data
            if data_x.shape[0]:
                data_x = np.append(data_x, x, axis=0)
                data_y = np.append(data_y, y, axis=0)

            else:
                data_x = x
                data_y = y

        # 新数据储存
        np.save(f'{self.root}/data/{self.months}/train_data/{file_}_x.npy', data_x)
        np.save(f'{self.root}/data/{self.months}/train_data/{file_}_y.npy', data_y)

        print(f'{model_name}, shape: {data_x.shape};')

    def data_cycle_length(self):
        x = self.X[0]
        y = self.Y[0]
        self.data_common(model_name=self.model_name[0], con_x=x, con_y=y)

    def data_cycle_change(self):
        x = self.X[1]
        y = self.Y[1]
        self.data_common(model_name=self.model_name[1], con_x=x, con_y=y)

    def data_bar_change(self):
        x = self.X[2]
        y = self.Y[2]
        self.data_common(model_name=self.model_name[2], con_x=x, con_y=y)

    def data_bar_volume(self):
        x = self.X[3]
        y = self.Y[3]
        self.data_common(model_name=self.model_name[3], con_x=x, con_y=y)


class TrainingDataCalculate(ModelData):

    def __init__(self, stock: str, months: str, start_date: str, _month):

        ModelData.__init__(self)
        self.stock_name, self.stock_code, self.stock_id = Stocks(stock)

        self.months = months  # '2021SEP'
        self._month = _month

        self.data_1m = None
        self.data_15m = None
        self.times_data = None

        self.RecordStartDate = None
        self.RecordEndDate = None

        self.freq = '15m'
        self.start_date = start_date
        self.start_date_1m = None

        self.daily_volume_max = None

    def rnn_parser_data(self):
        data = ReadSaveFile.read_json(self.months, self.stock_code)

        if self.stock_code not in data:
            data[self.stock_code] = {}
            ReadSaveFile.save_json(data, self.months, self.stock_code)

    def stand_save_parser(self, data, column, drop_duplicates, drop_column):

        if drop_duplicates:
            if drop_column == SignalChoice:
                df = data.dropna(subset=[SignalChoice])

            else:
                df = data.drop_duplicates(subset=[column])

            med = df[column].median()
            mad = abs(df[column] - med).median()

        else:
            med = data[column].median()
            mad = abs(data[column] - med).median()

        high = round(med + (3 * 1.4826 * mad), 2)
        low = round(med - (3 * 1.4826 * mad), 2)

        # 查看参数
        if self._month:
            parser_data = ReadSaveFile.read_json(self._month, self.stock_code)
            pre_high = parser_data[self.stock_code][column]['num_max']
            pre_low = parser_data[self.stock_code][column]['num_min']

        else:
            pre_high = high
            pre_low = low

        high = max([high, pre_high])
        low = min([low, pre_low])

        # 去极值
        data.loc[(data[column] > high), column] = high
        data.loc[(data[column] < low), column] = low

        high = data[column].max()
        low = data[column].min()

        # 数据归一化
        data.loc[:, column] = (data[column] - low) / (high - low)

        parser_data = ReadSaveFile.read_json(self.months, self.stock_code)
        parser_data[column] = {'num_max': high, 'num_min': low}
        ReadSaveFile.save_json(parser_data, self.months, self.stock_code)  # 更新参数

        return data

    def stand_read_parser(self, data, column, match):
        parser_data = ReadSaveFile.read_json(self.months, self.stock_code)
        num_max = parser_data[self.stock_code][match]['num_max']
        num_min = parser_data[self.stock_code][match]['num_min']

        data.loc[data[column] > num_max, column] = num_max
        data.loc[data[column] < num_min, column] = num_min
        data.loc[:, column] = (data[column] - num_min) / (num_max - num_min)
        return data

    def column_stand(self):
        # 保存 daily_volume_max
        if not self.daily_volume_max:
            _date = '2018-01-01'
            self.data_1m = StockData1m.load_1m(self.stock_code, _date)
            self.data_1m = self.data_1m[self.data_1m['date'] > pd.to_datetime(_date)]

            data_daily = ResampleData.resample_1m_data(data=self.data_1m, freq='daily')
            data_daily.loc[:, 'date'] = pd.to_datetime(data_daily['date']) + pd.Timedelta(minutes=585)
            data_daily.loc[:, DailyVolEma] = data_daily['volume'].rolling(90, min_periods=1).mean()

            self.daily_volume_max = round(data_daily[DailyVolEma].max(), 2)

        parser_data = ReadSaveFile.read_json(self.months, self.stock_code)
        parser_data[DailyVolEma] = self.daily_volume_max
        ReadSaveFile.save_json(parser_data, self.months, self.stock_code)

        save_list = [('volume', False, None),
                     (Daily1mVolMax1, True, Daily1mVolMax1),
                     (Daily1mVolMax5, True, Daily1mVolMax5),
                     (Daily1mVolMax15, True, Daily1mVolMax15),
                     (Bar1mVolMax1, False, None),
                     (Cycle1mVolMax1, True, SignalChoice),
                     (Cycle1mVolMax5, True, SignalChoice),
                     (Bar1mVolMax5, False, None),
                     (CycleLengthMax, True, SignalChoice),
                     (nextCycleLengthMax, True, SignalChoice),
                     (CycleLengthPerBar, False, None),
                     (CycleAmplitudeMax, True, SignalChoice),
                     (nextCycleAmplitudeMax, True, SignalChoice),
                     (CycleAmplitudePerBar, False, None),
                     ('EndDaily1mVolMax5', True, SignalChoice)]

        for i in save_list:
            column = i[0]
            drop_duplicates = i[1]
            drop_column = i[2]
            self.data_15m = self.stand_save_parser(self.data_15m, column, drop_duplicates, drop_column)

        read_dict = {preCycle1mVolMax1: Cycle1mVolMax1,
                     preCycle1mVolMax5: Cycle1mVolMax5,
                     preCycleLengthMax: CycleLengthMax,
                     preCycleAmplitudeMax: CycleAmplitudeMax}

        for key, value in read_dict.items():
            self.data_15m = self.stand_read_parser(self.data_15m, key, value)

        self.data_15m = self.data_15m.dropna(subset=[Signal])

        last_signal_times = self.data_15m.iloc[-1][SignalTimes]

        self.data_15m = self.data_15m[self.data_15m[SignalTimes] != last_signal_times]

        # 选择模型数据
        all_columns = ['date', Signal, SignalTimes, SignalChoice,
                       StartPriceIndex, EndPriceIndex, CycleAmplitudePerBar, CycleAmplitudeMax,
                       Cycle1mVolMax1, Cycle1mVolMax5,
                       CycleLengthMax, CycleLengthPerBar,
                       Daily1mVolMax1, Daily1mVolMax5, Daily1mVolMax15,
                       preCycle1mVolMax1, preCycleLengthMax, Bar1mVolMax1,
                       nextCycleLengthMax, preCycle1mVolMax5,
                       'volume', Bar1mVolMax5, preCycleAmplitudeMax,
                       'EndDaily1mVolMax5', nextCycleAmplitudeMax]

        self.data_15m = self.data_15m[all_columns]

        return self.data_15m

    def first_calculate(self):
        self.data_15m = ResampleData.resample_1m_data(data=self.data_1m, freq=self.freq)
        self.data_15m = SignalMethod.signal_by_MACD_3ema(self.data_15m, self.data_1m).set_index('date', drop=True)

        data_daily = ResampleData.resample_1m_data(data=self.data_1m, freq='daily')
        data_daily.loc[:, 'date'] = pd.to_datetime(data_daily['date']) + pd.Timedelta(minutes=585)
        data_daily.loc[:, DailyVolEma] = data_daily['volume'].rolling(90, min_periods=1).mean()

        daily_volume_max = round(data_daily[DailyVolEma].max(), 2)

        # 读取旧参数
        if self._month:
            parser_data = ReadSaveFile.read_json(self._month, self.stock_code)
            pre_daily_volume_max = parser_data[self.stock_code][DailyVolEma]

        else:
            pre_daily_volume_max = daily_volume_max

        self.daily_volume_max = max(daily_volume_max, pre_daily_volume_max)

        data_daily.loc[:, DailyVolEmaParser] = self.daily_volume_max / data_daily[DailyVolEma]
        data_daily = data_daily[['date', DailyVolEmaParser]].set_index('date', drop=True)

        self.data_15m = self.data_15m.join([data_daily]).reset_index()

        self.data_15m[DailyVolEmaParser] = self.data_15m[DailyVolEmaParser].fillna(method='ffill')

        # 排除最后 signalTimes , 可能周期并未走完整
        last_signal_times = self.data_15m.iloc[-1][SignalTimes]
        self.data_15m = self.data_15m[self.data_15m[SignalTimes] != last_signal_times]
        return self.data_15m

    def find_bar_max_1m(self, x, num):
        st = pd.to_datetime(x) + pd.Timedelta(minutes=-15)
        ed = pd.to_datetime(x)

        max_vol = self.data_1m[(self.data_1m['date'] > st) & (self.data_1m['date'] < ed)]
        max_vol = max_vol.sort_values(by=['volume'])['volume'].tail(num).mean()

        try:
            max_vol = int(max_vol)  # volume 1m is 'nan'

        except ValueError:
            max_vol = None

        except Exception as ex:
            max_vol = None
            print(f'{self.stock_name} 函数： find_bar_max_1m 错误;\n{ex}')

        return max_vol

    def second_calculate(self):

        for index in self.data_15m.dropna(subset=[SignalChoice, EndPriceIndex]).index:
            signal_times = self.data_15m.loc[index, SignalTimes]
            end_price_time = self.data_15m.loc[index, EndPriceIndex]
            selects = self.data_15m[(self.data_15m[SignalTimes] == signal_times) &
                                    (self.data_15m[EndPriceIndex] <= end_price_time)].tail(35)

            st_index = selects.index[0]
            ed_index = selects.index[-1]

            self.data_15m.loc[st_index:ed_index, Bar1mVolMax1] = \
                self.data_15m.loc[st_index:ed_index]['date'].apply(self.find_bar_max_1m, args=(1,))

            self.data_15m.loc[st_index:ed_index, Bar1mVolMax5] = \
                self.data_15m.loc[st_index:ed_index]['date'].apply(self.find_bar_max_1m, args=(5,))

        # # 保存计算数据
        self.data_15m = self.data_15m.replace([np.inf, -np.inf], np.nan)

        # 计算数据保存
        self.save_15m_data()

        return self.data_15m

    def third_calculate(self):

        self.data_15m[Signal] = self.data_15m[Signal].astype(float)

        vol_parser = ['volume', Cycle1mVolMax1, Cycle1mVolMax5,
                      Daily1mVolMax1, Daily1mVolMax5, Daily1mVolMax15,
                      Bar1mVolMax1, Bar1mVolMax5, 'EndDaily1mVolMax5']  # 成交量乘以参数，相似化

        for i in vol_parser:
            self.data_15m.loc[:, i] = round(self.data_15m[i] * self.data_15m[DailyVolEmaParser])

        next_dic = {nextCycleAmplitudeMax: CycleAmplitudeMax,
                    nextCycleLengthMax: CycleLengthMax}
        condition = (~self.data_15m[SignalChoice].isnull())
        for keys, values in next_dic.items():
            self.data_15m.loc[condition, keys] = self.data_15m.loc[condition, values].shift(-1)

        # 提取前周期相关数据：
        condition = (~self.data_15m[SignalChoice].isnull())

        pre_dic = {preCycle1mVolMax1: Cycle1mVolMax1,
                   preCycle1mVolMax5: Cycle1mVolMax5,
                   preCycleAmplitudeMax: CycleAmplitudeMax,
                   preCycleLengthMax: CycleLengthMax}

        for key, values in pre_dic.items():
            self.data_15m.loc[condition, key] = self.data_15m.loc[condition, values].shift(1)

        fills = list(pre_dic.keys()) + list(next_dic.keys())

        self.data_15m[fills] = self.data_15m[fills].fillna(method='ffill')

        return self.data_15m

    def data_1m_calculate(self):

        self.rnn_parser_data()

        if self.RecordStartDate:
            self.data_1m = StockData1m.load_1m(self.stock_code, self.RecordStartDate)
            self.data_1m = self.data_1m.sort_values(by=['date'])
            self.start_date_1m = self.data_1m.iloc[0]['date']

            self.data_1m = self.data_1m[
                (self.data_1m['date'] > (pd.to_datetime(self.RecordStartDate) + pd.Timedelta(days=-30))) &
                (self.data_1m['date'] < (pd.to_datetime(self.months) + pd.Timedelta(days=-30)))]

        else:
            self.data_1m = StockData1m.load_1m(self.stock_code, self.start_date)
            self.data_1m = self.data_1m.sort_values(by=['date'])
            self.start_date_1m = self.data_1m.iloc[0]['date']

            self.data_1m = self.data_1m[(self.data_1m['date'] > pd.to_datetime(self.start_date)) &
                                        (self.data_1m['date'] < (
                                                pd.to_datetime(self.months) + pd.Timedelta(days=-30)))]

        self.data_1m = self.data_1m.dropna(subset=['date']).drop_duplicates(subset=['date']).reset_index(drop=True)

        return self.data_1m

    def save_15m_data(self):

        if self.RecordStartDate:
            self.data_15m = self.data_15m[self.data_15m['date'] > self.RecordEndDate]
            import sqlalchemy

            try:
                StockData15m.append_15m(data=self.data_15m, code_=self.stock_code)

            except sqlalchemy.exc.IntegrityError:
                old = StockData15m.load_15m(self.stock_code)
                last_date = old.iloc[-1]['date']
                new = self.data_15m[self.data_15m['date'] > last_date]
                old = pd.concat([old, new], ignore_index=True)
                StockData15m.replace_15m(data=old, code_=self.stock_code)

        else:
            StockData15m.replace_15m(data=self.data_15m, code_=self.stock_code)

        """     
        保存15m 数据截止日期
        
        record end date : red
        record end signal: res
        record end Signal Times : rest 
        record end Signal Start Time : resst
        record end next start : rens
        """

        red = self.data_15m.iloc[-1]['date'].strftime('%Y-%m-%d %H:%M:%S')
        res = self.data_15m.iloc[-1]['Signal']
        rest = self.data_15m.iloc[-1]['SignalTimes']
        resst = self.data_15m.iloc[-1]['SignalStartTime'].strftime('%Y-%m-%d %H:%M:%S')
        rens = self.data_15m.drop_duplicates(subset=[SignalTimes]).tail(6).iloc[0]['date'].strftime('%Y-%m-%d %H:%M:%S')

        records = ReadSaveFile.read_json(self.months, self.stock_code)
        records['RecordEndDate'] = red
        records['RecordEndSignal'] = res
        records['RecordEndSignalTimes'] = rest
        records['RecordEndSignalStartTime'] = resst
        records['RecordNextStartDate'] = rens
        ReadSaveFile.save_json(records, self.months, self.stock_code)  # 更新参数

    def data_15m_calculate(self):
        # # 1m 数据选择
        self.data_1m_calculate()

        self.data_15m = self.first_calculate()

        self.data_15m = self.second_calculate()

        self.data_15m = self.third_calculate()

        self.data_15m = self.column_stand()  # 标准化数据

        return self.data_15m

    def calculation_single(self):

        if self._month:
            try:
                record = ReadSaveFile.read_json(self._month, self.stock_code)
                self.RecordEndDate = record[self.stock_code]['RecordEndDate']
                self.RecordStartDate = record[self.stock_code]['NextStartDate']

            except ValueError:
                pass

        self.data_15m = self.data_15m_calculate()

        for i in range(4):
            x = self.X[i]
            y = self.Y[i]
            model_name = self.model_name[i]
            self.data_common(model_name=model_name, con_x=x, con_y=y)

    def calculation_read_from_sql(self):

        self.data_15m = StockData15m.load_15m(self.stock_code)

        self.data_15m = self.third_calculate()
        self.data_15m = self.column_stand()  # 标准化数据

        for i in range(4):
            x = self.X[i]
            y = self.Y[i]
            model_name = self.model_name[i]
            self.data_common(model_name=model_name, con_x=x, con_y=y)


class RMTrainingData:

    def __init__(self, months: str, start_: str, _month):
        self.months = months
        self.start_date = start_
        self._month = _month

    def single_stock(self, Stock):
        calculation = TrainingDataCalculate(Stock, self.months, self.start_date, self._month)
        calculation.calculation_single()

    def all_stock(self):
        records = LoadRnnModel.load_train_record()
        records = records[records['ParserMonth'] == self.months]

        if not records.shape[0]:
            records['ParserMonth'] = self.months
            records['ModelData'] = 'pending'
            ids = tuple(records.id)

            sql = f'''update {LoadRnnModel.db_rnn}.{LoadRnnModel.tb_train_record} 
            set ParserMonth = '{self.months}', 
            ModelData ='pending' where id in {ids};'''

            LoadRnnModel.rnn_execute_sql(sql)

        records = records[~records['ModelData'].isin(['success'])].reset_index(drop=True)
        print(records)
        shapes = records.shape[0]
        current = pd.Timestamp().now().date()

        if shapes:
            for i, index in zip(range(shapes), records.index):
                Stock = records.loc[index, 'name']
                id_ = records.loc[index, 'id']
                print(f'\n计算进度：\n剩余股票: {(shapes - i)} 个; 总股票数: {shapes}个;\n当前股票：{Stock};')

                try:
                    run = TrainingDataCalculate(Stock, self.months, self.start_date, self._month)
                    run.calculation_read_from_sql()

                    sql = f'''update {LoadRnnModel.db_rnn}.{LoadRnnModel.tb_train_record} set 
                    ModelData = 'success',
                    ModelDataTiming = '{current}' where id = '{id_}'; '''

                    LoadRnnModel.rnn_execute_sql(sql)

                except Exception as ex:
                    print(f'Model Data Create Error: {ex}')

                    sql = f'''update {LoadRnnModel.db_rnn}.{LoadRnnModel.tb_train_record} set ModelData = 'error', 
                    ModelDataTiming = NULL where id = {id_}; '''

                    LoadRnnModel.rnn_execute_sql(sql)

        else:
            print('Training Data create success;')


if __name__ == '__main__':
    month_ = '2022-02'
    _month = None
    start_date = '2018-01-01'

    running = RMTrainingData(month_, start_date, _month)
    running.all_stock()
