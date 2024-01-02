# -*- coding: utf-8 -*-
from code.MySql.LoadMysql import LoadRnnModel

from keras import Sequential
from keras.layers import Dense
from keras.layers import Flatten
from keras.layers import Conv2D
from keras.layers import AveragePooling2D

from keras.optimizers import adam_v2 # Adam
from code.Normal import ReadSaveFile as rf
from code.MySql.sql_utils import Stocks
import numpy as np
from keras import backend as k
import pandas as pd
from code.parsers.RnnParser import *

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 5000)


class BuiltModel:

    def __init__(self, stock: str, months: str, _month):

        self.name, self.code, self.stock_id = Stocks(stock)

        self.months = months
        self._month = _month

    def train_model(self, modelName: str, lr=0.01, num_train=30, num_test=10):
        k.clear_session()  # 清除缓存

        # 导入数据 train data , test data
        data_x = np.load(f'data/{self.months}/train_data/{modelName}_{self.code}_x.npy')
        data_y = np.load(f'data/{self.months}/train_data/{modelName}_{self.code}_y.npy')

        # 数据拆分
        len_data = int(data_y.shape[0] * 0.8)

        train_x = data_x[:len_data]
        train_y = data_y[:len_data]

        test_x = data_x[len_data:]
        test_y = data_y[len_data:]

        # 搭建模型
        model = Sequential()
        model.add(Conv2D(filters=6, kernel_size=(5, 5),
                         strides=(1, 1), input_shape=(30, 30, 1),
                         padding='valid', activation='relu'))
        model.add(AveragePooling2D(pool_size=(2, 2)))
        model.add(Conv2D(filters=16, kernel_size=(5, 5), strides=(1, 1),
                         padding='valid', activation='relu'))
        model.add(AveragePooling2D(pool_size=(2, 2)))
        model.add(Flatten())
        model.add(Dense(units=120, activation='relu'))
        model.add(Dense(units=84, activation='relu'))
        model.add(Dense(units=1))

        try:
            weight_path = f'data/{self._month}/weight/weight_{modelName}_{self.code}.h5'
            model.load_weights(filepath=weight_path)
            epochs = 100

        except OSError:
            epochs = 500

        model.compile(loss='mean_squared_error', optimizer=adam_v2.Adam(lr))  # 编译
        model.fit(train_x, train_y, epochs=epochs, batch_size=num_train)  # 训练
        loss = model.evaluate(test_x, test_y, batch_size=num_test)  # 评估
        print(loss)

        # 评估， 保存评估， 保存训练参数， 保存模型
        records = rf.read_json(self.months, self.code)
        records[modelName] = loss
        rf.save_json(records, self.months, self.code)

        # 保存参数
        model.save_weights(f'data/{self.months}/weight/weight_{modelName}_{self.code}.h5')

        # 保存模型
        model.save(f'data/{self.months}/model/{modelName}_{self.code}.h5')

    def model_one(self, modelname: str):
        self.train_model(modelname)

    def model_all(self):

        for name in ModelName:
            self.train_model(name)


class RMBuiltModel:

    def __init__(self, months: str, _month):
        self.months = months
        self._month = _month

    def train1(self, stock):
        train = BuiltModel(stock, self.months, _month=self._month)
        train.model_all()

    def train_all(self):

        data = LoadRnnModel.load_train_record()
        data = data[(data['ParserMonth'] == self.months) &
                    (data['ModelData'] == 'success') &
                    (data['ModelCreate'] != 'success')]

        shapes = data.shape[0]
        current = pd.Timestamp().today().date()
        print(data)
        for i, index in zip(range(shapes), data.index):

            Stock = data.loc[index, 'name']
            id_ = data.loc[index, 'id']
            print(f'当前股票：{Stock};\n训练进度：\n总股票数: {shapes}个; 剩余股票: {(shapes - i)}个;')

            try:
                train = BuiltModel(Stock, self.months, _month=self._month)
                train.model_all()

                sql = f'''update {LoadRnnModel.db_rnn}.{LoadRnnModel.tb_train_record} set 
                ModelCreate = 'success', 
                ModelCreateTiming = {current} where id = {id_};'''

            except Exception as ex:
                print(f'ModelCreate Error : {ex}')
                sql = f'''update {LoadRnnModel.db_rnn}.{LoadRnnModel.tb_train_record} set 
                ModelCreate = 'error', 
                ModelCreateTiming = '{current}' where id = '{id_}';'''

            LoadRnnModel.rnn_execute_sql(sql)


if __name__ == '__main__':
    month_ = '2022-02'
    _month = None

    run = RMBuiltModel(month_, _month)
    run.train_all()
