import numpy as np
from keras.models import load_model
from keras import backend as k

from Distinguish_utils import array_data, calculate_distinguish_data
import imageio
from root_ import file_root


class TrendDistinguishModel:

    """
    趋势识别模型
    变量： 下跌前期：_down， 下跌后期: down_， 上涨前期:_up，上涨后期: up_；
    """

    def __init__(self):

        self.root = file_root()

        self.path_ = f'{self.root}/data/output/MacdTrend'

        self.values = {0: '_down', 1: 'down_', 2: '_up', 3: 'up_'}
        self.labels = {'_down': 0, 'down_': 1, '_up': 2, 'up_': 3}

    def predictive_value(self, code_):

        """
        模型预估，return value & label ;
        """

        img = imageio.imread(f'{self.path_}/predict/{code_}.jpg')
        img.shape = (1, img.shape[0], img.shape[1], img.shape[2])

        k.clear_session()
        path = f'{self.path_}/model.h5'
        model = load_model(path)

        value = model.predict(img)

        value_ = int(np.argmax(value[0]))
        label = self.values[value_]

        img = img.reshape(img.shape[1], img.shape[2], img.shape[3])

        imageio.imsave(f'{self.path_}/predict/trends/{label}_{code_}.jpg', img)

        result = (label, value_)

        return result

    def distinguish_1m(self, code_: str, freq: str, date_, returnFreq=False):

        """
        预估 1m 数据
        """

        fig_name = f'{self.path_}/predict/{code_}.jpg'
        # print(fig_name)
        data = calculate_distinguish_data(code_, freq, date_=date_)

        array_data(data=data, name_=fig_name)  # , showTicks=True)

        label_, value_ = self.predictive_value(code_)

        if returnFreq:
            result = data, (label_, value_)

        else:
            result = (label_, value_)

        return result

    def distinguish_freq(self, code_, data):
        data = data.tail(100).reset_index(drop=True)
        fig_name = f'{self.path_}/predict/{code_}.jpg'
        array_data(data=data, name_=fig_name)
        label_, value_ = self.predictive_value(code_)
        result = (label_, value_)
        return result


if __name__ == '__main__':

    stock = 'BK0465'

    for i in range(16):
        i = i+1
        date_ = f'2022-11-{i}'
        dis = TrendDistinguishModel()
        l, v = dis.distinguish_1m(code_=stock, returnFreq=False, freq='120m', date_=date_)

        print(date_)
        print(l, v)
