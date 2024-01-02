# -*- coding: utf-8 -*-
from code.parsers.BollingerParser import *


def Bollinger(data, ma_mid=20):
    data.loc[:, BollMid] = data['close'].rolling(ma_mid, min_periods=1).mean()
    data.loc[:, BollStd] = data['close'].rolling(ma_mid, min_periods=1).std()
    data.loc[:, BollUp] = data[BollMid] + 2 * data[BollStd]
    data.loc[:, BollDn] = data[BollMid] - 2 * data[BollStd]
    data.loc[:, StopLoss] = round(data[BollDn] - 2 * data[BollStd], 2)
    return data


# if __name__ == '__main__':
#     print(Bollinger)
