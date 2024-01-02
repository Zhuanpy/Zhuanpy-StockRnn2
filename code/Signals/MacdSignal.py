# -*- coding: utf-8 -*-
from code.parsers.MacdParser import *


def calculate_MACD(data, s=12, m=20, l=30, em=9):

    """
    MACD 计算 :
    1. 需要计算的几个数据  ema short: s, ema long: l, ema mid: m;
    2. Dif值: , DifSm值： ，   DifMl值： ，    Dea值: ,   macd值: ,
    """
    data.loc[:, EmaShort] = data['close'].rolling(s, min_periods=1).mean()
    data.loc[:, EmaMid] = data['close'].rolling(m, min_periods=1).mean()
    data.loc[:, EmaLong] = data['close'].rolling(l, min_periods=1).mean()

    data.loc[:, Dif] = data[EmaShort] - data[EmaLong]
    data.loc[:, DifSm] = data[EmaShort] - data[EmaMid]
    data.loc[:, DifMl] = data[EmaMid] - data[EmaLong]

    data.loc[:, Dea] = data[Dif].rolling(em, min_periods=1).mean()
    data.loc[:, macd_] = (data[Dif] - data[Dea]) * 2

    return data
