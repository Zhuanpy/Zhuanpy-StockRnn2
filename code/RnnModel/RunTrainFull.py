# -*- coding: utf-8 -*-
from RnnCreationData import RMTrainingData
from RnnCreationModel import RMBuiltModel
from CheckModel import RMHistoryCheck


def full_running(month_, _month, _start):

    train = RMTrainingData(months=month_,  _month=_month, start_=_start)
    train.all_stock()

    model = RMBuiltModel(month_, _month)
    model.train_all()

    check = RMHistoryCheck(months=month_)
    check.loop_by_date()


if __name__ == '__main__':
    _months = '2022-01'
    months = '2022-02'
    start = '2018-01-01'
    full_running(months, _months, start)
