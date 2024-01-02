# -*- coding: utf-8 -*-
import time
import pandas as pd
from downloads.DlStockData import RMDownloadData
from RnnModel.CheckModel import RMHistoryCheck
from RnnModel.MonitorModel import RMMonitor
from Evaluation.CountPool import PoolCount
from Evaluation.StockPool import ScoreStockPool


def monitor(months='2022-02'):

    timings = {'o1': '09:30',
               'o2': '13:00',
               'c1': '11:35',
               'c2': '15:00',
               'd1': '15:30'}

    open1 = pd.to_datetime(timings['o1'])  # 股市开盘时间
    close1 = pd.to_datetime(timings['c1'])  # 股市收盘时间

    open2 = pd.to_datetime(timings['o2'])  # 股市开盘时间
    close2 = pd.to_datetime(timings['c2'])  # 股市收盘时间

    review = pd.to_datetime(timings['d1'])  # 日常数据爬取，更新时间：

    count, dl_count, check_count, pl_count, bd_count = 0, 0, 0, 0, 0

    while count == 0:

        now = pd.Timestamp('today')

        if now < open1:
            st = open1 - now
            st = st.seconds
            print(f'等待开盘时间等待{st}秒；')
            time.sleep(st)

        if open1 < now <= close1 or open2 < now <= close2:

            run = RMMonitor(months)
            run.monitor_position_stock()

            print(f'监测暂停{15}分钟；')
            time.sleep(900)

        if close1 < now <= open2:
            num = (open2 - now).seconds + 10
            print(f'中午收盘，暂停{num}秒')
            time.sleep(num)

        if close2 < now <= review:
            num = (review - now).seconds + 10
            print(f'下午收盘，暂停{num}秒')
            time.sleep(num)

        if now >= review and dl_count == 0:
            dl = RMDownloadData()
            dl.daily_renew_data()
            dl_count += 1

        if now >= review and bd_count == 0:
            score = ScoreStockPool()
            score.analysis_Industry()
            bd_count += 1

        if now >= review and pl_count == 0:
            data = PoolCount.count_trend()  # 当天趋势统计
            pl_count += 1
            print(f'Trend count: {data}')

        if now >= review and check_count == 0:
            check = RMHistoryCheck()  # 查看当天触发买卖的股票；
            check.loop_by_date()

            check_count += 1
            count += 1


if __name__ == '__main__':
    monitor()
