from code.MySql.LoadMysql import StockPoolData
from code.Evaluation.EvaluateBoard import multiprocessing_count_board
from code.Evaluation.CountPool import count_board_by_date
# 1

# 哪些日期需要统计？ ：  stockpool.board表格中板块数据统计不对的日期
count_data = StockPoolData.load_poolCount()
count_data = count_data.tail(2)
# exit()
for date_ in count_data.date:

    date_ = date_.strftime('%Y-%m-%d')

    # 执行了趋势统计
    multiprocessing_count_board(date_)

    # 更新统计表格表格
    count_board_by_date(date_)

    print(count_data)


# count :  stock pool.board
# renew data·
