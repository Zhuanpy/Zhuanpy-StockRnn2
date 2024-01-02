from LoadMysql import LoadBasicInform, StockData1m, LoadFundsAwkward
import pandas as pd


def correction_date_1m_table():
    """
    1分钟数据，日期记录有误时，通过遍历每个表格，再次记录
    """

    # 读取1m数据
    # 获取最后一天的数据
    # 更新1m表格

    table1m = LoadBasicInform.load_minute()
    table1m['EndDate'] = pd.to_datetime(table1m['EndDate'])
    table1m = table1m[table1m['EndDate'] > pd.to_datetime('2022-01-01')]
    table1m = table1m[table1m['EndDate'] < pd.to_datetime('2050-01-01')]

    table1m = table1m.sort_values(by=['EndDate'])

    i = 0

    for index in table1m.index:
        id_ = table1m.loc[index, 'id']
        code = table1m.loc[index, 'code']
        name = table1m.loc[index, 'name']
        record_end = table1m.loc[index, 'EndDate'].date()

        if i % 10 == 0:
            print(i)
        # 读取1m表格

        data_1m = StockData1m.load_1m(code_=code, _year='2022')

        data_1m['date'] = pd.to_datetime(data_1m['date'])
        data_1m = data_1m.sort_values(by='date')
        _shape = data_1m.shape

        data_1m = data_1m.drop_duplicates(subset=['date'])

        shape_ = data_1m.shape
        end_date = data_1m.iloc[-1]['date'].date()

        if end_date != record_end:

            tb = 'record_stock_minute'
            sql1 = f'update {tb} set EndDate = {end_date} where id = {id_};'

            LoadBasicInform.basic_execute_sql(sql1)

            print(f'{name}, {code}: {sql1}')

        if _shape != shape_:

            # 替换表格
            StockData1m.replace_1m(code_=code, year_='2022', data=data_1m)
            print(f'{name}, {code}: 更新了数据')

        i = i + 1


def awkward_data():
    df = LoadFundsAwkward.load_awkwardNormalization()
    return df


if __name__ == "__main__":
    data = awkward_data()
    data = data.sort_values(by=['Date', 'count']).tail(50)
    print(data)
