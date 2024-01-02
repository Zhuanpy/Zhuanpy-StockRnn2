import pandas as pd

from DB_MySql import execute_sql
from DB_MySql import MysqlAlchemy as msql
from DataBaseAction import load_tables
from code.Normal import ResampleData as rsp


def count_daily_data(tb: str):
    years = ['2021', '2022', '2023']

    data_day = pd.DataFrame()

    for y in years:
        db = f'data1m{y}'
        try:
            d = msql.pd_read(db, tb)

        except:
            d = pd.DataFrame()

        if not d.shape[0]:
            continue

        d = d.fillna(0)

        d[['open', 'close', 'high', 'low']] = d[['open', 'close', 'high', 'low']].astype('float')
        d[['volume', 'money']] = d[['volume', 'money']].astype('int')

        # 处理成 日 K 数据
        d = rsp.resample_1m_data(d, 'day')
        # 合并数据
        data_day = pd.concat([data_day, d])

    return data_day


# 整理数据库 daily data , 有些表格没有数据
def check_my_daily_data():
    error_list = []

    db = 'datadaily'

    daily = pd.read_csv('dailys.csv')

    pending_daily = daily[daily['count'].isnull()]

    print(pending_daily)
    # exit()

    for index in pending_daily.index:

        code_ = daily.loc[index, 'code']

        data = msql.pd_read(db, code_)

        if data.shape[0]:

            d_start = data.loc[0, 'date']
            d_end = data.iloc[-1]['date']

            daily.loc[index, 'start'] = d_start
            daily.loc[index, 'end'] = d_end

            if pd.Timestamp(d_end) < pd.Timestamp('2023-01-01'):

                try:
                    sql = f'DROP TABLE `{db}`.`{code_}`;'
                    execute_sql(database=db, sql=sql)

                    daily.loc[index, 'Delete'] = 'Y'
                    daily.loc[index, 'Ddone'] = 'Y'

                except:

                    daily.loc[index, 'Delete'] = 'F'
                    daily.loc[index, 'Ddone'] = 'F'

            daily.loc[index, 'count'] = 'Y'
            daily.to_csv('dailys.csv', header=True, index=False)

            continue

        # 失败的数据重新整理
        day_data = count_daily_data(code_)

        #  判断 day data 是否为空
        if not day_data.shape:
            error_list = error_list.append(code_)
            continue

        # 储存数据
        try:
            msql.pd_replace(data=day_data, database=db, table=code_)

            d_start = day_data.loc[0, 'date']
            d_end = day_data.iloc[-1]['date']

            daily.loc[index, 'start'] = d_start
            daily.loc[index, 'end'] = d_end
            daily.loc[index, 'count'] = 'Y'

            if pd.Timestamp(d_end) < pd.Timestamp('2023-01-01'):
                try:
                    sql = f'DROP TABLE `{db}`.`{code_}`;'
                    execute_sql(database=db, sql=sql)

                    daily.loc[index, 'Delete'] = 'Y'
                    daily.loc[index, 'Ddone'] = 'Y'

                except:

                    daily.loc[index, 'Delete'] = 'F'
                    daily.loc[index, 'Ddone'] = 'F'

            # save data
            daily.loc[index, 'count'] = 'Y'
            daily.to_csv('dailys.csv', header=True, index=False)

        except:
            continue

        print(f'Save success : {code_}')

    # save data
    # daily.to_excel('dailys.xlsx', header=True, index=False, mode='w')
    print(f'Save error list: {error_list}')


def table_to_csv():
    db = 'datadaily'
    tables = load_tables(db)
    df = pd.DataFrame(data=tables, columns=['code'])
    df.loc[:, ['start', 'end', 'settingId', 'addColumns', 'complete']] = None

    df.to_csv('dailys.csv', header=True, index=False)

    print(df)
    return df


def my_daily_data():
    db = 'datadaily'
    data = pd.read_csv('dailys.csv')

    print(data.head())
    for index in data.index:
        code = data.loc[index, 'code']
        complete = data.loc[index, 'complete']

        if complete == 'Y':
            continue

        #  统计开始日期 & 结束日期
        daily = msql.pd_read(database=db, table=code)

        # 清除表格重复数据
        daily = daily.drop_duplicates(subset=['date'])
        daily = daily.sort_values(by=['date']).reset_index(drop=True)
        msql.pd_replace(data=daily, database=db, table=code)

        print(daily.shape)
        print(daily.head())

        start = daily.iloc[0]['date']
        end = daily.iloc[-1]['date']

        print(start)
        print(end)

        ### 设置 ID
        try:
            sql = f'ALTER TABLE `{db}`.`{code}` CHANGE COLUMN `date` `date` DATE NOT NULL , ADD PRIMARY KEY (`date`);'
            execute_sql(database=db, sql=sql)
            settingid = 'Y'

        except:
            settingid = 'F'

        print(settingid)

        #  添加运行列
        exit()
    pass


if __name__ == '__main__':
    # table_to_csv()
    my_daily_data()
