from struct import unpack
import multiprocessing
from code.MySql.DB_MySql import MysqlAlchemy as alc
from code.MySql.DB_MySql import *
import pandas as pd
import pandas
import math
import pymysql
from code.Normal import StockCode
from code.Normal import ReadSaveFile

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)


def tb_txd_record():
    data = pd.read_excel('data/output/Tx_code.xls')

    data.loc[(data['TxMarket'] == 'sh') & (data['HsMarket'] == 'sz'),
             'Classification'] = '指数'

    data['code'] = data['code'].astype(str)

    for i in data.index:
        faker_code = '000000'
        code = data.loc[i, 'code']

        if len(code) < 6:
            data.loc[i, 'code'] = f'{faker_code[len(code):]}{code}'  # .format(, )

    data = data.rename(columns={'TxMarket': 'TxdMarket'})

    data.loc[:, ['name', 'Level1', 'Level2']] = None
    data.loc[:, ['StartDate', 'EndDate', 'RecordDate']] = pd.Timestamp().today().date()

    columns_list = ['name', 'code', 'TxdMarket', 'HsMarket', 'Classification',
                    'Level1', 'Level2', 'StartDate', 'EndDate', 'RecordDate']

    data = data[columns_list]

    str_list = ['name', 'code', 'TxdMarket', 'HsMarket', 'Classification', 'Level1', 'Level2']

    data[str_list] = data[str_list].astype(str)

    alc.pd_replace(data=data, database='stock_basic_information', table='record_stock_daily_data')


class StockDailyData:

    def __init__(self):
        self.file_list = ['E:/SOFT/Finace_software/vipdoc/sz/lday/',
                          'E:/SOFT/Finace_software/vipdoc/sh/lday/']

    def read_stock_file(self):
        code_list = []
        market_list = []
        hs_market_list = []
        hs_classification_list = []

        for path in self.file_list:
            stock_file = ReadSaveFile.find_all_file(path)

            for i in stock_file:
                stock_code = i[2:8]
                tx_market = i[:2]

                hs_market = StockCode.code2market(stock_code)
                hs_classification = StockCode.code2classification(stock_code)

                code_list.append(stock_code)
                market_list.append(tx_market)
                hs_market_list.append(hs_market)
                hs_classification_list.append(hs_classification)

        df = pd.DataFrame(data={'code': code_list, 'TxMarket': market_list, 'HsMarket': hs_market_list,
                                'Classification': hs_classification_list})

        df['code'] = df['code'].astype(str)

        df.to_excel('data/output/Tx_code.xls', sheet_name='Sheet1', index=False, header=True)

        print(df)


class TongxindaDailyData:

    def __init__(self):
        self.data = None

    # 解析日线数据
    def exact_data(self, FilePath):

        try:
            ofile = open(FilePath, 'rb')
            buf = ofile.read()
            ofile.close()
            num = len(buf)
            no = num / 32
            items = list()
            b = 0
            e = 32
            for i in range(int(no)):
                a = unpack('IIIIIfII', buf[b:e])
                dd = pd.to_datetime(str(a[0])).date()
                op = a[1] / 100.0
                high = a[2] / 100.0
                low = a[3] / 100.0
                close = a[4] / 100.0
                money = a[5]
                vol = int(a[6] / 100.0)
                item = [dd, op, close, high, low, vol, money]
                items.append(item)

                b = b + 32

                e = e + 32

            columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'money']

            df = pd.DataFrame(data=items, columns=columns)

        except FileNotFoundError:
            df = pd.DataFrame(data=None)

        return df

    def daily_data(self, num_start, num_end):

        for index in range(num_start, num_end):
            code = self.data.loc[index, 'code']
            TxdMarket = self.data.loc[index, 'TxdMarket']
            HsMarket = self.data.loc[index, 'HsMarket']
            MarketCode = self.data.loc[index, 'MarketCode']
            file_path = None

            if TxdMarket == 'sz':
                file_path = f'E:/SOFT/Finace_software/vipdoc/sz/lday/sz{code}.day'

            if TxdMarket == 'sh':
                file_path = f'E:/SOFT/Finace_software/vipdoc/sh/lday/sh{code}.day'

            if file_path:
                td_date = pd.Timestamp('today').date()
                table_name = f'{HsMarket}{code}'
                stock_data = self.exact_data(FilePath=file_path)

                if len(stock_data):
                    alc.pd_replace(data=stock_data, database='stock_daily_data', table=table_name)
                    # 更新表格记录信息
                    StartDate = stock_data.iloc[0]['date'].date()
                    EndDate = stock_data.iloc[-1]['date'].date()


                else:
                    print(f'{table_name}, 更新失败；')

    def multiple_process(self):
        self.data = alc.pd_read(database='stock_basic_information', table='record_stock_daily_data')
        self.data = self.data[self.data['RecordDate'] != pd.to_datetime('2021-12-14').date()].reset_index(drop=True)

        print(self.data)
        # exit()
        num_index = len(self.data)
        index1 = int(num_index * 0.2)
        index2 = int(num_index * 0.4)
        index3 = int(num_index * 0.6)
        index4 = int(num_index * 0.8)

        p1 = multiprocessing.Process(target=self.daily_data, args=(0, index1,))
        p2 = multiprocessing.Process(target=self.daily_data, args=(index1, index2,))
        p3 = multiprocessing.Process(target=self.daily_data, args=(index2, index3,))
        p4 = multiprocessing.Process(target=self.daily_data, args=(index3, index4,))
        p5 = multiprocessing.Process(target=self.daily_data, args=(index4, num_index,))

        p1.start()
        p2.start()
        p3.start()
        p4.start()
        p5.start()


class TongxindaMinuteData:

    def __init__(self):
        self.data = None

    def get_date_str(self, h1, h2) -> str:  # H1->0,1字节; H2->2,3字节;
        year = math.floor(h1 / 2048) + 2004  # 解析出年
        month = math.floor(h1 % 2048 / 100)  # 月
        day = h1 % 2048 % 100  # 日
        hour = math.floor(h2 / 60)  # 小时
        minute = h2 % 60  # 分钟

        if hour < 10:  # 如果小时小于两位, 补0
            hour = "0" + str(hour)

        if minute < 10:  # 如果分钟小于两位, 补0
            minute = "0" + str(minute)

        return str(year) + "-" + str(month) + "-" + str(day) + " " + str(hour) + ":" + str(minute)

    def exact_stock(self, FilePath):
        try:
            ofile = open(FilePath, 'rb')
            buf = ofile.read()
            ofile.close()
            num = len(buf)
            no = num / 32

            items = list()

            e = 32
            b = 0
            for i in range(int(no)):
                a = unpack('HHfffffif', buf[b:e])
                dd = self.get_date_str(a[0], a[1])
                dd = pd.to_datetime(dd)
                op = a[2]
                high = a[3]
                low = a[4]
                close = a[5]
                money = a[6]
                vol = a[7]
                item = [dd, op, close, high, low, vol, money]
                items.append(item)

                b = b + 32
                e = e + 32

            columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'money']
            data = pd.DataFrame(data=items, columns=columns)

        except FileNotFoundError:
            data = pd.DataFrame(data=None)

        return data

    def minute_data(self, num_start, num_end):

        td_date = pd.Timestamp('today').date()

        for index in range(num_start, num_end):
            code = self.data.loc[index, 'code']
            TxdMarket = self.data.loc[index, 'TxdMarket']
            MarketCode = self.data.loc[index, 'MarketCode']

            file_path = None
            if TxdMarket == 'sz':
                file_path = f'E:/SOFT/Finace_software/vipdoc/sz/minline/sz{code}.lc1'

            if TxdMarket == 'sh':
                file_path = f'E:/SOFT/Finace_software/vipdoc/sh/minline/sh{code}.lc1'

            if file_path:
                table_name = f'{MarketCode}'
                stock_data = self.exact_stock(FilePath=file_path)

                if len(stock_data):
                    alc.pd_replace(data=stock_data, database='stock_1m_data', table=table_name)

                else:
                    # 更新表格记录信息
                    failed_date = pd.to_datetime('2050-01-01').date()

    def multiple_process_minute_data(self):
        self.data = alc.pd_read(database='stock_basic_information', table='record_stock_minute_data')
        self.data = self.data[self.data['StartDate'].isnull()].reset_index(drop=True)
        print(self.data)
        # exit()
        num_index = len(self.data)
        index1 = int(num_index * 0.2)
        index2 = int(num_index * 0.4)
        index3 = int(num_index * 0.6)
        index4 = int(num_index * 0.8)

        p1 = multiprocessing.Process(target=self.minute_data, args=(0, index1,))
        p2 = multiprocessing.Process(target=self.minute_data, args=(index1, index2,))
        p3 = multiprocessing.Process(target=self.minute_data, args=(index2, index3,))
        p4 = multiprocessing.Process(target=self.minute_data, args=(index3, index4,))
        p5 = multiprocessing.Process(target=self.minute_data, args=(index4, num_index,))

        p1.start()
        p2.start()
        p3.start()
        p4.start()
        p5.start()


class CombineMinuteData:

    def __init__(self):
        self.db = 'stock_daily_data'
        self.table_list = None
        self.daily_record = None
        self.record_1m = None

    def combine_minute(self, stock_code, stock_name, MarketCode):

        # 读取 stock_data_1m;
        db_a = 'stock_data_1m'
        tb_a = f'1m_{stock_code}_{stock_name}'
        try:
            data_1ma = alc.pd_read(database=db_a, table=tb_a)
            data_1ma = data_1ma.rename(columns={'trade_date': 'date'})
            data_1ma = data_1ma[['date', 'open', 'close', 'high', 'low', 'volume', 'money']]

        except pandas.io.sql.DatabaseError:
            data_1ma = pd.DataFrame(data=None)

        # 读取 stock_1m_data;
        db_b = 'stock_1m_data'
        tb_b = f'{MarketCode}'
        try:
            data_1mb = alc.pd_read(database=db_b, table=tb_b)

        except pandas.io.sql.DatabaseError:
            data_1mb = pd.DataFrame(data=None)

        # data combine
        data_all = pd.concat([data_1ma, data_1mb]).drop_duplicates(subset=['date']).sort_values(
            by=['date']).reset_index(drop=True)

        st_time = data_all.iloc[0]['date'].date()
        ed_time = data_all.iloc[-1]['date'].date()

        alc.pd_replace(data=data_all, database=db_b, table=tb_b)

        print(f'{stock_name}, {MarketCode}, 数据合并成功；')

    def combine_minute_data(self, num_start=None, num_end=None):

        for index in range(num_start, num_end):
            stock_name = self.record_1m.loc[index, 'stock_name']
            stock_code = self.record_1m.loc[index, 'stock_code']
            MarketCode = self.record_1m.loc[index, 'MarketCode']

            # 记录表格
            try:
                self.combine_minute(stock_code=stock_code, stock_name=stock_name, MarketCode=MarketCode)

            except:
                # 修改表格记录
                print(f'{stock_name}, {MarketCode}, 数据合并失败；')

    def multiple_combine_minute(self):
        self.record_1m = alc.pd_read(database='stock_basic_information', table='stock_record_1m_data')
        print(self.record_1m)
        exit()
        num_index = len(self.record_1m)
        if num_index:
            index1 = int(num_index * 0.2)
            index2 = int(num_index * 0.4)
            index3 = int(num_index * 0.6)
            index4 = int(num_index * 0.8)

            p1 = multiprocessing.Process(target=self.combine_minute_data, args=(0, index1,))
            p2 = multiprocessing.Process(target=self.combine_minute_data, args=(index1, index2,))
            p3 = multiprocessing.Process(target=self.combine_minute_data, args=(index2, index3,))
            p4 = multiprocessing.Process(target=self.combine_minute_data, args=(index3, index4,))
            p5 = multiprocessing.Process(target=self.combine_minute_data, args=(index4, num_index,))

            p1.start()
            p2.start()
            p3.start()
            p4.start()
            p5.start()

    # sh603087 error
    def rename_daily_data_a(self, num_start=None, num_end=None):

        for i in range(num_start, num_end):
            MarketCode = self.table_list[i][0]
            sql = f'alter table {self.db}.{MarketCode} change amount money int;'

            try:
                execute_sql(database=self.db, sql=sql)
                print(f'success; {sql}')

            except pymysql.err.DataError:
                pass

    def multiple_process_rename_daily_a(self):

        self.table_list = 'return_all_table(database=self.db)'

        num_index = len(self.table_list)
        index1 = int(num_index * 0.2)
        index2 = int(num_index * 0.4)
        index3 = int(num_index * 0.6)
        index4 = int(num_index * 0.8)

        p1 = multiprocessing.Process(target=self.rename_daily_data_a, args=(0, index1,))
        p2 = multiprocessing.Process(target=self.rename_daily_data_a, args=(index1, index2,))
        p3 = multiprocessing.Process(target=self.rename_daily_data_a, args=(index2, index3,))
        p4 = multiprocessing.Process(target=self.rename_daily_data_a, args=(index3, index4,))
        p5 = multiprocessing.Process(target=self.rename_daily_data_a, args=(index4, num_index,))

        p1.start()
        p2.start()
        p3.start()
        p4.start()
        p5.start()

    def rename_daily_data_b(self, num_start=None, num_end=None):

        for index in range(num_start, num_end):
            MarketCode = self.daily_record.loc[index, 'MarketCode']
            print(MarketCode)
            tb = DbTongxindaData(primary_key=MarketCode)
            try:
                daily_data = alc.pd_read(database='stock_daily_data', table=MarketCode)
                daily_data = daily_data.rename(columns={'amount': 'money'})
                alc.pd_replace(data=daily_data, database='stock_daily_data', table=MarketCode)

                tb.renew_TbRecordStockDailyData(column='Transfer', new_data='success')

            except:
                tb.renew_TbRecordStockDailyData(column='Transfer', new_data='ffed')
                pass

    def multiple_process_rename_daily_b(self):
        self.daily_record = alc.pd_read(database='stock_basic_information', table='record_stock_daily_data')
        self.daily_record = self.daily_record[self.daily_record['Transfer'] == 'failed'].reset_index(drop=True)
        print(self.daily_record)
        # exit()
        num_index = len(self.daily_record)
        index1 = int(num_index * 0.2)
        index2 = int(num_index * 0.4)
        index3 = int(num_index * 0.6)
        index4 = int(num_index * 0.8)

        p1 = multiprocessing.Process(target=self.rename_daily_data_b, args=(0, index1,))
        p2 = multiprocessing.Process(target=self.rename_daily_data_b, args=(index1, index2,))
        p3 = multiprocessing.Process(target=self.rename_daily_data_b, args=(index2, index3,))
        p4 = multiprocessing.Process(target=self.rename_daily_data_b, args=(index3, index4,))
        p5 = multiprocessing.Process(target=self.rename_daily_data_b, args=(index4, num_index,))

        p1.start()
        p2.start()
        p3.start()
        p4.start()
        p5.start()


if __name__ == '__main__':
    dd = TongxindaMinuteData()
    dd.multiple_process_minute_data()

    # dd = TongxindaDailyData()
    # dd.multiple_process()

    # com = CombineMinuteData()
    # com.multiple_process_rename_daily_b()
