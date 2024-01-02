import pymysql
from root_ import file_root
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 5000)


def sql_password():
    path_ = file_root()
    path_ = f'{path_}/pp/sql.txt'
    f = open(path_, 'r')
    w = f.read()
    return w


def sql_cursor(database: str):
    w = sql_password()
    cur = pymysql.connect(host='localhost', user='root', password=w,
                          database=database, charset='utf8', autocommit=True)
    cursor = cur.cursor()
    return cursor


def execute_sql(database: str, sql: str):
    cursor = sql_cursor(database)
    cursor.execute(sql)
    d = cursor.fetchall()
    cursor.close()
    return d


def sql_data(database: str, sql: str):
    cursor = sql_cursor(database)
    cursor.execute(sql)
    d = cursor.fetchall()
    cursor.close()
    return d


def my_conn(database: str):
    w = sql_password()
    conn = f'mysql+pymysql://root:{w}@localhost:3306/{database}?charset=utf8'
    return conn


def create_session(database: str):

    conn = my_conn(database)
    engine = create_engine(conn)

    DbSession = sessionmaker(bind=engine)

    session = DbSession()

    return session


class MysqlAlchemy:

    @classmethod
    def pd_read(cls, database: str, table: str):
        conn = my_conn(database)
        sql = f'SELECT * FROM {database}.{table};'
        d = pd.read_sql(sql=sql, con=conn)  # 读取SQL数据库中数据;
        return d

    @classmethod
    def pd_append(cls, data, database: str, table: str):
        conn = my_conn(database)
        data.to_sql(table, con=conn, if_exists='append', index=False, chunksize=None, dtype=None)

    @classmethod
    def pd_replace(cls, data, database: str, table: str):
        conn = my_conn(database)
        data.to_sql(table, con=conn, if_exists='replace', index=False, chunksize=None, dtype=None)


if __name__ == '__main__':
    # MysqlAlchemy .;
    path = sql_password()
    print(path)
    alc = MysqlAlchemy()

    data = alc.pd_read(database='stock_basic_information', table='record_stock_minute')
    print(data)
