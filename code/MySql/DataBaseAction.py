from DB_MySql import sql_data
from DB_MySql import execute_sql
import multiprocessing


def load_tables(db: str, upper=True):

    sql = f'''SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{db}'; '''
    li = sql_data(database=db, sql=sql)
    li = list(li)
    li = [i[0].upper() for i in li]

    if not upper:
        li = [i[0] for i in li]

    return li


class DropDatabase:

    """
    useful sql lunguage
    """

    @classmethod
    def drop_tabel(cls, tb: str):
        db = 'stock_1m_data'
        sql = f'''DROP TABLE {db}.{tb}; '''
        execute_sql(db, sql)

    @classmethod
    def drop_tabel_loop(cls, _i: int, i_: int, tabel: list):
        count = 0

        for i in range(_i, i_):  # in tabel
            t = tabel[i]
            print(f'剩余股票：{i_ - _i - count};\nDROP {t} success;')
            cls.drop_tabel(t)

            count += 1

    @classmethod
    def drop_all_tabel(cls, ):
        tabel = load_tables('stock_1m_data', upper=False)

        l = len(tabel)

        l1 = l // 3
        l2 = l // 3 * 2

        p1 = multiprocessing.Process(target=cls.drop_tabel_loop, args=(0, l1, tabel,))
        p2 = multiprocessing.Process(target=cls.drop_tabel_loop, args=(l1, l2, tabel,))
        p3 = multiprocessing.Process(target=cls.drop_tabel_loop, args=(l2, l, tabel,))

        p1.start()
        p2.start()
        p3.start()


if __name__ == '__main__':
    db = 'data1m2022'
    tables = load_tables(db=db)

