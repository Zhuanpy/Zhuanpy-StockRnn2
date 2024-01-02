from LoadMysql import LoadBasicInform
from DB_MySql import MysqlAlchemy as al


def normalize_stock_id():

    """"
    统一股票 id
    """

    data = LoadBasicInform.load_minute()
    data = data[['id', 'name', 'code', 'Classification']]
    db = 'stockbasic'
    tb = 'basic'
    al.pd_append(data, db, tb)
    print(data)


def others_code():

    data = LoadBasicInform.load_minute()
    data = data[['id', 'name', 'code', 'EsCode', 'MarketCode', 'EsDownload', 'TxdMarket', 'HsMarket']]
    db = 'stockbasic'
    tb = 'otherscode'
    al.pd_append(data, db, tb)


if __name__ == "__main__":
    others_code()
