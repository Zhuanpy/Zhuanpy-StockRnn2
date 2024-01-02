from LoadMysql import LoadBasicInform
from code.Normal import StockCode


def Stocks(stock):

    tables = LoadBasicInform.load_minute()

    try:
        name = stock
        code = tables[tables['name'] == name].iloc[0]['code']
        id_ = tables[tables['name'] == name].iloc[0]['id']

    except IndexError:

        try:
            code = stock
            code = StockCode.stand_code(code)  # stand code

            name = tables[tables['code'] == code].iloc[0]['name']
            id_ = tables[tables['code'] == code].iloc[0]['id']

        except IndexError:
            try:
                id_ = stock
                name = tables[tables['id'] == id_].iloc[0]['name']
                code = tables[tables['id'] == id_].iloc[0]['code']

            except IndexError:
                name = None
                code = None
                id_ = None
    r = (name, code, id_)
    return r
# sinaptca@airchina.com  # 0065 6541 8543
