from LoadMysql import LoadBasic, StockPoolData


def print_fun(pool, basic):

    print(pool)
    print(basic)
    exit()


pool = StockPoolData.load_board()
pool = pool.sort_values(by=['code']).reset_index(drop=True)

basic = LoadBasic.load_basic()


basic = basic[basic['name'].isin(pool['name'])]

basic = basic.sort_values(by=['code']).reset_index(drop=True)
pool['id'] = basic['id']

# print_fun(pool, basic)

# 替换目标
StockPoolData.replace_board(pool)
