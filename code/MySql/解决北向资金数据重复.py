from LoadMysql import LoadNortFunds

data = LoadNortFunds.load_funds2board()

print(data)
exit()
data = data.drop_duplicates(subset=['SECURITY_CODE', 'TRADE_DATE', 'BOARD_CODE'])
print(data)
exit()
# data = data[data['SECURITY_CODE'] == 'BK0459'].tail(30)
LoadNortFunds.replace_funds2board(data)

# todo
#  1. 北向资金表格按年份整理； =》 2018toboard、2018tobstock、2019年、2020年


