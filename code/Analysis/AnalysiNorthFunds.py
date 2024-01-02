from code.MySql.LoadMysql import LoadNortFunds

data = LoadNortFunds.load_amount()

data = data.sort_values(by=['trade_date'])

print(data)