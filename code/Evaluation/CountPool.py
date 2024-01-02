from code.MySql.LoadMysql import StockPoolData
import pandas as pd


def count_board_by_date(date_):

    """统计板块表格"""

    ''' count board '''

    board = StockPoolData.load_board()

    b_down = board[board['Trends'] == 0].shape[0]
    b_down_ = board[board['Trends'] == 1].shape[0]
    b_up = board[board['Trends'] == 2].shape[0]
    b_up_ = board[board['Trends'] == 3].shape[0]

    sql = f'''update  stockpool.poolcount set 
               _BoardUp = '{b_up}', 
               BoardUp_='{b_up_}', 
               _BoardDown= '{b_down}', 
               BoardDown_= '{b_down_}'
               where date = '{date_}';'''

    StockPoolData.pool_execute_sql(sql)


class PoolCount:
    """
    统计stock pool data;
    """

    @classmethod
    def count_trend(cls, date_=None):

        pool = StockPoolData.load_StockPool()

        if not date_:
            date_ = pool.iloc[0]['RecordDate']

        pool = pool[pool['RecordDate'] == pd.to_datetime(date_)]
        pool = pool[['RecordDate', 'Trends', 'ReTrend', 'RnnModel']].reset_index(drop=True)

        ''' 统计趋势 '''
        pool.loc[pool['Trends'].isin([2, 3]), 'UpDown'] = 1
        pool.loc[pool['Trends'].isin([0, 1]), 'UpDown'] = -1

        ups = pool[pool['UpDown'] == 1].shape[0]
        re_ups = pool[(pool['UpDown'] == 1) & (pool['ReTrend'] == 1)].shape[0]

        downs = pool[pool['UpDown'] == -1].shape[0]
        re_downs = pool[(pool['UpDown'] == -1) & (pool['ReTrend'] == 1)].shape[0]

        _down = pool[pool['Trends'] == 0].shape[0]
        down_ = pool[pool['Trends'] == 1].shape[0]
        _up = pool[pool['Trends'] == 2].shape[0]
        up_ = pool[pool['Trends'] == 3].shape[0]

        ''' 统计Rnn得分'''
        up1 = pool[(pool['RnnModel'] > 0) & (pool['RnnModel'] < 2.5)].shape[0]
        up2 = pool[(pool['RnnModel'] >= 2.5) & (pool['RnnModel'] < 5)].shape[0]
        up3 = pool[(pool['RnnModel'] >= 5)].shape[0]

        down1 = pool[(pool['RnnModel'] > -2.5) & (pool['RnnModel'] < 0)].shape[0]
        down2 = pool[(pool['RnnModel'] > -5) & (pool['RnnModel'] <= -2.5)].shape[0]
        down3 = pool[(pool['RnnModel'] <= -5)].shape[0]

        ''' count board '''
        board = StockPoolData.load_board()

        b_down = board[board['Trends'] == 0].shape[0]
        b_down_ = board[board['Trends'] == 1].shape[0]
        b_up = board[board['Trends'] == 2].shape[0]
        b_up_ = board[board['Trends'] == 3].shape[0]

        ''' values DataFrame '''
        dic = {'date': [date_], 'Up': [ups], 'ReUp': [re_ups], 'Down': [downs], 'ReDown': [re_downs],
               '_BoardUp': [b_up], 'BoardUp_': [b_up_], '_BoardDown': [b_down], 'BoardDown_': [b_down_],
               '_up': [_up], 'up_': [up_], '_down': [_down], 'down_': [down_],
               'Up1': [up1], 'Up2': [up2], 'Up3': [up3], 'Down1': [down1], 'Down2': [down2], 'Down3': [down3]}

        data = pd.DataFrame(dic)

        import sqlalchemy

        try:
            StockPoolData.append_poolCount(data)

        except sqlalchemy.exc.IntegrityError:

            sql = f'''update  {StockPoolData.db_pool}.{StockPoolData.tb_poolCount} set 
            Up = '{ups}', 
            ReUp='{re_ups}', 
            Down= '{downs}', 
            _up = '{_up}', 
            up_  = '{up_}',
            _down = '{_down}', 
            down_ = '{down_}',
            ReDown= '{re_downs}', 
            _BoardUp = '{b_up}', 
            BoardUp_='{b_up_}', 
            _BoardDown= '{b_down}', 
            BoardDown_= '{b_down_}',
            Up1 = '{up1}', 
            Up2 = '{up2}', 
            Up3= '{up3}', 
            Down1 = '{down1}', 
            Down2 = '{down2}', 
            Down3 = '{down3}' 
            where date = '{date_}';'''

            StockPoolData.pool_execute_sql(sql)

        print('Count Pool Trends Success;')

        return data


if __name__ == '__main__':
    date_ = '2022-11-18'
    count_board_by_date(date_=date_)
