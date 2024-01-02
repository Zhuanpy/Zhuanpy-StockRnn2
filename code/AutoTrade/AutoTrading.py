import sys
from code.MySql.LoadMysql import StockPoolData
from pywinauto import Application
from pywinauto.keyboard import send_keys
import pyautogui as ag
from time import sleep
import cv2
from utils import match_screenshot
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)


def clic_location(screen, temp):
    temp_size = cv2.imread(temp)
    temp_size = temp_size.shape[:2]
    temp_x = int(temp_size[1] / 2)
    temp_y = int(temp_size[0] / 2)
    result = match_screenshot(screen, temp)
    x1 = result[3][0] + temp_x
    y1 = result[3][1] + temp_y
    return x1, y1


def get_screenshot():
    img = ag.screenshot()
    img.save('targetfile/screenshot.jpg')
    target = 'targetfile/screenshot.jpg'
    return target


def start_trading_app():
    # 判断是否打开交易界面
    r = 0
    app_, win_ = None, None

    while r < 0.9:
        app_ = Application(backend='uia').start(
            f'E:/MyApp/FinancialSoftware/TonghuashunApp/xiadan.exe')  # backend='uia'
        win_ = app_.window(class_name="网上股票交易系统5.0")
        sleep(1)

        # 截屏判断是否打开交易平台

        # 判断是否顺利打开交易平台, 如果匹配大于 0.9 就判断顺利打开了
        target = get_screenshot()
        temp = 'targetfile/loginsuccess.jpg'
        result = match_screenshot(target, temp)

        r = result[1]

        # 如果匹配大于 0.9 就判断顺利打开了
        if r > 0.9:
            print('success open')

        else:
            print('fail open')

    return app_, win_


class TongHuaShunAutoTrade:

    def __init__(self):

        self.app, self.win = start_trading_app()

    def sleep2stop(self, s=0.1):
        x, y = ag.position()

        if x > 1800 or y > 665:
            sys.exit()

        else:
            sleep(s)

    def buy_action(self, code_, num_, price_=None):
        path = 'targetfile/buy/'

        # buy screen
        screen = 'targetfile/screenshot.jpg'
        tmp = f'{path}f1.jpg'
        x, y = clic_location(screen, tmp)
        ag.moveTo(x, y)
        ag.click()
        self.sleep2stop()

        # 证券代码location
        tmp = f'{path}code.jpg'
        x, y = clic_location(screen, tmp)
        ag.moveTo(x, y)
        self.sleep2stop()
        ag.doubleClick()
        self.sleep2stop()
        send_keys(code_)
        self.sleep2stop(0.5)

        # 买入价格
        if price_:
            tmp = f'{path}price.jpg'
            x, y = clic_location(screen, tmp)
            ag.moveTo(x, y)
            self.sleep2stop()
            ag.doubleClick()
            self.sleep2stop(0.5)

            send_keys(price_)
            self.sleep2stop(0.5)

        # 买入数量
        tmp = f'{path}num.jpg'
        x, y = clic_location(screen, tmp)
        ag.moveTo(x, y)
        self.sleep2stop(0.5)
        ag.doubleClick()
        self.sleep2stop()
        send_keys(num_)
        self.sleep2stop()

        # 买入
        tmp = f'{path}buy.jpg'
        x, y = clic_location(screen, tmp)
        ag.moveTo(x, y)
        self.sleep2stop(0.5)
        ag.doubleClick()
        self.sleep2stop()
        send_keys(num_)
        self.sleep2stop()

        # 确认-买入-提示
        screen = get_screenshot()  # 从新获取截屏 出现新的界面
        self.sleep2stop()
        tmp = f'{path}yes.jpg'
        x, y = clic_location(screen, tmp)
        ag.moveTo(x, y)
        self.sleep2stop()
        ag.click()
        self.sleep2stop()

    def sell_action(self, code_, num_, price_=None):
        path = 'targetfile/sell/'

        screen = 'targetfile/screenshot.jpg'
        tmp = f'{path}f2.jpg'
        x, y = clic_location(screen, tmp)
        ag.moveTo(x, y)
        ag.click()
        self.sleep2stop()

        # 证券代码location
        tmp = f'{path}sellcode.jpg'
        x, y = clic_location(screen, tmp)
        ag.moveTo(x, y)
        self.sleep2stop()
        ag.doubleClick()
        self.sleep2stop()
        send_keys(code_)
        self.sleep2stop(0.5)

        # 价格
        if price_:
            tmp = f'{path}sellprice.jpg'
            x, y = clic_location(screen, tmp)
            ag.moveTo(x, y)
            self.sleep2stop()
            ag.doubleClick()
            self.sleep2stop(0.5)

            send_keys(price_)
            self.sleep2stop(0.5)

        # 买入数量
        tmp = f'{path}sellnum.jpg'
        x, y = clic_location(screen, tmp)
        ag.moveTo(x, y)
        self.sleep2stop(0.5)
        ag.doubleClick()
        self.sleep2stop()
        send_keys(num_)
        self.sleep2stop()

        # 确认-卖出
        tmp = f'{path}selling.jpg'
        x, y = clic_location(screen, tmp)
        ag.moveTo(x, y)
        self.sleep2stop(0.5)
        ag.click()
        self.sleep2stop(0.5)

        # 确认-卖出-提示
        screen = get_screenshot()  # 从新获取截屏 出现新的界面
        tmp = f'{path}yes.jpg'
        x, y = clic_location(screen, tmp)
        ag.moveTo(x, y)
        self.sleep2stop(0.5)
        ag.click()
        self.sleep2stop()


class TradePoolFaker:
    """ 买入数量计算， 持股金额约 5000， 大约5000只持有一手；"""

    @classmethod
    def buy_num(cls, close):
        num_ = 5000 / close

        if num_ < 100:
            num_ = 100

        if num_ > 100:
            num_ = (num_ // 100) * 100 + 100

        return int(num_)

    @classmethod
    def buy_pool(cls, score=-5, show_pool=False):
        pool = StockPoolData.load_StockPool()
        pool = pool[['id', 'name', 'code', 'Classification', 'Industry', 'RnnModel', 'close', 'Position', 'BoardBoll']]
        pool = pool[(pool['RnnModel'] < score) &
                    (~pool['Classification'].isin(['创业板', '科创板'])) &
                    (pool['close'] < 200) &
                    (~pool['Industry'].isin(['房地产', '煤炭采选'])) &
                    (pool['Position'] == 0)  # & (pool['BoardBoll'].isin([1, 2]))
                    ].sort_values(by=['RnnModel'])

        if show_pool:
            print(pool)
            sys.exit()

        trade_ = TongHuaShunAutoTrade()
        for i in pool.index:
            code_ = pool.loc[i, 'code']
            close = pool.loc[i, 'close']
            num_ = cls.buy_num(close)
            print(f'code_:{code_}, num_:{num_}')
            price_ = ''

            trade_.buy_action(code_=code_, num_=str(num_))

        print(f'Buy Succeed;')

    @classmethod
    def sell_pool(cls):
        position = StockPoolData.load_StockPool()
        position = position[(position['Position'] == 1) &
                            (position['TradeMethod'] == 1)]

        print(position.head())

        #  打开交易软件
        trade_ = TongHuaShunAutoTrade()

        for index in position.index:
            code_ = position.loc[index, 'code']
            num_ = str(position.loc[index, 'PositionNum'])
            price_ = ''
            trade_.sell_action(code_, num_)

        print(f'Sell Succeed;')


class TradePoolReal:
    """ 买入数量计算， 持股金额约 5000， 大约5000只持有一手；"""

    def buy_num(self, close):

        num_ = 5000 / close

        if num_ <= 100:
            num_ = 100

        if num_ > 100:
            num_ = (num_ // 100) * 100 + 100

        return int(num_)

    def bottom_down_data(self):
        data_ = StockPoolData.load_StockPool()
        data_ = data_[(data_['Trends'] == -1) &
                      (data_['RnnModel'] < -4.5)]
        return data_

    def bottom_up_data(self):
        data_ = StockPoolData.load_StockPool()
        data_ = data_[(data_['Trends'] == 1) &
                      (data_['RnnModel'] <= 1.5)]
        return data_

    def position_data(self):
        data_ = StockPoolData.load_StockPool()
        data_ = data_[(data_['Position'] == 1) &
                      (data_['TradeMethod'] <= 1)]
        return data_

    def buy_pool(self):
        print(f'Buy Succeed;')
        pass

    def sell_pool(self):
        print(f'Sell Succeed;')
        pass


if __name__ == '__main__':
    code = '002475'
    num = '300'
    price = '10'

    trade = TradePoolFaker()
    trade.buy_pool(score=-4, show_pool=True)

    # >py -3,10 -m pip install pyautogui
