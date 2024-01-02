# -*-coding:utf8-*-

import requests
from selenium import webdriver


def WebDriver():
    # TODO: how to make web driver available
    driver = webdriver.Chrome()

    return driver


def page_source(url, headers=None, cookies=None):
    i = 0

    while i < 5:

        try:
            r = requests.get(url, headers=headers, cookies=cookies, timeout=(5, 10)).text

            return r

        except requests.exceptions.RequestException:

            i += 1


def UrlCode(code: str):

    if code[0] == '0' or code[0] == '3':
        code = f'0.{code}'

    elif code[0] == '6':
        code = f'1.{code}'

    else:
        print(f'东方财富代码无分类:{code};')
        pass

    return code
