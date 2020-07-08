import json
import traceback
from urllib.request import urlopen
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import torch
from urllib import request
from urllib.parse import quote
import string
from selenium import webdriver


def get_amazon(isbn):
    firefox_headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:60.0) Gecko/20100101 Firefox/60.0',
                       'Cookie': 'x-wl-uid=12vLW6HmVo7pBEbTojK0CTiDAzK2CcUemf/KFEnf16JTUksZgN1a1gnQuyymFhSs1p1ZmHFUuvC8='
                       }
    url = 'https://www.amazon.cn/s?k=' + str(
        isbn) + '&rh=n%3A116169071&__mk_zh_CN=%E4%BA%9A%E9%A9%AC%E9%80%8A%E7%BD%91%E7%AB%99&ref=nb_sb_noss'
    response = requests.get(url, headers=firefox_headers)
    content = response.content
    strs = str(content, encoding="utf-8")
    bs = BeautifulSoup(strs)
    price_class = bs.find(class_="a-offscreen")
    print("amazon: " + str(price_class))

    price = price_class.get_text().replace("￥", "")
    print("amazon: " + str(price_class))
    return price


def get_weixin_price(isbn):
    # url = 'https://i.weread.qq.com/store/search?keyword=%E7%88%B1%E7%9A%84%E8%89%BA%E6%9C%AF&author=&authorVids=&categoryId=&count=7&maxIdx=0&type=0&v=2&outer=1&fromBookId=&scope=0&scene=5&filterType=0&filterField='
    url = 'https://i.weread.qq.com/store/search?keyword=' + isbn + '&author=&authorVids=&categoryId=&count=7&maxIdx=0&type=0&v=2&outer=1&fromBookId=&scope=0&scene=5&filterType=0&filterField='
    # url='https://i.weread.qq.com/book/info?bookId=807170'
    data = {}
    headers = {
        'accessToken': 'RKrpgCd0',
        'vid': '12000759',
        'appver': '4.5.1.10141765',
        'User-Agent': 'WeRead/4.5.1 WRBrand/null Dalvik/2.1.0 (Linux; U; Android 9; 16s Build/PKQ1.190202.001)',
        'osver': '9',
        'beta': '0',
        'channelId': '8',
        'basever': '4.5.1.10141765',
        'Host': 'i.weread.qq.com',
        'Connection': 'Keep-Alive',
        'Accept-Encoding': 'gzip',
        'If-Modified-Since': 'Mon, 10 Feb 2020 07:43:14 GMT'
    }
    response = requests.get(url, data, headers=headers)

    print('weixin: ' + str(response.status_code))

    weixin_json = json.loads(str(response.content, encoding="utf-8"))
    price = weixin_json['books'][0]['bookInfo']['price']
    print('weixin: ' + str(price))


def get_fanden(isbn):
    url = 'https://api.dushu.io/fragment/content'
    headers = {'Cookie': 'SERVERID=1ebdc1cc2a3d66d97da2b6d9c90558b4|1581337819|1581337262'}
    data = {'token': '03imMq7oeMfn0Rongoa', 'fragmentId': '200003845'}
    response = requests.post(url, data, headers=headers)
    print(response.text)


def get_weixin_token():
    url = 'https://i.weread.qq.com/guestLogin'
    headers = {

        "appver": "5.5.1.10141765",
        "User-Agent": "WeRead/4.5.1 WRBrand/null Dalvik/2.1.0 (Linux; U; Android 9; 16s Build/PKQ1.190202.001)",
        "osver": "9",
        "beta": "0",
        "channelId": "8",
        "basever": "4.5.1.10141765",
        "Content-Type": "application/json; charset=UTF-8",
        "Content-Length": "161",
        "Host": "i.weread.qq.com",
        "Connection": "Keep-Alive",
        "Accept-Encoding": "gzip"
    }
    data = {"deviceId": "3377144265988864465426732911", "random": 114,
            "signature": "7f0fefc1485271f18c48e31d87e5f2a8b86b30f038001be757465ad5eabe933e", "timestamp": 1581746926383}
    # data={}
    response = requests.post(url, data=json.dumps(data), headers=headers)
    print(response.text)
    print(json.loads(response.text)['accessToken'])
    return json.loads(response.text)


def get_douban(isbn):
    # href = bs0bj.find(class_="cover-link").find('a').get('href')

    # 包装头部
    firefox_headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:60.0) Gecko/20100101 Firefox/60.0'}
    search_url = "https://search.douban.com/book/subject_search?search_text=" + str(isbn) + "&cat=1001"
    print(search_url)
    search_url = "https://search.douban.com/book/subject_search?search_text=9787536091672&cat=1001"
    # 构建请求
    s = quote(search_url, safe=string.printable)
    # response = request.urlopen(search_url)

    # print(search_bs)
    # self.driver = webdriver.Firefox(executable_path='D:\soft\Mozilla Firefox\geckodriver')
    browser = webdriver.Firefox()
    browser.get(search_url)
    url = browser.find_element_by_class_name("cover-link").get_attribute("href")
    response = requests.get(url, headers=firefox_headers)
    content = response.content
    strs = str(content, encoding="utf-8")

    search_bs = BeautifulSoup(strs)
    print(search_bs)
    # browser.close()


get_douban(9787805677316)

# get_weixin_token()

# print("sfsdf,".split(",")[1])


# import torch
# x = torch.rand(5, 3)
# print(x)
#
# print(torch.cuda.is_available())
#
# torch.nn

# l = [1, 2, 3]
#
# print(l[0])
#
# tup1 = ('Google', 'Runoob', 1997, 2000)
# tup2 = (1, 2, 3, 4, 5, 6, 7)
#
# print("tup1[0]: ", tup1[0])
# print("tup2[1:5]: ", tup2[1:5])
