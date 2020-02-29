# coding=utf-8
import json
import traceback
from urllib.request import urlopen
import pandas as pd
import requests
from bs4 import BeautifulSoup


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
    data = {"deviceId": "3337144251034016266476571997", "random": 269,
            "signature": "7c6185191f5d141707b156ace593840280790992547106f41d2c65b6e9596c0d", "timestamp": 1581588963117}
    # data={}
    response = requests.post(url, data=json.dumps(data), headers=headers)
    print(response.text)
    print(json.loads(response.text)['accessToken'])
    return json.loads(response.text)


def get_weixin_price(isbn, weixin_token):
    # url = 'https://i.weread.qq.com/store/search?keyword=%E7%88%B1%E7%9A%84%E8%89%BA%E6%9C%AF&author=&authorVids=&categoryId=&count=7&maxIdx=0&type=0&v=2&outer=1&fromBookId=&scope=0&scene=5&filterType=0&filterField='
    url = 'https://i.weread.qq.com/store/search?keyword=' + isbn + '&author=&authorVids=&categoryId=&count=7&maxIdx=0&type=0&v=2&outer=1&fromBookId=&scope=0&scene=5&filterType=0&filterField='
    # url='https://i.weread.qq.com/book/info?bookId=807170'
    data = {}
    headers = {
        'accessToken': weixin_token['accessToken'],
        'vid': weixin_token['vid'],
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
    return str(price)


def get_amazon(isbn):
    firefox_headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:60.0) Gecko/20100101 Firefox/60.0',
                       'Cookie': 'x-wl-uid=12vLW6HmVo7pBEbTojK0CTiDAzK2CcUemf/KFEnf16JTUksZgN1a1gnQuyymFhSs1p1ZmHFUuvC8='
                       }
    url = 'https://www.amazon.cn/s?k=' + isbn + '&rh=n%3A116169071&__mk_zh_CN=%E4%BA%9A%E9%A9%AC%E9%80%8A%E7%BD%91%E7%AB%99&ref=nb_sb_noss'

    response = requests.get(url, headers=firefox_headers)
    print("amazon: " + str(response))

    content = response.content
    strs = str(content, encoding="utf-8")
    bs = BeautifulSoup(strs)
    price_class = bs.find(class_="a-offscreen")
    price = price_class.get_text().replace("￥", "")

    print("amazon: " + str(price))
    return price


def get_dangdang(isbn):
    html = urlopen("http://search.dangdang.com/?key=" + isbn + "&category_path=98.00.00.00.00.00#J_tab")  # 获取html结构与内容

    bs0bj = BeautifulSoup(html)  # 提取name信息

    namelist = bs0bj.find(class_="search_now_price")

    dd_price = str(namelist.get_text()).replace("¥", "")
    print('dangdang: ' + dd_price)
    return dd_price


def get_jingdong(isbn):
    html_jd = urlopen("https://s-e.jd.com/Search?key=" + isbn + "&enc=utf-8")
    bs0bj_jd = BeautifulSoup(html_jd)
    skuId = bs0bj_jd.find(class_="p-img").find('a').get('href').replace('//e.jd.com/', '').replace('.html', '')

    html_sku = urlopen("http://p.3.cn/prices/mgets?skuIds=" + skuId)
    bs0bj_sku = BeautifulSoup(html_sku)
    url = "http://p.3.cn/prices/mgets?skuIds=" + skuId
    # 包装头部
    firefox_headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:60.0) Gecko/20100101 Firefox/60.0'}
    # 构建请求
    response = requests.get(url, headers=firefox_headers)
    content = response.content
    strs = str(content, encoding="utf-8")

    # 截取字符串
    djson = json.loads(strs.replace("[", "").replace("]", ""))
    price = djson['p']
    print('jingdong: ' + price)
    return price


def get_douban(isbn):
    url = "https://douban-api.uieee.com/v2/book/isbn/" + isbn
    print("url:  " + url)
    # 包装头部
    firefox_headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:60.0) Gecko/20100101 Firefox/60.0'}
    # 构建请求
    response = requests.get(url, headers=firefox_headers)
    content = response.content
    strs = str(content, encoding="utf-8")
    print("douban: " + str(response))
    # 截取字符串
    djson = json.loads(strs)
    if strs.__contains__("ebook_price"):
        eprice = djson['ebook_price']
    else:
        eprice = "  "

    l = ""
    for i in range(len(djson["tags"])):
        l = l + djson["tags"][i]["name"] + "  "
    # 3. 构建列表头 ISBN	名称	作者	出版时间	对应出版社		豆瓣书名	豆瓣评分	参与评分人数	豆瓣电子书价格	豆瓣标签
    row = [djson["title"], djson['author'][0], djson['publisher'], djson['rating']['average'],
           djson['rating']['numRaters'],
           eprice, l, djson['summary']]
    print("douban: " + eprice)
    return row


def to_xlsx(input_row, weixin_token):
    isbn = str(input_row[0]).replace("-", "").replace(".0", "")
    row = [isbn, input_row[1]]
    try:
        row.extend(get_douban(isbn))
    except:
        traceback.print_exc()
        row.extend(["", "", "", "", "", "", "", ""])

    try:
        row.extend([get_dangdang(isbn)])
    except:
        traceback.print_exc()
        row.extend([""])

    try:
        row.extend([get_jingdong(isbn)])
    except:
        traceback.print_exc()
        row.extend([""])

    try:
        row.extend([get_amazon(isbn)])
    except:
        traceback.print_exc()
        row.extend([""])

    # try:
    #     row.extend([get_weixin_price(isbn, weixin_token)])
    # except:
    #     traceback.print_exc()
    #     row.extend([""])

    print(row)
    write_df.append(row)


# weixin_token = get_weixin_token()
df = pd.read_excel("../excel/input.xlsx")
write_df = []
columns = ["isbn", "书名", "豆瓣书名", "作者", "出版社",
           "评分", "评分人数", "电子书价格", "标签", "豆瓣简介",
           "当当电子书价格", "京东电子书价格", "亚马逊电子书价格"]
# columns = ["当当电子书价格", "京东电子书价格"]

for i in range(len(df)):
    input_row = df.loc[i]
    print(input_row)
    try:
        to_xlsx(input_row, "")
    except:
        traceback.print_exc()
        # write_df.append([input_row[0], input_row[5], input_row[3], input_row[1], input_row[2],
        #                  "", "", "", "", "", "", "", ""])
        write_df.append([input_row[0], input_row[1], "", "", "",
                         "", "", "", "", "",
                         "", "", ""])

dt = pd.DataFrame(write_df, columns=columns)
dt.to_excel("../excel/output.xlsx")
print("输出成功")
