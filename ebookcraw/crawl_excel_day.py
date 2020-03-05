# coding=utf-8


from sqlalchemy import create_engine
import json
import traceback
from urllib.request import urlopen
import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

from ebookcraw.crawl_mysql import to_line


def getYesterday():
    today = datetime.date.today()
    oneday = datetime.timedelta(days=1)
    yesterday = today - oneday
    return str(yesterday)


day_id = getYesterday()

engine = create_engine('mysql+pymysql://root:dzx561.@118.25.112.177:3306/bigdata')
sql_query = "SELECT tid,isbn,title,create_time FROM bigdata.t_ebook_excel  where  DATE_FORMAT(create_time,'%%Y-%%m-%%d')='" + day_id + "' limit 2"
df = pd.read_sql(sql=sql_query, con=engine)
print(df)

write_df = []
columns = ["ebook_id", "isbn", "ebook_name", "douban_name", "author", "publisher",
           "rate", "num_raters", "douban_price", "tags", "douban_summary",
           "dangdang_price", "jingdong_price", "amazon_price"]

for i in range(len(df)):
    input_row = df.loc[i]
    isbn = str(input_row["isbn"]).replace("-", "").replace(".0", "")
    ebook_name = input_row['title']
    print("isbn   " + str(input_row['isbn']))
    try:
        write_df.append(to_line(input_row, ""))

    except:
        traceback.print_exc()
        # write_df.append([isbn, ebook_name, "", "", "",
        #                  "", 0, "", "", "",
        #                  "", "", "",])

dt = pd.DataFrame(write_df, columns=columns)
print(df)
print("输出成功")

dt.to_sql('t_ebook_excel_crawl', engine, if_exists='append', index=False, chunksize=100)
