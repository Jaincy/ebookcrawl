# coding=utf-8


import datetime
import traceback
from datetime import timedelta

import pandas as pd
from sqlalchemy import create_engine

from .crawl_mysql import to_line


def getYesterday():
    today = datetime.date.today()
    oneday = datetime.timedelta(days=1)
    yesterday = today - oneday
    return str(yesterday)


day_id = getYesterday()

now = datetime.datetime.now()
# 本周第一天
this_week_start = (now - timedelta(days=now.weekday())).strftime("%Y-%m-%d")

engine = create_engine('mysql+pymysql://root:dzx561.@118.25.112.177:3306/bigdata')
sql_query = "SELECT tid,isbn,title,create_time FROM bigdata.t_ebook_library  where  DATE_FORMAT(create_time,'%%Y-%%m-%%d')='" + day_id + "' limit 2"
df = pd.read_sql(sql=sql_query, con=engine)
print(df)

write_df = []
columns = ["ebook_id", "isbn", "ebook_name", "douban_name", "author", "publisher",
           "rate", "num_raters", "douban_price", "tags", "douban_summary",
           "dangdang_price", "jingdong_price", "amazon_price", "week_id"]

for i in range(len(df)):
    input_row = df.loc[i]
    isbn = str(input_row["isbn"]).replace("-", "").replace(".0", "")
    ebook_name = input_row['title']
    print("isbn   " + str(input_row['isbn']))
    try:
        row = to_line(input_row, "")
        row.append(this_week_start)
        write_df.append(row)

    except:
        traceback.print_exc()


dt = pd.DataFrame(write_df, columns=columns)
print(df)
print("输出成功")

dt.to_sql('t_ebook_crawl', engine, if_exists='append', index=False, chunksize=100)
