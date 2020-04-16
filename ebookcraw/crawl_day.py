# coding=utf-8


import datetime
import sys
import traceback
from datetime import timedelta
import random
from time import sleep
import mysql.connector

import pandas as pd
from sqlalchemy import create_engine

from crawl_mysql import crawl_row


def to_line(input_row):
    ebook_id = int(input_row['tid'])
    isbn = str(input_row["isbn"]).replace("-", "").replace(".0", "")
    if isbn is None or isbn == "None":
        return
    ebook_name = str(input_row["title"])
    row = [ebook_id, isbn, ebook_name]
    row_end = crawl_row(row, isbn)
    return row_end


def getYesterday():
    today = datetime.date.today()
    oneday = datetime.timedelta(days=1)
    yesterday = today - oneday
    return str(yesterday)


if len(sys.argv) == 2:
    day_id = sys.argv[1]

# sys.argv[1]
day_id = getYesterday()

now = datetime.datetime.now()
# 本周第一天
this_week_start = (now - timedelta(days=now.weekday())).strftime("%Y-%m-%d")

engine = create_engine('mysql+pymysql://root:dzx561.@118.25.112.177:3306/bigdata')
sql_query = "SELECT tid,isbn,title,create_time FROM bigdata.t_ebook_library  where  DATE_FORMAT(create_time,'%%Y-%%m-%%d')='" + day_id + "' "
df = pd.read_sql(sql=sql_query, con=engine)

columns = ["ebook_id", "isbn", "ebook_name", "douban_name", "author", "publisher",
           "rate", "num_raters", "douban_price", "tags", "douban_summary",
           "dangdang_price", "jingdong_price", "amazon_price", "week_id"]

for i in range(len(df)):
    conn = mysql.connector.connect(user='root', passwd='dzx561.', database='bigdata', host="118.25.112.177")
    cursor = conn.cursor()
    last_line = df.loc[i]
    print(last_line)
    try:
        # 爬出新数据
        this_data = []
        this_row = to_line(last_line)
        this_row.append(this_week_start)
        this_data.append(this_row)

        write_df = pd.DataFrame(this_data, columns=columns)
        print(write_df)
        # 删除本周之前的数据
        sql = "delete from t_ebook_crawl where week_id=%s and ebook_id=%s"
        value = (str(this_week_start), str(last_line["tid"]))
        cursor.execute(sql, value)
        conn.commit()
        conn.close()
        cursor.close()
        # 插入新的数据
        write_df.to_sql('t_ebook_crawl', engine, if_exists='append', index=False,
                        chunksize=100)

        sleep(random.randint(1, 10))
    except:
        traceback.print_exc()

print("输出成功")
