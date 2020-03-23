# coding=utf-8

import datetime
import random
import traceback
from datetime import timedelta
from time import sleep

import mysql
import pandas as pd
from sqlalchemy import create_engine

from crawl_mysql import crawl_row


def to_line_week(input_row):
    ebook_id = int(input_row['ebook_id'])
    isbn = str(input_row["isbn"]).replace("-", "").replace(".0", "")
    if isbn is None or isbn == "None":
        return
    ebook_name = str(input_row["ebook_name"])

    row = [ebook_id, isbn, ebook_name, ""]
    row_end = crawl_row(row, isbn)
    return row_end


now = datetime.datetime.now()

# 本周第一天
this_week_start = (now - timedelta(days=now.weekday())).strftime("%Y-%m-%d")

# 上周第一天
last_week_start = (now - timedelta(days=now.weekday() + 7)).strftime("%Y-%m-%d")

engine = create_engine('mysql+pymysql://root:dzx561.@118.25.112.177:3306/bigdata')

# 上周数据
last_query = "SELECT tid,ebook_id,isbn,ebook_name,douban_price,dangdang_price,jingdong_price,amazon_price FROM bigdata.t_ebook_crawl " + \
             "where week_id='" + last_week_start + "' order by tid asc"
last_df = pd.read_sql(sql=last_query, con=engine)

# 书库
lib_query = "SELECT tid,isbn,title FROM bigdata.t_ebook_library "
lib_df = pd.read_sql(sql=lib_query, con=engine)

columns = ["ebook_id", "isbn", "ebook_name", "douban_name", "author", "publisher",
           "rate", "num_raters", "douban_price", "douban_change", "tags", "douban_summary",
           "dangdang_price", "dangdang_change", "jingdong_price", "jingdong_change", "amazon_price", "amazon_change",
           "week_id"]

this_columns = ["ebook_id", "isbn", "ebook_name", "douban_name", "author", "publisher",
                "rate", "num_raters", "douban_price", "tags", "douban_summary",
                "dangdang_price", "jingdong_price", "amazon_price"]

conn = mysql.connector.connect(user='root', password='dzx561.', database='bigdata')
cursor = conn.cursor()

for i in range(len(last_df)):
    last_line = last_df.loc[i]
    print(last_line)
    try:
        # 爬出新数据
        this_data = []
        row = to_line_week(last_line)
        this_data.append(row)
        this_df = pd.DataFrame(this_data, columns=this_columns)
        this_line = this_df.loc[0]

        # 判断价格变动
        if last_line['douban_price'] == this_line['douban_price']:
            douban_change = 0
        else:
            douban_change = 1

        if last_line['dangdang_price'] == this_line['dangdang_price']:
            dangdang_change = 0
        else:
            dangdang_change = 1

        if last_line['jingdong_price'] == this_line['jingdong_price']:
            jingdong_change = 0
        else:
            jingdong_change = 1

        if last_line['amazon_price'] == this_line['amazon_price']:
            amazon_change = 0
        else:
            amazon_change = 1
        # 将价格变动赋值
        this_line["douban_change"] = douban_change
        this_line["dangdang_change"] = dangdang_change
        this_line["jingdong_change"] = jingdong_change
        this_line["amazon_change"] = amazon_change
        # 赋值week_id
        this_line["week_id"] = this_week_start
        # 插入新的数据
        df = pd.DataFrame().append(this_line)
        print(df)

        # 删除本周之前的数据
        cursor.execute(
            "delete from t_ebook_crawl where week_id='" + this_week_start + "' and ebook_id='" + this_data[0] + "'")
        # 插入
        df.to_sql('t_ebook_crawl', engine, if_exists='append', index=False,
                  chunksize=100)
        # sleep(random.randint(1, 300))
    except:
        traceback.print_exc()

conn.close()
cursor.close()
print("输出成功")
