# coding=utf-8


import datetime
import traceback
from datetime import timedelta

import pandas as pd
from sqlalchemy import create_engine

from crawl_mysql import to_line


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
sql_query = "SELECT tid,isbn,ebook_name,create_time FROM bigdata.t_ebook_excel  where  DATE_FORMAT(create_time,'%%Y-%%m-%%d')='" + day_id + "' "
df = pd.read_sql(sql=sql_query, con=engine)

columns = ["tid", "isbn", "ebook_name", "douban_name", "author", "publisher",
           "rate", "num_raters", "douban_price", "tags", "douban_summary",
           "dangdang_price", "jingdong_price", "amazon_price"]

for i in range(len(df)):
    last_line = df.loc[i]
    print(last_line)
    try:
        # 爬出新数据
        this_data = []
        row = to_line(last_line)
        # row.append(this_week_start)
        this_data.append(row)

        # 插入新的数据
        write_df = pd.DataFrame(this_data, columns=columns)
        write_df.to_sql('t_ebook_excel_crawl', engine, if_exists='replace', index=False,
                  chunksize=100)
        # sleep(random.randint(1, 300))
    except:
        traceback.print_exc()

print("输出成功")
