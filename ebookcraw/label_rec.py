import re

import jieba
import jieba.analyse
import pymysql
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pymysql
import MySQLdb
import pandas
import mysql.connector

from numpy import *
from scipy.spatial.distance import pdist
from scipy.spatial.distance import cosine

connection = mysql.connector.connect(host="rm-bp1o0v1268qba8fwyzo.mysql.rds.aliyuncs.com", port=3306, user="fddsh_ro",
                                     password="776b1418FD", database="resourcesystem", charset='utf8')

# data = pandas.read_sql("""
# select fk_user_tid user_id ,label_id,sum(like_count) from
# (SELECT
#
# l.fk_user_tid ,
# d.tid  book_id,
# l.invalid like_count,
#
# ll.tid label_id,
# ll.name label_name
#
# from  t_resource_like l
#
# left JOIN t_book d ON d.fk_resource_tid = l.fk_resource_tid
# left join t_label_record r on d.tid=r.fk_resource_type_tid
# left join  t_label ll on ll.tid=r.fk_label_tid  where d.tid is not null and  l.fk_user_tid is not null limit 10000  ) w   group by user_id,label_id
# """, con=connection)
# print(data.get("content"))

data = pandas.read_sql(
    """select  b.tid book_id ,b.`name`, c.classify_no classify_no,c.`name` from t_book b left join t_classify c on b.fk_classify_tid=c.classify_no  where c.type=2""",
    con=connection)

# 生活 心灵 家庭 职场 创业 人文 作者光临 管理  非樊精读
content = data.to_numpy()
boo_data = []
columns = ["boo_id", "vec"]
dic = ["生活", "心灵", "家庭", "职场", "创业", "人文", "作者光临", "管理", "非樊精读"]
for row in content:
    # print(str(row[0]) + "\t" + row[3])
    for i in range(8):
        if row[3] == dic[i]:
            vec = array([0, 0, 0, 0, 0, 0, 0, 0])
            vec[i] = 1
            # print(vec)
            boo_data.append([row[0], vec])

vec_df = pandas.DataFrame(boo_data, columns=columns)
print(vec_df)

user_df = pandas.read_sql(
    """select user_id,GROUP_CONCAT(name) ,GROUP_CONCAT(cnt) from (
select   user_id ,name,count(tid) cnt from (
SELECT
t1.fk_user_tid user_id,
t2.tid,
t3.name 
from  t_resource_like t1
left JOIN t_book t2 ON t1.fk_resource_tid = t2.fk_resource_tid
left join t_classify t3 on t3.classify_no=t2.fk_classify_tid  where t3.type=2  limit 100000) t4 group by user_id,name ) t5   group by user_id   limit 100
  """,
    con=connection)
user_data = user_df.to_numpy()

print(user_data)
user_class_data = []
user_class_columns = ["user_id", "vec"]
for row in user_data:
    vec = array([0, 0, 0, 0, 0, 0, 0, 0])
    for i in range(8):
        name_array = str(row[1]).split(",")
        cnt_array = str(row[2]).split(",")
        for j in range(len(name_array)):
            for k in range(8):
                if dic[k] == name_array[j]:
                    vec[k] = cnt_array[j]

    user_class_data.append([row[0], vec])

user_class_df = pandas.DataFrame(user_class_data, columns=user_class_columns)

print(user_class_df)

for user_row in user_class_data:
    for book_row in boo_data:
        cos = cosine(user_row[1], book_row[1])
        print(cos)
