import re

import jieba
import jieba.analyse
import pymysql
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pymysql
import pandas
import mysql.connector


def translate(str):
    line = str.strip()  # 处理前进行相关的处理，包括转换成Unicode等
    pattern = re.compile('[^\u4e00-\u9fa50-9]')  # 中文的编码范围是：\u4e00到\u9fa5
    zh = " ".join(pattern.split(line)).strip()
    outStr = zh  # 经过相关处理后得到中文的文本
    return re.sub(r"[0-9]", "", outStr).replace(" ", "").replace(
        "西瓜插件运行于电脑浏览器上的插件可在公众号后台找优质文章素材一键美化排版检测文章违规查看任意公众号粉丝阅读数", "").replace("点击", "").replace("导图", "").replace("音视频",
                                                                                                                  "").replace(
        "本书", "").replace("按钮", "").replace("页码", "")


def keyword(content):
    keyword = str(content).replace(r"[a-z]", "").replace("[A-Z]", "").replace(
        "[0-9]", "").replace("[;.\\,<>\"/:-=%()《》!\\-，\\[\\]。“”、___?？……；：&]", "").replace(" ", "")

    new_key = re.sub(r"[;=\"%<>:\\/()◆《》＆‘’.,％&，＋＝℃#。≠°÷「×＋＞」‧*・！?：→!★、√“”？…；•+$『』~'－\\[【】—（）；|．·-]", "",
                     re.sub(r"\w", "", keyword))
    return content


# conf = SparkConf().setAppName("SparkEtl_Mysql").setMaster("local[*]")
# spark = SparkSession \
#     .builder \
#     .appName("Python Spark SQL basic example") \
#     .config("spark.driver.extraClassPath",
#             "D:\\360MoveData\\Users\\60944\\Desktop\\jar\\mysql-connector-java-5.1.41.jar") \
#     .getOrCreate()
#
# t_fragment = spark.read.format("jdbc").option("url",
#                                               "jdbc:mysql://rm-bp1o0v1268qba8fwyzo.mysql.rds.aliyuncs.com:3306/resourcesystem") \
#     .option("driver", "com.mysql.jdbc.Driver") \
#     .option("dbtable", "t_fragment") \
#     .option("user", "fddsh_ro") \
#     .option("password", "776b1418FD").load()
# t_fragment.createOrReplaceTempView("t_fragment")
#
# t_book = spark.read.format("jdbc").option("url",
#                                           "jdbc:mysql://rm-bp1o0v1268qba8fwyzo.mysql.rds.aliyuncs.com:3306/resourcesystem") \
#     .option("driver", "com.mysql.jdbc.Driver") \
#     .option("dbtable", "t_book") \
#     .option("user", "fddsh_ro") \
#     .option("password", "776b1418FD").load()
# t_book.createOrReplaceTempView("t_book")
#
# t_book.show()
#
# t_fragment.select("tid", "fk_resource_tid", "title", "summary", "content")
# # summary = t_fragment.select("summary")
# # content = str(t_fragment.select("content").head())
# #
# df1 = spark.sql(
#     "select a.tid id,name,b.content content, b.summary  summary from t_book a left join t_fragment b on a.fk_resource_tid=b.fk_resource_tid   where type=1 and a.tid is not null")
# df1.write.("/t_content_text")

# .replace("[a-z]", "").replace("[A-Z]", "").replace(
# "[0-9]", "").replace("[;.\\,<>\"/:-=%()《》!\\-，\\[\\]。“”、___?？……；：&]", "").replace(" ", "")


# df2 = spark.read.parquet("/t_content")
# df2.show()
# list1 = df2.select("id", "name", "content", "summary").take(100).rdd
# for i in range(len(list1)):
#     print(keyword(list1[i][3]))

# print(type(content))
# print(content(0).__str__())
# cuts = jieba.cut("content")
# #
# text = "能让你过的更幸福的一本书"
# # print(u"[精确模式]: ", "/ ".join(cuts))
#
# keywords = jieba.analyse.extract_tags(content, topK=5, withWeight=False, allowPOS=())
# print(keywords)

# summary.rdd.collect().foreach(lambda l: print(u"[精确模式]: ", "/ ".join(jieba.cut(l))))
# df=t_fragment.select("tid","fk_resource_tid", "title", "summary")
# print(t_fragment.rdd.partitions.size)

# db = pymysql.connect(host="rm-bp1o0v1268qba8fwyzo.mysql.rds.aliyuncs.com", port=3306, user="fddsh_ro",
#                      password="776b1418FD", database="resourcesystem", charset='utf8')
# # 使用 cursor() 方法创建一个游标对象 cursor
# cursor = db.cursor()

# 使用 execute()  方法执行 SQL 查询
# cursor.execute("SELECT VERSION()")

connection = mysql.connector.connect(host="rm-bp1o0v1268qba8fwyzo.mysql.rds.aliyuncs.com", port=3306, user="fddsh_ro",
                                     password="776b1418FD", database="resourcesystem", charset='utf8')

data = pandas.read_sql("""
select a.tid id,name,b.content content, b.summary  summary from t_book a left join t_fragment b on a.fk_resource_tid=b.fk_resource_tid   where type=1 and a.tid is not null
""", con=connection)
# print(data.get("content"))
content = data.get_values()
for row in content:
    print(row[1] + "\t" + jieba.analyse.extract_tags(translate(row[2]), topK=6, withWeight=False,
                                                     allowPOS=()).__str__() + "\t" + jieba.analyse.extract_tags(
        translate(row[2]), topK=3, withWeight=False, allowPOS=()).__str__())

