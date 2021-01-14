import pymysql
import json
import re
import time
import datetime


def sql_loader_comment(content):
    text = json.loads(content)
    #连接数据库
    db = pymysql.connect(host='59.110.174.5', port=3306, user='root', passwd='sherlock', db='stock_db', charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    stock_info = text["stock_info"]
    stock_name = stock_info["stock_name"]
    stock_id = stock_info["stock_id"]
    comment_list = text["comment_list"]
    comment_num = 0
    for i in comment_list:
        comment_id = comment_num
        comment_num = comment_num + 1
        comment_name = i["author"]
        comment_info = i["title"]
        read_number = i["read_num"]
        comment_number = i["comment_num"]#这里需要修改数据库的字段值
        time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(comment_info)
        #sentiment_value
        update_time = i["update_time"]#设置成数据库识别的格式
        #datetime.striptime(update_time,"%m-%d %h-%m")
        #print(update_time)
        sql = "INSERT INTO stock_comment_info(stock_id,stock_name,comment_id,comment_name,comment_info,read_number,comment_number,update_time,time) \
            values('%s','%s','%d','%s','%s','%d','%d','%s','%s')" \
          % (stock_id,stock_name,comment_id,comment_name,comment_info,read_number,comment_number,update_time,time_now)
        cursor.execute(sql)
        # try:
        #     cursor.execute(sql)
        # except:
        #     print("sql error in "+comment_info)
        #     pass
    db.commit()
    # 关闭数据库连接
    db.close()
def sql_loader_news(content):
    text = json.loads(content)
    #连接数据库
    db = pymysql.connect(host='59.110.174.5', port=3306, user='root', passwd='sherlock', db='stock_db', charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    for i in text:
        news_info = i["content"]
        news_source = i["source"]
        #comment_number
        quote_words = i["quote_words"]
        keywords = quote_words.keys

        #read_number
        url = i["url"]
        time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        update_time = i["time"]
        abstract = i["abstract"]
        print(news_info)
        sql = "INSERT INTO news_info(news_info,news_source,keywords,url,time,update_time,abstract) \
            values('%s','%s','%s','%s','%s','%s','%s')" \
          % (news_info,news_source,str(keywords),url,time_now,update_time,abstract)
        cursor.execute(sql)
        # try:
        #     cursor.execute(sql)
        # except:
        #     print("sql error in "+comment_info)
        #     pass
    db.commit()
    # 关闭数据库连接
    db.close()
if __name__ == '__main__':
    input_str = input("请输入处理的数据类型，news or comment:\n")
    if input_str == "news":
        with open("/Users/songdi/Documents/2020秋课堂资料/云计算技术/data/news_01-08.json", 'r') as f:
            for content in f.readlines():
                sql_loader_news(content)
    if input_str == "comment":
        with open("/Users/songdi/Documents/2020秋课堂资料/云计算技术/data/comment_002417_01-06.json", 'r') as f:
            for content in f.readlines():
                sql_loader_comment(content)

