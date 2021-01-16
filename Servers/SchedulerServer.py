# encoding=utf-8
"""
调度服务器
调度服务器定时（每天晚上12点）向爬虫模块发起请求，然后根据响应结果顺序执行：爬虫、上传爬虫结果、分析、上传分析结果
"""
import pika
import json
import datetime
import time
import os
import zipfile
import pymysql
from apscheduler.schedulers.background import BackgroundScheduler
from config import *


class SchedulerServer(object):
    def __init__(self, host, port, username, password, response_queue, crawl_cmd_queue, parse_cmd_queue, vhost='/'):
        self._host = host
        self._port = port
        self._response_queue = response_queue
        self._crawl_cmd_queue = crawl_cmd_queue
        self._parse_cmd_queue = parse_cmd_queue
        self._credentials = pika.PlainCredentials(username, password)
        self._vhost = vhost
        self._connection = None
        self._channel = None
        self.process_crawled_data = None
        self.process_parsed_data = None
        self.upload_url = None
        self.download_url = None

    def set_functions(self, process_crawled_data, process_parsed_data):
        self.process_crawled_data = process_crawled_data
        self.process_parsed_data = process_parsed_data

    def set_urls(self, upload_url, download_url):
        self.upload_url = upload_url
        self.download_url = download_url

    def send_command(self, queue, command):
        try:
            command = json.dumps(command)
            parameters = pika.ConnectionParameters(self._host, self._port, self._vhost, self._credentials)
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.queue_declare(queue=queue)
            channel.basic_publish(
                exchange='',
                routing_key=queue,
                body=command
            )
            print(f'>>>>>>INFO: Send command to Server successfully')
            connection.close()
        except pika.exceptions.AMQPError as e:
            print(f'------AMQPError {self._host}:{self._port} , {e}')

    def send_crawl_command(self):
        date = (datetime.date.today() - datetime.timedelta(1)).strftime("%Y-%m-%d")
        t = int(time.time())
        command = {
            'code': 101,
            'command': 'CrawlData',
            'date': date,
            'filename': f'{t}.txt',
            'url': 'empty'
        }
        print(f'Send command: [CrawlData] to Crawler Server')
        self.send_command(self._crawl_cmd_queue, command)
        print('*********************************************')

    def on_message_callback(self, channel, method, properties, body):
        message = json.loads(body)

        code = message['code']
        cmd = message['command']
        filename = message['filename']
        date = message['date']
        url = message['url']
        status = message['status']
        info = message['info']
        print(f"Receive response of command: [{cmd}]")
        print(f'>>>>>>INFO: Response status: {status}\t info: {info}')

        if self.process_crawled_data is None or self.process_parsed_data is None:
            print("------ERROR: Functions to process data are empty, you need to call set_functions() first")
            return
        if self.upload_url is None or self.download_url is None:
            print("------ERROR: URL for upload and download are empty, you need to call set_urls() first")
            return

        if status == 1:
            if code == 101:
                command = {
                    'code': 102,
                    'command': 'UploadCrawledData',
                    'date': date,
                    'filename': filename,
                    'url': self.upload_url
                }
                print(f'>>>>>>INFO: Send command: [UploadCrawledData] to Crawler Server')
                self.send_command(self._crawl_cmd_queue, command)

            elif code == 102:
                print(f'>>>>>>INFO: Prepare to process raw crawled data')
                self.process_crawled_data(filename)
                command = {
                    'code': 201,
                    'command': 'ParseData',
                    'date': date,
                    'filename': filename,
                    'url': self.download_url
                }
                print(f'>>>>>>INFO: Send command: [ParseData] to Parser Server')
                self.send_command(self._parse_cmd_queue, command)

            elif code == 201:
                command = {
                    'code': 202,
                    'command': 'UploadParsedData',
                    'date': date,
                    'filename': filename,
                    'url': self.upload_url
                }
                print(f'>>>>>>INFO: Send command: [UploadParsedData] to Parser Server')
                self.send_command(self._parse_cmd_queue, command)

            elif code == 202:
                print(f'>>>>>>INFO: Prepare to process parsed data')
                self.process_parsed_data(filename)
                print(f'All Server have finished Crawl and Parse Stock Data of date: [{date}]')

        elif status == -1:
            message = {
                'code': code,
                'command': cmd,
                'date': date,
                'filename': filename,
                'url': url
            }
            print(f'>>>>>>INFO: Send command: {cmd} again')
            if 100< code < 200:
                self.send_command(self._crawl_cmd_queue, message)
            elif 200< code < 300:
                self.send_command(self._parse_cmd_queue, message)

        channel.basic_ack(delivery_tag=method.delivery_tag)
        print('******************************************************************')
        return

    def waiting_for_response(self):
        try:
            parameters = pika.ConnectionParameters(self._host, self._port, self._vhost, self._credentials, heartbeat=0)
            self._connection = pika.BlockingConnection(parameters)

            self._channel = self._connection.channel()
            self._channel.queue_declare(queue=self._response_queue)
            self._channel.basic_consume(self._response_queue, self.on_message_callback)
            print('Scheduler Server Waiting for responses. To exit press CTRL+C')
            print('*************************************************************')
            self._channel.start_consuming()

        except pika.exceptions.AMQPError as e:
            print(f'ERROR: AMQPError {self._host}:{self._port} , {e}')


def process_crawled_data(filename):
    """
    预处理爬虫结果，该函数被调用时，爬虫模块已经将爬虫结果上传到主服务器，并存放在CRAWLDIR文件夹下
    函数会抽取出其中的文本，保存到TEMPDIR文件夹
    :param filename:
    :return:
    """
    un_zip(filename)
    with open(CRAWLDIR + filename, 'r', encoding='utf-8') as f:
        with open(TEMPDIR + filename, 'w', encoding='utf-8') as wf:
            for line in f:
                wf.write('[Scheduler Server]-' + line)
                
def sql_loader_comment(content,zip_file_name):
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
        update_time = zip_file_name
        #update_time = i["update_time"]#设置成数据库识别的格式
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
def sql_loader_news(content,zip_file_name):
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
        #update_time = i["time"]
        update_time = zip_file_name
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

def sql_loader_stockvalue(content,zip_file_name):
    #这部分还得看爬虫模块传回来的数据格式
    text = json.loads(content)
    #连接数据库
    db = pymysql.connect(host='59.110.174.5', port=3306, user='root', passwd='sherlock', db='stock_db', charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    for i in text:
        stock_id = i["stock_id"]
        stock_name = i["stock_name"]
        value_list = i["value_list"]
        time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        #update_time = i["time"]
        update_time = zip_file_name
        sql = "INSERT INTO stock_value_info(stock_id,stock_name,value_list,time,update_time) \
            values('%s','%s','%s','%s','%s')" \
          % (stock_id,stock_name,value_list,time_now,update_time)
        cursor.execute(sql)
        # try:
        #     cursor.execute(sql)
        # except:
        #     print("sql error in "+comment_info)
        #     pass
    db.commit()
    # 关闭数据库连接
    db.close()

def un_zip(file_path):
    """unzip zip file"""
    #file_name是zip文件路径,例如file_name = /Users/songdi/Documents/2020秋课堂资料/云计算技术/data/2021-1-16.zip
    zip_file = zipfile.ZipFile(file_path)
    #获取不带.zip的zip文件路径,例如zip_path = /Users/songdi/Documents/2020秋课堂资料/云计算技术/data/2021-1-16
    (zip_path,ext) = os.path.splitext(file_path)
    #获取zip文件名，file_name_1 = 2020-1-16.zip
    (path_1,file_name_1) = os.path.split(file_path)
    #获取zip文件名日期,zip_file_name = 2021-1-16
    (zip_file_name,ext) = os.path.splitext(file_name_1)
    #print(zip_file_name)
    #print(zip_path)
    if os.path.isdir(zip_path + "_files"):
        pass
    else:
        os.mkdir(zip_path + "_files")
    for names in zip_file.namelist():
        zip_file.extract(names,zip_path + "_files/")
    for names in zip_file.namelist():
        path = zip_path+"_files/"+names
        #存储路径为/Users/songdi/Documents/2020秋课堂资料/云计算技术/data/2021-1-16_files
        #print(path)
        if names == "news.json":
            with open(path, 'r') as f:
                for content in f.readlines():
                    sql_loader_news(content,zip_file_name)
        if names == "comment.json":
            with open(path, 'r') as f:
                for content in f.readlines():
                    sql_loader_comment(content,zip_file_name)
        if names == "stock_value.json":
            with open(path, 'r') as f:
                for content in f.readlines():
                    sql_loader_stockvalue(content,zip_file_name)
    zip_file.close()

def process_parsed_data(filename):
    """
    处理分析结果，该函数被调用时，机器学习模块已经将分析结果上传到主服务器，并存放在PARSEDIR文件夹下
    函数会读取结果并进行相应操作
    :param filename:
    :return:
    """
    pass


if __name__ == "__main__":
    server = SchedulerServer('127.0.0.1', 5672, 'stock_admin', 'CLOUD-2020', 'response', 'crawler_command', 'parser_command')
    server.set_functions(process_crawled_data, process_parsed_data)
    server.set_urls('http://127.0.0.1:5000/upload', 'http://127.0.0.1:5000/download')
    server.send_crawl_command()

    scheduler = BackgroundScheduler()
    #scheduler.add_job(send_crawel_command, 'cron', hour='0', args=[server])
    scheduler.add_job(server.send_crawl_command, 'interval', seconds=600)
    scheduler.start()

    server.waiting_for_response()




