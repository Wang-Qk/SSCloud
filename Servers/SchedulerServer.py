# encoding=utf-8
"""
调度服务器
调度服务器定时（每天晚上12点）向爬虫模块发起请求，然后根据响应结果顺序执行：爬虫、上传爬虫结果、分析、上传分析结果
"""
import pika
import json
import datetime
import time
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
    with open(CRAWLDIR + filename, 'r', encoding='utf-8') as f:
        with open(TEMPDIR + filename, 'w', encoding='utf-8') as wf:
            for line in f:
                wf.write('[Scheduler Server]-' + line)

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




