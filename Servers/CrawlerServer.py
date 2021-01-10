# encoding=utf-8
"""
爬虫模块服务器
爬虫模块服务器启动后，时刻等待调度模块的命令，根据调度模块的命令执行相应的函数，并将执行结果返回给调度模块
命令的格式为：{code: 命令代号, command: 命令名称, date: 日期, filename: 文件名, url:相关的URL}
爬虫模块接收的命令有2种：
    code: 101 CrawlData，爬取日期为date的股票信息，并将其保存到filename中
    code: 102 UploadCrawledData，将爬取的文件通过url上传到主服务器
"""
import pika
import json
import requests
from requests_toolbelt import MultipartEncoder

class CrawlerServer(object):
    def __init__(self, host, port, username, password, command_queue, response_queue, vhost='/'):
        self._host = host
        self._port = port
        self._command_queue = command_queue
        self._response_queue = response_queue
        self._credentials = pika.PlainCredentials(username, password)
        self._vhost = vhost
        self._connection = None
        self._channel = None
        self.crawl_func = None
        self.upload_func = None

    def return_response(self, response):
        """
        向调度模块返回响应
        :param response:
        :return:
        """
        try:
            response = json.dumps(response)
            parameters = pika.ConnectionParameters(self._host, self._port, self._vhost, self._credentials)
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.queue_declare(queue=self._response_queue)
            channel.basic_publish(
                exchange='',
                routing_key=self._response_queue,
                body=response
            )
            print(f'>>>>>>INFO: Return command response to Scheduler Server successfully')
            connection.close()
        except pika.exceptions.AMQPError as e:
            print(f'------ERROR: AMQPError {self._host}:{self._port} , {e}')

    def set_functions(self, crawl_func, upload_func):
        """
        设置爬虫函数和上传函数
        :param crawl_func:
        :param upload_func:
        :return:
        """
        self.crawl_func = crawl_func
        self.upload_func = upload_func

    def on_message_callback(self, channel, method, properties, body):
        """
        根据接收的命令执行相关函数
        :param channel:
        :param method:
        :param properties:
        :param body:
        :return:
        """
        command = json.loads(body)
        code = command['code']
        cmd = command['command']
        filename = command['filename']
        date = command['date']
        url = command['url']
        print(f'Receive command: [{cmd}] from Scheduler Server')
        if self.crawl_func is None or self.upload_func is None:
            print('------ERROR: Functions to execute the command are empty, you need to call set_functions() first')
            return

        if code == 101:
            print(f'>>>>>>INFO: Prepare to execute command: [{cmd}]')
            response = self.crawl_func(filename, date)
            print(f'>>>>>>INFO: Finish executing command: [{cmd}]')
            response.update(command)
            self.return_response(response)

        elif code == 102:
            print(f'>>>>>>INFO: Prepare to execute command: [{cmd}]')
            response = self.upload_func(filename, url)
            print(f'>>>>>>INFO: Finish executing command: [{cmd}]')
            response.update(command)
            self.return_response(response)
        else:
            print(f"------Error: Can't recognize command code: {code}")
        channel.basic_ack(delivery_tag=method.delivery_tag)
        print('*****************************************************************')
        return

    def waiting_for_command(self):
        """
        等待命令
        :return:
        """
        try:
            parameters = pika.ConnectionParameters(self._host, self._port, self._vhost, self._credentials, heartbeat=0)
            self._connection = pika.BlockingConnection(parameters)

            self._channel = self._connection.channel()
            self._channel.queue_declare(queue=self._command_queue)
            self._channel.basic_consume(self._command_queue, self.on_message_callback)
            print('Crawler Server Waiting for commands. To exit press CTRL+C')
            print('**********************************************************')
            self._channel.start_consuming()

        except pika.exceptions.AMQPError as e:
            print(f'ERROR: AMQPError {self._host}:{self._port} , {e}')


def crawl(filename, date):
    """
    爬虫函数，爬取指定日期date的股票信息，并将结果保存到filename中
    :param filename: 文件名
    :param date: 日期
    :return: {status, info}
            爬取成功时 status=1，爬取失败时status=-1
    """
    with open(f"crawler_file/{filename}", 'w', encoding='utf-8') as f:
        for i in range(1000000):
            f.write('Hello From Crawler Server\n')
    if True:
        return {'status': 1, 'info': f'crawl stock data successfully'}
    else:
        return {'status': -1, 'info': f'some error'}

def upload(filename, url):
    """
    上传爬取结果
    :param filename: 文件名
    :param url: 上传url
    :return: {status, info}
            上传成功时 status=1，上传失败时status=-1
    """
    # 根据传送的文件格式修改
    payload = {
        'file': (filename, open(f'crawler_file/{filename}', 'rb'), 'text/plain'),
        'source': 'crawler',
        'filename': filename
    }
    m = MultipartEncoder(payload)
    headers = {
        "Content-Type": m.content_type
    }
    r = requests.post(url, headers=headers, data=m)
    rsp = json.loads(r.text)
    if r.status_code == 200 and rsp['status'] == 1:
        return {'status': 1, 'info': f'upload crawl file:{filename} to scheduler successfully'}
    else:
        return {'status': -1, 'info': rsp['msg']}


if __name__ == "__main__":
    server = CrawlerServer('127.0.0.1', 5672, 'stock_admin', 'CLOUD-2020', 'crawler_command', 'response')
    server.set_functions(crawl, upload)
    server.waiting_for_command()

