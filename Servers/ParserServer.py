# encoding=utf-8
"""
机器学习模块服务器
机器学习模块服务器启动后，时刻等待调度模块的命令，根据调度模块的命令执行相应的函数，并将执行结果返回给调度模块
命令的格式为：{code: 命令代号, command: 命令名称, date: 日期, filename: 文件名, url:相关的URL}
机器学习模块接收的命令有2种：
    code: 201 ParseData，首先根据url和filename下载要分析的数据，然后进行分析
    code: 202 UploadParsedData，将分析后的文件结果通过url上传到主服务器
"""
import pika
import json
import requests
from requests_toolbelt import MultipartEncoder

class ParserServer(object):
    def __init__(self, host, port, username, password, command_queue, response_queue, vhost='/'):
        self._host = host
        self._port = port
        self._command_queue = command_queue
        self._response_queue = response_queue
        self._credentials = pika.PlainCredentials(username, password)
        self._vhost = vhost
        self._connection = None
        self._channel = None
        self.parse_func = None
        self.upload_func = None

    def return_response(self, response):
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

    def set_functions(self, parse_func, upload_func):
        self.parse_func = parse_func
        self.upload_func = upload_func

    def on_message_callback(self, channel, method, properties, body):
        command = json.loads(body)
        code = command['code']
        cmd = command['command']
        filename = command['filename']
        date = command['date']
        url = command['url']
        print(f'Receive command: [{cmd}] from Scheduler Server')
        if self.parse_func is None or self.upload_func is None:
            print('------ERROR: Functions to execute the command are empty, you need to call set_functions() first')
            return

        if code == 201:
            print(f'>>>>>>INFO: Prepare to execute command: [{cmd}]')
            response = self.parse_func(filename, url)
            print(f'>>>>>>INFO: Finish executing command: [{cmd}]')
            response.update(command)
            self.return_response(response)

        elif code == 202:
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
        try:
            parameters = pika.ConnectionParameters(self._host, self._port, self._vhost, self._credentials, heartbeat=0)
            self._connection = pika.BlockingConnection(parameters)

            self._channel = self._connection.channel()
            self._channel.queue_declare(queue=self._command_queue)
            self._channel.basic_consume(self._command_queue, self.on_message_callback)
            print('Parser Server Waiting for commands. To exit press CTRL+C')
            print('*********************************************************')
            self._channel.start_consuming()

        except pika.exceptions.AMQPError as e:
            print(f'ERROR: AMQPError {self._host}:{self._port} , {e}')


def parse(filename, url):
    """
    首先下载要分析的文件，然后分析文件并保存分析结果
    :param filename:
    :param url:
    :return:
    """
    # 下载文件
    file = requests.get(url, params={'target': 'scheduler', 'filename': filename}, stream=True)
    with open(f"parser_file/original/{filename}", 'wb+') as f:
        for chunk in file.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
    # 分析文件，并保存分析结果
    with open(f"parser_file/original/{filename}", 'r', encoding='utf-8') as rf:
        with open(f"parser_file/parsed/{filename}", 'w', encoding='utf-8') as wf:
            for line in rf:
                wf.write('[Parser Server]-' + line)
    if True:
        return {'status': 1, 'info': f'parse data successfully'}
    else:
        return {'status': -1, 'info': f'some error'}


def upload(filename, url):
    """
    将分析的结果上传到调度服务器
    :param filename:
    :param url:
    :return: {status, info}
            上传成功时 status=1，上传失败时status=-1
    """
    # send result in filename to scheduling server
    payload = {
        'file': (filename, open(f'parser_file/parsed/{filename}', 'rb'), 'text/plain'),
        'source': 'parser',
        'filename': filename
    }
    m = MultipartEncoder(payload)
    headers = {
        "Content-Type": m.content_type
    }
    r = requests.post(url, headers=headers, data=m)
    rsp = json.loads(r.text)
    if r.status_code == 200 and rsp['status'] == 1:
        return {'status': 1, 'info': f'upload parsed file to scheduler successfully'}
    else:
        return {'status': -1, 'info': rsp['msg']}


if __name__ == "__main__":
    server = ParserServer('127.0.0.1', 5672, 'stock_admin', 'CLOUD-2020', 'parser_command', 'response')
    server.set_functions(parse, upload)
    server.waiting_for_command()

