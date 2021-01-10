# encoding=utf-8
"""
Flask主服务器
负责和前端交互，响应前端请求
同时提供上传和下载函数，用于和爬虫模块、机器学习模块传送文件
"""
from flask import Flask, flash, request, redirect, url_for,send_from_directory
from werkzeug.utils import secure_filename
from config import *


app = Flask(__name__)
app.config['FILEDIR'] = FILEDIR
app.config['CRAWLDIR'] = CRAWLDIR
app.config['PARSEDIR'] = PARSEDIR
app.config['TEMPDIR'] = TEMPDIR

@app.route('/upload', methods=['POST'])
def upload_file():
    """
    上传文件，即接收请求中的文件，保存到本地对应的文件夹中
    请求中包含3个参数
        file：上传的文件内容
        source：发起上传请求的模块(crawler: 爬虫模块，parser: 机器学习分析模块)
        filename：文件名
    返回值：{status, msg}
        成功接收并保存时，status=1， msg=success
        存在错误时，status=-1，msg=错误原因
    """
    if 'file' not in request.files:
        flash('No file part')
        return {'status': -1, 'msg': 'No file part'}
    file = request.files['file']
    if file.filename == '':
        flash('No selected file')
        return {'status': -1, 'msg': 'No selected file'}
    if file:
        source = request.form.get('source')
        filename = request.form.get('filename')
        if source is None or filename is None:
            return {'status': -1, 'msg': 'empty source or filename'}
        filename = secure_filename(filename)
        if source == 'crawler':
            file.save(os.path.join(app.config['CRAWLDIR'], filename))
        elif source == 'parser':
            file.save(os.path.join(app.config['PARSEDIR'], filename))
        else:
            return {'status': -1, 'msg': 'source error'}
        return {'status': 1, 'msg': 'success'}

@app.route('/download', methods=['GET'])
def download_file():
    """
    下载文件，即将请求的文件发送到请求的模块
    请求中包含2个参数
        target：请求哪一模块的文件(crawler: 爬虫模块，parser: 机器学习分析模块，scheduler：对爬虫结果进行预处理)
        filename：文件名
    返回值：
        成功发送时，返回发送的文件
        存在错误时，status=-1，msg=错误原因
    """
    target = request.args.get('target')
    filename = request.args.get('filename')
    if target is None or filename is None:
        return {'status': -1, 'msg': 'empty target or filename'}
    if target == 'scheduler':
        return send_from_directory(app.config['TEMPDIR'], filename)
    elif target == 'parser':
        return send_from_directory(app.config['PARSEDIR'], filename)
    elif target == 'crawler':
        return send_from_directory(app.config['CRAWLDIR'], filename)
    else:
        return {'status': -1, 'msg': 'unrecognized target'}


@app.route('/hello', methods=['GET'])
def hello():
    return 'hello world'


if __name__ == "__main__":
    app.run(debug=True)

