import os
FILEDIR = './stock_data/'
CRAWLDIR = FILEDIR + 'crawl_data/'
TEMPDIR = FILEDIR + 'temp_data/'
PARSEDIR = FILEDIR + 'parse_data/'
def check_dir(dir):
    if os.path.exists(dir):
        return
    os.mkdir(dir)

check_dir(FILEDIR)
check_dir(CRAWLDIR)
check_dir(TEMPDIR)
check_dir(PARSEDIR)
status_d = {
     1: 'success',
    -1: 'fail'
}
code_d = {
    101: {
        'command': 'CrawlData',
        'info': 'crawl data'
    },
    102: {
        'command': 'UploadCrawledData',
        'info': 'upload crawled data to scheduling server'
    },
    201: {
        'command': 'ParseData',
        'info': 'download data from scheduling server and parse'
    },
    202: {
        'command': 'UploadParsedData',
        'info': 'upload parsed data to scheduling server'
    }
}

command_type = {
    'code': 101,
    'command': 'CrawlData',
    'date': '2021-01-09',
    'filename': '2021-01-09.rar',
    'url': 'http://127.0.0.1:5000/upload'
}
response_type = {
    'code': 101,
    'command': 'CrawlData',
    'date': '2021-01-09',
    'filename': '2021-01-09.rar',
    'url': 'http://127.0.0.1:5000/upload',

    'status': 1,
    'info': 'success'
}
