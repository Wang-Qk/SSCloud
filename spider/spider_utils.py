import re
import json
import time
import random
import requests
from tqdm import tqdm


def remove_angle_brackets(sent):
    """
    删去字符串中的“<”“>”以及其之间的内容。
    :param sent: 处理前的字符串
    :return: 处理后的字符串
    """
    pattern = re.compile(r'<.*?>')
    res = re.sub(pattern, "", sent).strip()
    return res


def generate_date(date_tuple, with_year=True):
    year, month, day = date_tuple
    year = str(year)
    month = "0%d" % month if month < 10 else str(month)
    day = "0%d" % day if day < 10 else str(day)
    if with_year:
        date = "%s-%s-%s" % (year, month, day)
    else:
        date = "%s-%s" % (month, day)

    return date


def get_agent_pc():
    user_agent_pc = [
        # 谷歌
        'Mozilla/5.0.html (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.html.2171.71 Safari/537.36',
        'Mozilla/5.0.html (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.html.1271.64 Safari/537.11',
        'Mozilla/5.0.html (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.html.648.133 Safari/534.16',
        # 火狐
        'Mozilla/5.0.html (Windows NT 6.1; WOW64; rv:34.0.html) Gecko/20100101 Firefox/34.0.html',
        'Mozilla/5.0.html (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10',
        # opera
        'Mozilla/5.0.html (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.html.2171.95 Safari/537.36 OPR/26.0.html.1656.60',
        # qq浏览器
        'Mozilla/5.0.html (compatible; MSIE 9.0.html; Windows NT 6.1; WOW64; Trident/5.0.html; SLCC2; .NET CLR 2.0.html.50727; .NET CLR 3.5.30729; .NET CLR 3.0.html.30729; Media Center PC 6.0.html; .NET4.0C; .NET4.0E; QQBrowser/7.0.html.3698.400)',
        # 搜狗浏览器
        'Mozilla/5.0.html (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.html.963.84 Safari/535.11 SE 2.X MetaSr 1.0.html',
        # 360浏览器
        'Mozilla/5.0.html (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.html.1599.101 Safari/537.36',
        'Mozilla/5.0.html (Windows NT 6.1; WOW64; Trident/7.0.html; rv:11.0.html) like Gecko',
        # uc浏览器
        'Mozilla/5.0.html (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.html.2125.122 UBrowser/4.0.html.3214.0.html Safari/537.36',
    ]
    return random.choice(user_agent_pc)


def get_stocks(page, stock_dict, output_file):

    base_url = 'http://31.push2.eastmoney.com/api/qt/clist/get'
    params = {
        'pn': str(page),
        'pz': '20',
        'po': '1',
        'np': '1',
        'ut': 'bd1d9ddb04089700cf9c27f6f7426281',
        'fltt': '2',
        'invt': '2',
        'fid': 'f3',
        'fs': 'm:0 t:6,m:0 t:13,m:0 t:80,m:1 t:2,m:1 t:23',
        'fields': 'f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152',
    }

    headers = {
        'Host': '31.push2.eastmoney.com',
        'Referer': 'http://quote.eastmoney.com/',
        'User-Agent': get_agent_pc()
    }

    r = requests.get(base_url, params=params, headers=headers)
    data = r.text.replace('(', "").replace(')', '').replace(';', '')
    data = json.loads(data)

    with open(output_file, "a", encoding="utf-8") as f:
        for info in data.get('data').get('diff'):
            stock_id, stock_name = info.get("f12"), info.get("f14")
            stock_dict[stock_id] = stock_name
            f.write("%s\t%s\n" % (str(stock_id), stock_name))


if __name__ == "__main__":
    output_file = "../data/stock_name_id.txt"
    stock_dict = {}

    for x in tqdm(range(1, 215 + 1)):
        get_stocks(x, stock_dict, output_file)
        time.sleep(1)