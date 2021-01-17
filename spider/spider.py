import os
import re
import time
import json
import random
import requests
from tqdm import tqdm
from bs4 import BeautifulSoup, NavigableString
from spider_utils import remove_angle_brackets, remove_angle_brackets, generate_date, get_agent_pc


class EmSpider():

    def __init__(self):
        self.base_url = "https://www.eastmoney.com/"
        self.news_url = "https://kuaixun.eastmoney.com/"
        self.guba_url = "http://guba.eastmoney.com/"
        self.ip_port = None
        self.last_set_ip = None

    def requests_with_headers(self, url, params=None, host=None, referer=None, max_try=3, timeout=3):

        headers = {
            'User-Agent': get_agent_pc(),
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive'
        }
        if referer:
            headers["referer"] = referer
        if host:
            headers["host"] = host

        proxies = {"https": "https://" + self.ip_port, "http": "http://" + self.ip_port} if self.ip_port else None

        while max_try > 0:
            try:
                if proxies:
                    response = requests.get(url, params=params, headers=headers, proxies=proxies, timeout=timeout)
                else:
                    response = requests.get(url, params=params, headers=headers, timeout=timeout)
                if response.status_code == 200:
                    response.encoding = 'utf-8'
                    return response
            except:
                print("Time out when getting url: %s" % url)
                time.sleep(random.random() * 5)
                max_try -= 1
                if max_try == 1:
                    self.set_new_ip()

        return None

    def set_new_ip(self, cd=16):
        if self.last_set_ip is not None:
            wait_time = max(0, cd - (time.time() - self.last_set_ip))
            time.sleep(wait_time)

        self.ip_port = self.__get_new_ip()
        self.last_set_ip = time.time()

    @staticmethod
    def __get_new_ip():
        ip_proxy_url = "http://api.xdaili.cn/xdaili-api//privateProxy/getDynamicIP/DD20211161611oOZiRF/a2ac1cc7832111e7bcaf7cd30abda612?returnType=2"

        try:
            r = requests.get(ip_proxy_url)
            data = json.loads(r.text)
            status_code = data.get("ERRORCODE")
            if status_code != "0":
                print("Getting DaiLi ip failed, code:%s" % status_code)
                return None
            else:
                d = data.get("RESULT")
                ip, port = d.get("wanIp"), d.get("proxyport")
                ip_port = "%s:%s" % (ip, port)
                print("Use new ip: %s" % ip_port)
                return ip_port
        except:
            return None

    def get_timely_price(self, stock_id):
        url = "http://push2ex.eastmoney.com/getStockFenShi?"
        params = {
            "pagesize": "5000",
            "ut": "7eea3edcaed734bea9cbfc24409ed989",
            "dpt": "wzfscj",
            "cb": "jQuery112406881952581333117_1610724268033",
            "pageindex": "0",
            "id": str(stock_id),
            "sort": "1",
            "ft": "1",
            "code": str(stock_id)[-1],
            "market": "1",
            "_": "1610724268036",
        }

        result = {"stock_id": stock_id, "stock_name": "", "value_list": []}

        response = self.requests_with_headers(url,
                                              params=params,
                                              host="push2ex.eastmoney.com",
                                              referer="http://quote.eastmoney.com")
        if not response:
            print("Getting price of stock %d failed" % stock_id)
            return result

        text = response.text.strip()
        i = text.index("(")
        text = text[i + 1: -2]
        data = json.loads(text).get("data")

        stock_name, value_list = data.get("n"), data.get("data")
        result["stock_name"] = stock_name
        result["value_list"] = value_list

        return result

    def get_news(self, select_time=None, page=None):
        """
        根据给定的时间提取信息。
        :param select_time: 日期 (year,month,day), example:(2021,1,8)
        :param page: 页数，获取前多少页，优先级低于时间。
        :return:
        """

        if not select_time:
            if not page:
                page = 1
            else:
                try:
                    page = int(page)
                except:
                    print("The type of parameter page should be \'int\', but get: %s" % type(page))
                    return []

            page = min(page, 20)  # 新闻最多可以获取20页
            news_info_list = []
            for i in range(1, page + 1):
                news_info_list = news_info_list + self.__news_url_extract(page=i)
        else:
            try:
                select_time = generate_date(select_time, with_year=True)
            except:
                print("The parameter select_time should be tuple (year, month, day)")
                return []

            news_info_list = []
            page, find, find_all = 1, False, False
            while True:
                news_infos = self.__news_url_extract(page=page)
                for news_dict in news_infos:
                    news_time = news_dict["show_time"].split(" ")[0]
                    if news_time == select_time:
                        find = True
                        news_info_list.append(news_dict)
                    else:
                        if find:
                            find_all = True
                            break
                if find_all:
                    break
                page += 1

        print("Successfully get %d news url" % len(news_info_list))
        news_detail_list = []

        for news in news_info_list:
            news_url = news["url"]
            host = news_url.split("/")[2]
            response = self.requests_with_headers(news_url, host=host)
            if not response:
                print("Getting detailed news failed: %s" % news_url)
                continue

            soup = BeautifulSoup(response.text, "html.parser")

            try:
                news_dict = self.__news_info_extract(soup)
                news_dict["url"] = news_url
                news_detail_list.append(news_dict)
                print("Successfully parse detailed news: %s" % news_url)
            except:
                print("Failed to parse detailed news: %s" % news_url)
                continue

        return news_detail_list

    def get_comments(self, stock_id, select_time=None, page=None):
        """
        根据股票代码和日期获取评论
        :param stock_id: 股票代码
        :param select_time: 日期 (year,month,day)，example(2021,1,8)
        :param page: 页数，优先级低于日期
        :return:
        """

        comments_dict = {"stock_info":{}, "comment_list":[]}
        title_set = set()
        stock_id = str(stock_id)

        if not select_time:
            if not page:
                page = 1
            else:
                try:
                    page = int(page)
                except:
                    print("The type of parameter page should be \'int\', but get: %s" % type(page))
                    return comments_dict

            for i in range(1, page + 1):
                url = "http://guba.eastmoney.com/list,%s_%d.html" % (stock_id, i)
                response = self.requests_with_headers(url, host="guba.eastmoney.com", max_try=1)
                if not response:
                    print("Getting comments failed: %s" % url)
                    continue

                soup = BeautifulSoup(response.text, "html.parser")
                stock_info, comment_info = self.__stock_info_extract(soup), self.__comment_info_extract(soup)

                if stock_info is None or comment_info is None:
                    print("Failed to parse stock comments: %s" % url)
                    continue

                comments_dict["stock_info"] = stock_info
                for comment in comment_info:
                    title = comment["title"]
                    if title in title_set:
                        continue
                    title_set.add(title)
                    comments_dict["comment_list"].append(comment)
        else:

            try:
                select_time = generate_date(select_time, with_year=False)
            except:
                print("The parameter select_time should be tuple (year, month, day)")
                return comments_dict

            page, find, find_all = 1, False, False
            while page < 100:
                url = "http://guba.eastmoney.com/list,%s_%d.html" % (stock_id, page)
                response = self.requests_with_headers(url, host="guba.eastmoney.com")
                if not response:
                    print("Getting comments failed: %s" % url)
                    continue

                soup = BeautifulSoup(response.text, "html.parser")
                stock_info, comment_info = self.__stock_info_extract(soup), self.__comment_info_extract(soup)
                if stock_info is None or comment_info is None:
                    print("Failed to parse stock comments: %s" % url)
                    continue

                comments_dict["stock_info"] = stock_info
                for comment in comment_info:
                    comment_time = comment["update_time"].split(" ")[0]
                    title = comment["title"]
                    if title in title_set:
                        continue
                    if comment_time == select_time:
                        comments_dict["comment_list"].append(comment)
                        find = True
                        title_set.add(title)
                    else:
                        if find:
                            find_all = True
                            break

                if find_all:
                    break

                page += 1

        return comments_dict

    def __news_url_extract(self, page):
        url = "https://newsapi.eastmoney.com/kuaixun/v1/getlist_102_ajaxResult_50_{0}_.html/".format(str(page))
        referer = "https://kuaixun.eastmoney.com/"
        host = "newsapi.eastmoney.com"
        response = self.requests_with_headers(url, referer=referer, host=host)

        if not response:
            print("Getting news url list failed: %s" % url)
            return []

        news_list = []

        try:
            data = response.text.replace("var ajaxResult=", "")
            data = json.loads(data)
            for news in data.get("LivesList"):
                news_dict = {"title": news.get("title"), "url": news.get("url_unique"), "digest": news.get("digest"),
                             "show_time": news.get("showtime"), "order_time": news.get("ordertime")}
                news_list.append(news_dict)
        except:
            print("Error happen when extract news url: %s" % url)

        return news_list

    @staticmethod
    def __news_info_extract(soup):
        news_dict = {"title": "", "source": "", "time": "", "abstract": "", "content": "", "quote_words": {}, "url": ""}
        newsContent = soup.find("div", attrs={"class": "newsContent"})

        title = newsContent.find("h1")
        news_dict["title"] = title.contents[0]
        time = newsContent.find("div", attrs={"class": "time"})
        news_dict["time"] = time.contents[0]
        source = newsContent.find("div", attrs={"class": "source"})
        news_dict["source"] = source.attrs["data-source"]

        contentBody = soup.find("div", attrs={"id": "ContentBody"})
        abstract = contentBody.find("div", attrs={"class": "b-review"})
        news_dict["abstract"] = abstract.contents[0]

        for node in contentBody.find_all("p"):
            if len(node.attrs) > 0:
                continue

            news_dict["content"] += remove_angle_brackets(str(node)).strip().replace(" ", "")

            for n in node.find_all(lambda tag: "href" in tag.attrs):
                key_word = n.contents[0]
                word_href = n.attrs["href"]
                news_dict["quote_words"][key_word] = word_href

        return news_dict

    @staticmethod
    def __stock_info_extract(soup):
        stock_info = {"stock_name": None, "stock_id": None}

        try:
            data = soup.find("span", attrs={"id": "stockname"})
            stock_info["stock_id"] = data.attrs["data-popstock"]

            stock_name = remove_angle_brackets(str(data))
            stock_info["stock_name"] = stock_name[:-1] if stock_name[-1] == "吧" else stock_name
        except:
            print("Error happened when extratcing stock info !")
            return None

        return stock_info

    @staticmethod
    def __comment_info_extract(soup):

        def get_info_by_attribute(attribute="articleh"):

            comment_list = []

            for data in soup.find_all("div", attrs={"class": attribute}):
                comment_dict = {"title": "", "author": "", "href": "", "author_href": "", "read_num": -1,
                                "comment_num": -1, "update_time": None}
                for child in data.children:
                    if type(child) == NavigableString:
                        continue

                    if child.attrs["class"][0] == "l1":
                        num = child.contents[0]
                        if "万" in num:
                            num = float(num[:-1]) * 10000
                        comment_dict["read_num"] = int(num)
                    elif child.attrs["class"][0] == "l2":
                        num = child.contents[0]
                        if "万" in num:
                            num = float(num[:-1]) * 10000
                        comment_dict["comment_num"] = int(num)
                    elif child.attrs["class"][0] == "l3":
                        d = child.find(lambda tag: "title" in tag.attrs)
                        comment_dict["title"] = d.attrs["title"]
                        comment_dict["href"] = d.attrs["href"]
                    elif child.attrs["class"][0] == "l4":
                        d = child.find(lambda tag: "href" in tag.attrs)
                        comment_dict["author"] = remove_angle_brackets(str(d))
                        comment_dict["author_href"] = d.attrs["href"]
                    elif child.attrs["class"][0] == "l5":
                        comment_dict["update_time"] = child.contents[0]

                comment_list.append(comment_dict)

            return comment_list

        try:
            comment_list = get_info_by_attribute(attribute="articleh")
            # comment_list = get_info_by_attribute(attribute="articleh normal_post")
        except:
            print("Error happened when extracting comment info !")
            return None

        return comment_list


if __name__ == "__main__":
    spider = EmSpider()

    # result = spider.get_timely_price(stock_id=600016)
    # print(result)

    news = spider.get_news()
    print(news)

    # 读取近1000条新闻，实际读取997条

    # news_list = spider.get_news(page=20)
    # news_json = "../data/news.json"
    # news_txt = "../data/news.txt"
    #
    # if os.path.exists(news_json):
    #     os.remove(news_json)
    # if os.path.exists(news_txt):
    #     os.remove(news_txt)
    #
    # with open(news_json, "a", encoding="utf-8") as f:
    #     f.write(json.dumps(news_list))
    #
    # with open(news_txt, "a", encoding="utf-8") as f:
    #     for i in range(len(news_list)):
    #         news = news_list[i]
    #         f.write("News - %d\n" % (i + 1))
    #         for key, value in news.items():
    #             if type(value) == dict:
    #                 f.write("%s:\n" % key)
    #                 for k, v in value.items():
    #                     f.write("\t%s: %s\n" % (k, v))
    #             else:
    #                 f.write("%s: %s\n" % (key, value))
    #         f.write("\n")

    # with open("../data/stock_name_id.txt", "r", encoding="utf-8") as f:
    #     stock_id_name_list = []
    #     for line in f.readlines():
    #         line = line.strip()
    #         if line == "":
    #             continue
    #         stock_id_name_list.append(line.split("\t"))
    #
    # spider.set_new_ip()
    # i = 0
    #
    # while i < len(stock_id_name_list):
    #     stock_id, stock_name = stock_id_name_list[i]
    #     json_file = "../data/stock_comment/comment_%s.json" % stock_id
    #     txt_file = "../data/stock_comment/comment_%s.txt" % stock_id
    #
    #     if os.path.exists(json_file):
    #         i += 1
    #         continue
    #
    #     # for file in [json_file, txt_file]:
    #     #     if os.path.exists(file):
    #     #         os.remve(file)
    #
    #     print(">>> Start getting stock %s - %s" % (stock_name, stock_id))
    #     comments_dict = spider.get_comments(stock_id=stock_id, page=20)
    #     get_stock_name, get_stock_id = comments_dict["stock_info"]["stock_name"], comments_dict["stock_info"]["stock_id"]
    #
    #     if not ("0" <= get_stock_id[0] <= "9"):
    #         print("【ERROR】: Info", comments_dict["stock_info"])
    #         spider.set_new_ip()
    #         continue
    #
    #     print(">>> Successfully getting %d comments of stock - %s" % (len(comments_dict["comment_list"]), stock_id))
    #     print("Info:", comments_dict["stock_info"])
    #     print("Example:", comments_dict["comment_list"][20])
    #
    #     with open(json_file, "a", encoding="utf-8") as f:
    #         f.write(json.dumps(comments_dict))
    #
    #     with open(txt_file, "a", encoding="utf-8") as f:
    #         stock_info = comments_dict["stock_info"]
    #         for key, value in stock_info.items():
    #             f.write("%s: %s\n" % (key, value))
    #         f.write("\n")
    #
    #         comment_list = comments_dict["comment_list"]
    #         for i in range(len(comment_list)):
    #             comment_info = comment_list[i]
    #             f.write("Comment - %d:\n" % (i + 1))
    #             for key, value in comment_info.items():
    #                 f.write("%s: %s\n" % (key, value))
    #             f.write("\n")
    #
    #     i += 1
    #
    #     # time.sleep(2)
