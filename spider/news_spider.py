import json
from spider import EmSpider
from flask import request, Flask, jsonify
from flask_restful import Api, Resource

app = Flask(__name__)
spider = EmSpider()

# result = spider.get_timely_price(stock_id)
# {"stock_id":"", "stock_name":"", "value_list":[]}

# detailed_news_list = spider.get_news(select_time=None, page=None)   select_time: (year, month, day)
# [{"title": "", "source": "", "time": "", "abstract": "", "content": "", "quote_words": {}, "url": ""}, ...]

# comments_dict = spider.get_comments(stock_id, select_time=None, page=None)     select_time: (year, month, day)
# {"stock_info":{}, "comment_list":[{"title": "", "author": "", "href": "", "author_href": "", "read_num": -1,
#                                 "comment_num": -1, "update_time": None}, ...]}


@app.route('/get_timely_price/', methods=['POST'])
def get_timely_price():
    params = json.loads(request.json) if request.json else None

    if not params or "stock_id" not in params:
        return jsonify({"stock_id": "", "stock_name": "", "value_list": []})

    result = spider.get_timely_price(stock_id=params["stock_id"])
    return jsonify(result)


@app.route('/get_news/', methods=['POST'])
def get_news():
    params = json.loads(request.json) if request.json else None

    if params:
        select_time = params["select_time"] if "select_time" in params else None
        page = params["page"] if "page" in params else None
    else:
        select_time, page = None, None

    detailed_news_list = spider.get_news(select_time=select_time, page=page)
    return jsonify(detailed_news_list)


@app.route('/get_comments/', methods=['POST'])
def get_comments():
    params = json.loads(request.json) if request.json else None
    if not params or "stock_id" not in params:
        return jsonify({"stock_info": {}, "comment_list": []})

    stock_id = params["stock_id"]
    select_time = params["select_time"] if "select_time" in params else None
    page = params["page"] if "page" in params else None

    comments_dict = spider.get_comments(stock_id, select_time=select_time, page=page)
    return jsonify(comments_dict)


if __name__=="__main__":
    app.run(host='0.0.0.0', port=8087, debug=True)
