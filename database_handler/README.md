# 存储内容
--- 

## 用户表(user_table)

字段名 | 字段类型 | 字段意义
---|---|---
id|int|id
user_name|text|用户名
password|text|用户密码
stock_info|text|订阅的股票信息
keywords|text|订阅的关键词们


## 股票评论信息表(stock_comment_info)
字段名 | 字段类型 | 字段意义
---|---|---
id|int|id
stock_name|text|股票名
storck_id|text|股票ID
comment_id|text|评论ID
comment_name|text|评论人姓名
comment_info|text|评论信息
read_number|int|阅读量
comment_number|int|浏览量
time|datetime|时间
sentiment_value|double|情感值
update_time|text|上传时间

## 新闻信息表(news_info)
字段名 | 字段类型 | 字段意义
---|---|---
id|int|id|
news_id|int|新闻id
news_info|text|新闻信息
news_source|text|新闻来源
comment_number|int|评论数
keywords|text|关键词们
read_number|int|阅读量
url|text|url
time|datetime|数据库上传时间
update_time|text|发布时间
abstract|text|摘要
## 股票行情表
字段名 | 字段类型 | 字段意义
---|---|---
id|int|id
