
Create table user_table (
	user_name text NULL ,
	password text NULL ,
	stock_info text NULL ,
	keywords text NULL ,
	id  int NOT NULL AUTO_INCREMENT ,
	PRIMARY KEY (id)
	);

Create table stock_comment_info(
	id  int NOT NULL AUTO_INCREMENT ,
	stock_name text NULL,
	stock_id int NULL,
	comment_id int NULL,
	comment_name text NULL,
	comment_info text NULL,
	read_number int NULL,
	comment_number int NULL,
	time datetime NULL,
	sentiment_value DOUBLE NULL,
	update_time datetime NULL,
	PRIMARY KEY (id)
	);
Create table news_info(
	id  int NOT NULL AUTO_INCREMENT ,
	news_id int NULL,
	news_info text NULL,
	news_source text NULL,
	comment_number int NULL,
	keywords text NULL,
	read_number int NULL,
	abstract text NULL,
	time datetime NULL,
	update_time datetime NULL,
	PRIMARY KEY (`id`)
	);