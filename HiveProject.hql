
# Tweets Sentiment Analysis

# Lets copy the required data to our local hdfs folder
hadoop fs -cp /data/sentiment_analysis /user/mohanakotkar/

# To check
hadoop fs -ls 

# Preparation Steps
# As we are going to provide JSON format as input
# we need to add hive json dependency jar i.e JSON SerDe
# Launch Hive and run following command
hive
hive >  ADD JAR hdfs:///data/hive/json-serde-1.1.9.9-Hive13-jar-with-dependencies.jar;
hive >  SET hive.support.sql11.reserved.keywords=false;

# Create new database 
hive >  CREATE TABLE IF NOT EXISTS analysis()

# Initialize new created database analysis for use
hive >  USE analysis;

# Create new table for tweets
hive >  CREATE EXTERNAL TABLE tweets (id BIGINT,created_at STRING,source STRING,favorited BOOLEAN,retweet_count INT,retweeted_status STRUCT<text:STRING,users:STRUCT<screen_name:STRING, name:STRING>>,entities STRUCT<urls:ARRAY<STRUCT<expanded_url:STRING>>,
		user_mentions:ARRAY<STRUCT<screen_name:STRING,name:STRING>>,
		hashtags:ARRAY<STRUCT<text:STRING>>>,
		text STRING,
		user STRUCT<screen_name:STRING,name:STRING,friends_count:INT,followers_count:INT,statuses_count:INT,verified:BOOLEAN,utc_offset:STRING, -- was INT but nulls are strings
		time_zone:STRING>,
		in_reply_to_screen_name STRING,
		year INT,
		month INT,
		day INT,
		hour INT) ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' WITH SERDEPROPERTIES ("ignore.malformed.json"="true") LOCATION '/user/mohanakotkar8434/SentimentFiles/SentimentFiles/upload/data/tweets_raw';

# Checking Table
hive > DESCRIBE tweets;
hive > SELECT count(id) FROM tweets;
hive > SELECT * FROM tweets LIMIT 10;

# Now lets create new table for Words Dictionary which holds the polarity of each words.
# Polarity Means if the word has positive, negative or neutral sentiment.

hive >	CREATE EXTERNAL TABLE dictionary (type STRING,
		length STRING,
		word STRING,
		pos STRING,
		stemmed STRING,
		polarity STRING)
		ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
		LOCATION '/user/mohanakotkar8434/SentimentFiles/SentimentFiles/upload/data/dictionary';

# Now knowing that the user may tweet from any country, we need to map the time zone of the country the user is in.
hive > 	CREATE EXTERNAL TABLE time_zone_map(
		time_zone STRING,
		country STRING,
		notes STRING)
		ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
		STORED AS TEXTFILE
		LOCATION '/user/mohanakotkar8434/SentimentFiles/SentimentFiles/upload/data/time_zone_map';


# Lets take the required information from tweets table to create VIEW and also clean the created_at column as timestamp
hive >  CREATE VIEW tweets_simple AS
		SELECT          
		id,
		cast(from_unixtime(unix_timestamp(concat( '2013 ', substring(created_at,5,15)), 'yyyy MMM dd hh:mm:ss')) as timestamp) ts,
		text,
		user.time_zone 
		FROM tweets;

# Joining New Created tweets_simple table with time_zone_map to find the country from which the user tweeted
hive >  CREATE VIEW tweets_clean AS
		SELECT
		id,
		ts,
		text,
		m.country 
		FROM tweets_simple t LEFT OUTER JOIN time_zone_map m ON t.time_zone = m.time_zone;

# Create l1 view to convert each tweet into lower case and explode into a list of words.
hive >  CREATE VIEW l1 AS SELECT id, words from tweets LATERAL VIEW EXPLODE(sentences(lower(words))) dummy as words;
hive >  SELECT * FROM l1 LIMIT 10;

# Create l2 view to store each word from tweet in a row.

hive >  CREATE VIEW l2 AS SELECT id, word FROM l1 LATERAL VIEW EXPLODE(words) dummy as word;

# Create view l3 to join l2 view with dictionary table to get the polarity of each word.
hive >  CREATE VIEW l3 AS SELECT id, l2.word , case d.polarity
		when 'negative' then -1
		when 'positive' then 1
		else 0 end as polarity
		FROM l2 LEFT OUTER JOIN dictionary d on l2.word=d.word;

# Create tweets sentiment table 
CREATE TABLE tweets_sentiment STORED AS ORC AS SELECT
id, CASE
WHEN SUM(polarity) > 0 then 'POSITIVE'
WHEN SUM(polarity) < 0 then 'NEGATIVE'
else 'NEUTRAL' end as sentiment
FROM l3 GROUP BY id;

# Creating Final Table with Sentiment mapping against each tweets by joining tweets_clean table with tweets_sentiment
CREATE TABLE tweetsbi STORED AS ORC AS SELECT t.*, s.sentiment FROM tweets_clean t LEFT OUTER JOIN tweets_sentiment s ON t.id=s.id;

 


