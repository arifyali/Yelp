SET mapred.input.di.recursive=true;
SET hive.mapred.supports.subdirectories=true;
SET hive.groupby.orderby.position.alias=true;

ADD JAR /home/hadoop/Yelp/json-serde-1.3.7-jar.jar;

DROP TABLE IF EXISTS reviews;
CREATE EXTERNAL TABLE reviews(
  votes struct<funny:boolean,
               useful:boolean,
	       cool:boolean>,
  user_id string,
  review_id string,
  stars int,
  date string,
  text string,
  business_id string
  )
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' 
STORED AS TEXTFILE
LOCATION 's3://gu-anly502-yelp/review_table/';

SELECT datediff(max(cast(date as date)),min(cast(date as date))), business_id  
FROM reviews 
GROUP BY business_id 
ORDER BY business_id DESC limit 20;
