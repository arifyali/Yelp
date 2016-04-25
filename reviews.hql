SET mapred.input.di.recursive=true;
SET hive.mapred.supports.subdirectories=true;
SET hive.groupby.orderby.position.alias=true;

ADD JAR /home/hadoop/Yelp/json-serde-1.3.7-jar.jar;

CREATE EXTERNAL TABLE IF NOT EXISTS reviews(
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

CREATE TABLE IF NOT EXISTS reviews_agg AS
SELECT datediff(max(cast(date as date)), min(cast(date as date))) as days_open, 
       business_id,
       count(*) as total_reviews,
       sum(if(stars=1,1,0)) as one_star,  
       sum(if(stars=2,1,0)) as two_stars,  
       sum(if(stars=3,1,0)) as three_stars,  
       sum(if(stars=4,1,0)) as four_stars,  
       sum(if(stars=5,1,0)) as five_stars
FROM reviews 
GROUP BY business_id;
