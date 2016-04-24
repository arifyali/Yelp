SET mapred.input.di.recursive=true;
SET hive.mapred.supports.subdirectories=true;
SET hive.groupby.orderby.position.alias=true;

ADD JAR /home/hadoop/Yelp/json-serde-1.3.7-jar.jar;

DROP TABLE IF EXISTS restaurants;
CREATE EXTERNAL TABLE restaurants (
  business_id string,
  full_address string,
  zipcode string,
  hours struct<Monday:struct<open:string,
                             close:string>,
               Tuesday:struct<open:string,
                             close:string>,
               Wednesday:struct<open:string,
                             close:string>,
               Thursday:struct<open:string,
                             close:string>,
               Friday:struct<open:string,
                            close:string>,
               Saturday:struct<open:string,
                             close:string>,
               Sunday:struct<open:string,
                             close:string>>,
  open boolean,
  city string,
  review_count int,
  name string,
  state string,
  stars string,
  attributes struct<Takeout:boolean,
                    DriveThru:boolean,
                    GoodFor:struct<dessert:boolean,
                                   latenight:boolean,
                                   lunch:boolean,
                                   dinner:boolean,
                                   brunch:boolean,
                                   breakfast:boolean>,
                    Caters:boolean,
                    NoiseLevel:string,
                    TakesReservation:boolean,
                    Delivery:boolean,
                    Ambience:struct<romantic:boolean,
                                    intimate:boolean,
                                    classy:boolean,
                                    hipster:boolean,
                                    divey:boolean,
                                    touristy:boolean,
                                    trendy:boolean,
                                    upscale:boolean,
                                    casual:boolean>,
                    Parking:struct<garage:boolean,
                                   street:boolean,
                                   validated:boolean,
                                   lot:boolean,
                                  valet:boolean>,
                    HasTV:boolean,
                    OutdoorSeating:boolean,
                    Attire:string,
                    Alcohol:string,
                    WaiterService:boolean,
                    AcceptsCreditCards:boolean,
                    GoodForKids:boolean,
                    GoodForGroups:boolean,
                    PriceRange:int>
  )
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' 
STORED AS TEXTFILE
LOCATION 's3://gu-anly502-yelp/restaurant_table/';

--Arif's edits
--select * from restaurants limit 0;
--omitted: latitude, longitude, categories, neighborhoods

DROP TABLE IF EXISTS census_data;
CREATE EXTERNAL TABLE census_data (
  regionID STRING,
  RegionName STRING,
  cCity STRING,
  cState STRING,
  Metro STRING,
  CountyName STRING,
  2015_01 DECIMAL,
  2016_01 DECIMAL,
  2016_02 DECIMAL,
  male_age_25_29 DECIMAL,
  female_age_25_29 DECIMAL,
  white DECIMAL,
  black DECIMAL,
  native_americans DECIMAL,
  asian DECIMAL,
  pacific_islander DECIMAL,
  other_race DECIMAL,
  multiple_race DECIMAL,
  hispanic DECIMAL,
  median_household_income DECIMAL,
  median_family_income DECIMAL,
  total_housing_units DECIMAL,
  total_occupied_housing_units DECIMAL,
  total_owner_occupied_housing_units DECIMAL,
  B25075_023E DECIMAL,
  B25075_024E DECIMAL,
  B25075_025E DECIMAL,
  total_vacate_housing_units DECIMAL,
  median_housing_value DECIMAL,
  Median_gross_rent DECIMAL
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION 's3://gu-anly502-yelp/census_table/'
tblproperties("skip.header.line.count"="1");

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

DROP TABLE IF EXISTS review_dates;
 create temporary table review_dates (
   days_open DECIMAL,
   business_id string
);

INSERT OVERWRITE TABLE review_dates
 SELECT datediff(max(cast(date as date)),min(cast(date as date))), business_id 
 FROM reviews
 GROUP BY business_id;


DROP TABLE IF EXISTS census_rest_success; 
CREATE TABLE census_rest_success AS 
SELECT if(restaurants.open, restaurants.stars/review_dates.days_open,0) AS success_metric
, (white+black+asian+native_americans+pacific_islander+other_race+multiple_race+hispanic) AS population 
,restaurants.*,census_data.*
FROM restaurants
JOIN census_data
ON restaurants.zipcode = census_data.regionID
JOIN review_dates
ON restaurants.business_id = review_dates.business_id;
