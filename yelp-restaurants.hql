SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;
SET hive.groupby.orderby.position.alias=true;

--TODO 
--remove non-restaurants in son file
--remove spaces in column names: Good For, Noise Level, Takes Reservations, Has TV, Outdoor Seating, Waiter Service, Accepts Credit Cards, Good For Kids, Good For Groups, Price Range
--test on cluster, starting with baby json example

ADD JAR s3://elasticmapreduce/samples/hive-ads/libs/jsonserde.jar;

DROP TABLE IF EXISTS restaurants;
CREATE EXTERNAL TABLE restaurants (
  business_id string,
  city string,
  monday_open string
  )
--CREATE TABLE restaurants (
--  business_id string,
--  full_address string
--  zipcode string,
--  hours struct<Monday:struct<open:string,
--                             close:string>,
--               Tuesday:struct<open:string,
--                             close:string>,
--               Wednesday:struct<open:string,
--                             close:string>,
--               Thursday:struct<open:string,
--                             close:string>,
--               Friday:struct<open:string,
--                            close:string>,
--               Saturday:struct<open:string,
--                             close:string>,
--               Sunday:struct<open:string,
--                             close:string>>,
--  open boolean,
--  city string,
--  review_count int,
--  name string
--  state string,
--  stars decimal,
--  attributes struct<Take-out:boolean,
--                    Drive-Thru:boolean,
--                   GoodFor:struct<dessert:boolean,
--                                   latenight:boolean,
--                                   lunch:boolean,
--                                   dinner:boolean,
--                                   brunch:boolean,
--                                   breakfast:boolean>,
--                    Caters:boolean,
--                    NoiseLevel:string,
--                    TakesReservation:boolean,
--                    Delivery:boolean,
--                    Ambience:struct<romantic:boolean,
--                                    intimate:boolean,
--                                    classy:boolean,
--                                    hipster:boolean,
--                                    divey:boolean,
--                                    touristy:boolean,
--                                    trendy:boolean,
--                                    upscale:boolean,
--                                    casual:boolean>,
--                    Parking:struct<garage:boolean,
--                                   street:boolean,
--                                   validated:boolean,
--                                   lot:boolean,
--                                   valet:boolean>,
--                    HasTV:boolean,
--                    OutdoorSeating:boolean,
--                    Attire:string,
--                    Alcohol:string,
--                    WaiterService:boolean,
--                    AcceptsCreditCards:boolean,
--                    GoodForKids:boolean,
--                    GoodForGroups:boolean,
--                    PriceRange:int>
--  )
ROW FORMAT SERDE 'com.amazon.elasticmapreduce.JsonSerde' 
WITH SERDEPROPERTIES (
'paths' = 'business_id, city, hours.Sunday.close'
)
STORED AS TEXTFILE
LOCATION 's3://gu-anly502-yelp/restaurants';

select business_id, monday_open from restaurants limit 10;

—- omitted: latitude, longitude, categories, neighborhoods