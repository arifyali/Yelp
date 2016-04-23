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
  zipcode STRING,
  RegionName STRING,
  City STRING,
  State STRING,
  Metro STRING,
  CountyName STRING,
  2015_01 DECIMAL,
  2016_01 DECIMAL,
  2016_02 DECIMAL,
  B01001_011E DECIMAL,
  B01001_035E DECIMAL,
  B02001_002E DECIMAL,
  B02001_003E DECIMAL,
  B02001_004E DECIMAL,
  B02001_005E DECIMAL,
  B02001_006E DECIMAL,
  B02001_007E DECIMAL,
  B02001_008E DECIMAL,
  B03001_003E DECIMAL,
  B19013_001E DECIMAL,
  B19113_001E DECIMAL,
  B25001_001E DECIMAL,
  B25002_002E DECIMAL,
  B25003_002E DECIMAL,
  B25075_023E DECIMAL,
  B25075_024E DECIMAL,
  B25075_025E DECIMAL,
  B25002_003E DECIMAL,
  B25077_001E DECIMAL,
  B25064_001E DECIMAL
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION 's3://gu-anly502-yelp/census_table/'
tblproperties("skip.header.line.count"="1");


SELECT count(*), open
FROM restaurants
JOIN census_data
ON restaurants.zipcode = census_data.zipcode
GROUP BY restaurants.open;
