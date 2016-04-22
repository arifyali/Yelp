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

select * from restaurants limit 10;

--omitted: latitude, longitude, categories, neighborhoods