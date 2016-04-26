CREATE EXTERNAL TABLE IF NOT EXISTS census_data (
  RegionID STRING,
  RegionName STRING,
  City STRING,
  State STRING,
  Metro STRING,
  CountyName STRING,
  2015_01 DECIMAL,
  2016_01 DECIMAL,
  2016_02 DECIMAL,
  Male_age_25_29 DECIMAL,
  Female_age_25_29 DECIMAL,
  white DECIMAL,
  black DECIMAL,
  native_american DECIMAL,
  asian DECIMAL,
  pacific_islander DECIMAL,
  other_race DECIMAL,
  multiple_race DECIMAL,
  hispanic DECIMAL,
  median_household_income DECIMAL,
  median_family_income DECIMAL,
  total_housing_units DECIMAL,
  occupied_housing_units DECIMAL,
  owner_occupied_housing_units DECIMAL,
  house_value_500g_750g DECIMAL,
  house_value_750g_1m DECIMAL,
  house_value_over_1m DECIMAL,
  vacant_housing_units DECIMAL,
  median_housing_value DECIMAL,
  median_rent DECIMAL
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION 's3://gu-anly502-yelp/census_table/'
tblproperties("skip.header.line.count"="1");

