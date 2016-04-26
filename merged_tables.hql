SET mapred.input.di.recursive=true;
SET hive.mapred.supports.subdirectories=true;
SET hive.groupby.orderby.position.alias=true;

ADD JAR /home/hadoop/Yelp/json-serde-1.3.7-jar.jar;

CREATE TABLE IF NOT EXISTS rest_rev_merge AS
SELECT tr.*, ra.days_open
FROM trunc_rest tr JOIN reviews_agg ra
ON (tr.business_id=ra.business_id);

CREATE TABLE IF NOT EXISTS census_rest_success AS
SELECT *, 
       if(open, rrm.stars*rrm.review_count/(rrm.days_open+1), 0) as success,
       cd.white+cd.black+cd.native_american+cd.asian+cd.pacific_islander+cd.other_race+cd.multiple_race+cd.hispanic as population
FROM rest_rev_merge rrm JOIN census_data cd
ON (rrm.zipcode=cd.regionid);


SELECT * from census_rest_success limit 2;
