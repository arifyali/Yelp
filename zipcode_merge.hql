CREATE TABLE IF NOT EXISTS zipcode_success AS
SELECT zipcode, 
       count(*) as num_restaurants, 
       avg(success_class) as success_avg,
       if(avg(success_class)>0.3, 1, 0) as success_class
FROM census_rest_success
GROUP BY zipcode;

CREATE TABLE IF NOT EXISTS zipcode_merge AS
SELECT *, 
       cd.white+cd.black+cd.native_american+cd.asian+cd.pacific_islander+cd.other_race+cd.multiple_race+cd.hispanic as population
FROM zipcode_success z JOIN census_data cd
ON (z.zipcode=cd.regionname);

SELECT corr(2015_01, success_class),
       corr(2016_01, success_class),
       corr(2016_02, success_class),
       corr(male_age_25_29, success_class),
       corr(female_age_25_29, success_class),
       corr(white, success_class),
       corr(black, success_class),
       corr(population, success_class),
       corr(median_household_income, success_class),
       corr(median_family_income, success_class),
       corr(total_housing_units, success_class),
       corr(occupied_housing_units, success_class),
       corr(owner_occupied_housing_units, success_class),
       corr(house_value_500g_750g, success_class),
       corr(house_value_750g_1m, success_class),
       corr(house_value_over_1m, success_class),
       corr(vacant_housing_units, success_class),
       corr(median_housing_value, success_class),
       corr(median_rent, success_class)
FROM zipcode_merge;
