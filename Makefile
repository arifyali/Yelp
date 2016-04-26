all: restaurants reviews census_data merged

restaurants: trunc_restaurants.hql
	beeline -u jdbc:hive2://localhost:10000 -n hadoop -p hadoop -f $<

reviews: reviews.hql
	beeline -u jdbc:hive2://localhost:10000 -n hadoop -p hadoop -f $<

census_data: census_data.hql
	beeline -u jdbc:hive2://localhost:10000 -n hadoop -p hadoop -f $<

merged: merged_tables.hql 
	beeline -u jdbc:hive2://localhost:10000 -n hadoop -p hadoop -f $<

clean: drop_tables.hql
	beeline -u jdbc:hive2://localhost:10000 -n hadoop -p hadoop -f $<
