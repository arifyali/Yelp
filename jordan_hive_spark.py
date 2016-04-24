from pyspark.sql import HiveContext

sc = SparkContext()
sqlContext = HiveContext(sc)
qry = "SELECT * FROM census_rest_success"
df = sqlContext.sql(qry).collect()
