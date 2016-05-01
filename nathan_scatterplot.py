from pyspark.sql import HiveContext
from pyspark.mllib.regression import LabeledPoint, LinearModel, LinearRegressionWithSGD, LassoWithSGD
from pyspark.ml.regression import LinearRegression
from pyspark.sql.functions import col, sum
from copy import deepcopy
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.evaluation import RegressionMetrics
import matplotlib.pyplot as plt

#sc = SparkContext()
sqlContext = HiveContext(sc)
# The races from the census data were normalized in order
qry = "SELECT AVG(success_metric) as success_metric, 2016_02, zipcode from census_rest_success group by zipcode, 2016_02"

df = sqlContext.sql(qry)

x = df.select("2016_02").collect()
y = df.select("success_metric").collect()

plt.scatter(x, y)

plt.show()

plt.savefig("plot.png", format='png')
