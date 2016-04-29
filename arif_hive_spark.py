from pyspark.sql import HiveContext
from pyspark.mllib.regression import LabeledPoint, LinearModel, LinearRegressionWithSGD, LassoWithSGD 
from pyspark.ml.regression import LinearRegression
from pyspark.sql.functions import col, sum
from copy import deepcopy
from pyspark.mllib.linalg import Vectors

sc = SparkContext()
sqlContext = HiveContext(sc)
# The races from the census data were normalized in order 
qry = "SELECT *,white/population as white_percent,black/population as black_percent,asian/population as asian_percent,pacific_islander/population as pi_percent,other_race/population as other_race_percent,multiple_race/population as multiple_percent,hispanic/population as hispanic_percent FROM census_rest_success"
df = sqlContext.sql(qry)

## Lets train a Support Vector Classifier on this data
#CITATION:
#http://stackoverflow.com/questions/33900726/count-number-of-non-nan-entries-in-each-column-of-spark-dataframe-with-pyspark
def count_not_null(c):
    return sum(col(c).isNotNull().cast("integer")).alias(c)

exprs = [count_not_null(c) for c in df.columns]
df.agg(*exprs).show()

df = df.dropna()

#TODO: Create Validation set, then create randomized train - test sets, map processing on all

features = df.select(df['pricerange'], df['2016_01'], df['2016_02'], df['male_age_25_29'],
          df['female_age_25_29'], df['white_percent'], df['black_percent'], df['asian_percent'],
          df['pi_percent'], df['other_race_percent'], df['multiple_percent'],
          df['hispanic_percent'], df['median_household_income'], df['median_family_income'],
          df['vacant_housing_units'], df['median_housing_value'], df['median_rent'],
          df['success_metric'], df['population'])

feats_list = features.collect()
feats_dict = [i.asDict() for i in feats_list]


def parsePoint(d): ## wont be able to use line.split here?
    d_copy = deepcopy(d) # I hate using deepcopy so much
    pred = d_copy['success_metric']
    d.pop('success_metric', None)
    values = [float(x) for x in d.values()] ##this block is unusable until we have our Hive Data
    return (pred, Vectors.dense(values))

parsedData = sc.parallelize(map(parsePoint, feats_dict))

## create training data from this
##Create test data

## create validation set

#lm_model = LinearRegressionWithSGD.train(parsedData, iterations=5, intercept = True)

# Training error
#lm_valuesAndPreds = parsedData.map(lambda p: (p.label, lm_model.predict(p.features)))
#MSE = lm_valuesAndPreds.map(lambda (v, p): (v - p)**2).reduce(lambda x, y: x + y) / lm_valuesAndPreds.count()
#print("Linear Regression Mean Squared Error = " + str(MSE))

df = sqlContext.createDataFrame(parsedData, ["prediction", "features"])
lm_model = LinearRegression(featuresCol="features", predictionCol="prediction", maxIter=100, regParam=0.0, elasticNetParam=0.0, tol=1e-6)
lm_model_fit = lm_model.fit(features)
lm_model.save(sc, "LinerRegressionModel")

# LASSO
#lasso_model = LinearRegressionWithSGD.train(parsedData, iterations=5, intercept = True, regType = "l1")

# training error
#lasso_valuesAndPreds = parsedData.map(lambda p: (p.label, lasso_model.predict(p.features)))
#MSE = lasso_valuesAndPreds.map(lambda (v, p): (v - p)**2).reduce(lambda x, y: x + y) / lasso_valuesAndPreds.count()
#print("LASSO Mean Squared Error = " + str(MSE))

#TODO: do same as 54-56 with test and validation

model.save(sc, "SVMModel")

### Run Model on Validation Set
## TODO: output file of zipcodes and predicted success metrics
## TODO: Use bokeh on file to make visualization of the US
