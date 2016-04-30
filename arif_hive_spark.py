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
(df.toPandas()).to_csv("yelp_dataset.csv")
## Lets train a Support Vector Classifier on this data
#CITATION:
#http://stackoverflow.com/questions/33900726/count-number-of-non-nan-entries-in-each-column-of-spark-dataframe-with-pyspark
def count_not_null(c):
    return sum(col(c).isNotNull().cast("integer")).alias(c)

exprs = [count_not_null(c) for c in df.columns]
df.agg(*exprs).show()

df = df.dropna()

#TODO: Create Validation set, then create randomized train - test sets, map processing on all

features = df.select(df['pricerange'], df['goodforkids'],
 df['goodforgroup'],
 df['goodfordessert'],
 df['goodforlatenight'],
 df['goodforlunch'],
 df['goodfordinner'],
 df['goodforbrunch'],
 df['goodforbreakfast'],
 df['romantic'],
 df['intimate'],
 df['classy'],
 df['hipster'],
 df['divey'],
 df['touristy'],
 df['trendy'],
 df['upscale'],
 df['casual'],df['2016_01'], df['2016_02'], df['male_age_25_29'],
          df['female_age_25_29'], df['white_percent'], df['black_percent'], df['asian_percent'],
          df['pi_percent'], df['other_race_percent'], df['multiple_percent'],
          df['hispanic_percent'], df['median_household_income'], df['median_family_income'],
          df['vacant_housing_units'], df['median_housing_value'], df['median_rent'],
          df['success_metric'], df['population'])

training, test = features.randomSplit([0.7, 0.3], seed=11L)

feats_train = training.collect()
train_dict = [i.asDict() for i in feats_train]

feats_test = test.collect()
test_dict = [i.asDict() for i in feats_test]

def parsePoint(d): ## wont be able to use line.split here?
    d_copy = deepcopy(d) # I hate using deepcopy so much
    pred = d_copy['success_metric']
    d.pop('success_metric', None)
    values = [float(x) for x in d.values()] ##this block is unusable until we have our Hive Data
    return (pred, Vectors.dense(values))

# training set
trainParsed = sc.parallelize(map(parsePoint, train_dict))
# test set 
testParsed = sc.parallelize(map(parsePoint, test_dict))


## create validation set

trainDf = sqlContext.createDataFrame(trainParsed, ["label", "features"])
testDf = sqlContext.createDataFrame(testParsed, ["label", "features"])
lm_model = LinearRegression(featuresCol="features", predictionCol="prediction", maxIter=100, regParam=0.0, elasticNetParam=0.0, tol=1e-6)
lm_model_fit = lm_model.fit(trainDf)
lm_transform = lm_model_fit.transform(trainDf)
results = lm_transform.select(lm_transform['prediction'], lm_transform['label'])
MSE = results.map(lambda(p,l):(p-l)**2).reduce(lambda x,y:x+y)/results.count()
print("Linear Regression training Mean Squared Error = " + str(MSE))

lm_transform = lm_model_fit.transform(testDf)
results = lm_transform.select(lm_transform['prediction'], lm_transform['label'])
MSE = results.map(lambda(p,l):(p-l)**2).reduce(lambda x,y:x+y)/results.count()
print("Linear Regression testing Mean Squared Error = " + str(MSE))


lm_model.save(sc, "LinerRegressionModel")

# LASSO

lasso_model = LinearRegression(featuresCol="features", predictionCol="prediction", maxIter=100, regParam=1.0, elasticNetParam=0.0, tol=1e-6)
lasso_model_fit = lasso_model.fit(trainDf)
lasso_transform = lasso_model_fit.transform(trainDf) #change to a test model
lasso_results = lasso_transform.select(lasso_transform['prediction'], lasso_transform['label'])
lasso_MSE = lasso_results.map(lambda(p,l):(p-l)**2).reduce(lambda x,y:x+y)/results.count()
print("LASSO training Mean Squared Error = " + str(lasso_MSE))

lasso_transform = lasso_model_fit.transform(testDf) #change to a test model
lasso_results = lasso_transform.select(lasso_transform['prediction'], lasso_transform['label'])
lasso_MSE = lasso_results.map(lambda(p,l):(p-l)**2).reduce(lambda x,y:x+y)/results.count()
print("LASSO testing Mean Squared Error = " + str(lasso_MSE))

model.save(sc, "LASSOModel")
