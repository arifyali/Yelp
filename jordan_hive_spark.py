from pyspark.sql import HiveContext
from pyspark.mllib.classification import SVMWithSGD, SVMModel
from pyspark.mllib.regression import LabeledPoint

from copy import deepcopy

sc = SparkContext()
sqlContext = HiveContext(sc)
qry = "SELECT * FROM census_rest_success"
df = sqlContext.sql(qry)

## Lets train a Support Vector Classifier on this data
#CITATION:
#http://stackoverflow.com/questions/33900726/count-number-of-non-nan-entries-in-each-column-of-spark-dataframe-with-pyspark
def count_not_null(c):
    return sum(col(c).isNotNull().cast("integer")).alias(c)

exprs = [count_not_null(c) for c in df.columns]
df.agg(*exprs).show()

#TODO: Clean nulls (with mean?)
#TODO: Create class variable for success metric (70/30 split) - must find 70th %tile
#TODO: Create Validation set, then create randomized train - test sets, map processing on all


features = df.select(df['pricerange'], df['2016_01'], df['2016_02'], df['male_age_25_29'],
          df['female_age_25_29'], df['white'], df['black'], df['asian'],
          df['pacific_islander'], df['other_race'], df['multiple_race'],
          df['hispanic'], df['median_household_income'], df['median_family_income'],
          df['vacant_housing_units'], df['median_housing_value'], df['median_rent'],
          df['success'], df['population'])

feats_list = features.collect()
feats_dict = [i.asDict() for i in feats_list]


def parsePoint(d): ## wont be able to use line.split here?
    d_copy = deepcopy(d) # I hate using deepcopy so much
    pred = d_copy['success']
    d.pop('success', None)
    values = [float(x) for x in d.values()] ##this block is unusable until we have our Hive Data
    return LabeledPoint(pred, values)

parsedData = map(parsePoint, feats_dict)

## create training data from this
##Create test data

## create validation set

model = SVMWithSGD.train(parsedData, iterations=100)

# Training error
labelsAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count()/float(parsedData.count())
print trainErr

#TODO: do same as 54-56 with test and validation

model.save(sc, "SVMModel")

### Run Model on Validation Set
## TODO: output file of zipcodes and predicted success metrics
## TODO: Use bokeh on file to make visualization of the US
