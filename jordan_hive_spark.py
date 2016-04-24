from pyspark.sql import HiveContext
from pyspark.mllib.classification import SVMWithSGD, SVMModel
from pyspark.mllib.regression import LabeledPoint

sc = SparkContext()
sqlContext = HiveContext(sc)
qry = "SELECT * FROM census_rest_success"
df = sqlContext.sql(qry)

## Lets train a Support Vector Classifier on this data


## TODO: 1, use df.select() to only select columns that can be converted to float.
def parsePoint(line): ## wont be able to use line.split here?
    values = [float(x) for x in line.split(' ')] ##this block is unusable until we have our Hive Data
    return LabeledPoint(values[0], values[1:])

parsedData = df.map(parsePoint)

## create training data from this 
##Create test data

## create validation set

model = SVMWithSGD.train(parsedData, iterations=100)

# Training error
labelsAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count()/float(parsedData.count())
print trainErr

model.save(sc, "SVMModel")

### Run Model on Validation Set
## TODO: output file of zipcodes and predicted success metrics
## TODO: Use bokeh on file to make visualization of the US




