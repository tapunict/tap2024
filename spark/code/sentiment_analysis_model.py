from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.classification import LogisticRegression

spark = SparkSession.builder.appName("SentimentAnalysis").getOrCreate()
# To reduce verbose output
spark.sparkContext.setLogLevel("ERROR") 

print("Reading training set...")
# read the dataset  
schema="target short, id long, ts string, flag string, user string, text string"
# Todo use proper timestamp
dataset = (
    spark.read.format("csv")
    .schema(schema)
    .load(
        "/tmp/dataset/training.1600000.processed.noemoticon.csv.gz"
    )
)
print("Done.")

tokenizer = Tokenizer(inputCol="text", outputCol="words")
hashtf = HashingTF(numFeatures=2**16, inputCol="words", outputCol='tf')
idf = IDF(inputCol='tf', outputCol="features", minDocFreq=5) #minDocFreq: remove sparse terms
TFIDFmodel = LogisticRegression(featuresCol= 'features', labelCol= 'target',maxIter=100)
TFIDFPipeline = Pipeline(stages=[tokenizer, hashtf, idf, TFIDFmodel])

print("Training model...")
# fit the pipeline model with the training data
TFIDFModel=TFIDFPipeline.fit(dataset)
print("Done.")

TFIDFModelSummary=TFIDFModel.stages[-1].summary
print("Accuracy")
print(TFIDFModelSummary.accuracy)

print("Saving model...")
TFIDFModel.write().overwrite().save("/opt/tap/models/sentitap")
print("Done.")

spark.stop()