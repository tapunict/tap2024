from pyspark.sql import SparkSession
import sparknlp
from sparknlp.pretrained import PretrainedPipeline

spark = SparkSession.builder \
    .appName("Spark NLP")\
    .master("local[8]")\
    .config("spark.driver.memory","16G")\
    .config("spark.driver.maxResultSize", "0") \
    .config("spark.kryoserializer.buffer.max", "2000M")\
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:5.3.3")\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR") 

sparknlp.start()
print("Spark NLP version: ", sparknlp.version())
print("Apache Spark version: ", spark.version)

pipeline = PretrainedPipeline('entity_recognizer_lg', lang = 'it')

annotations =  pipeline.fullAnnotate("Ciao da John Snow Labs! ")[0]
print(annotations)
