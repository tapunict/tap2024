from pyspark.sql import SparkSession
import sparknlp
from sparknlp.pretrained import PretrainedPipeline

spark = SparkSession.builder \
    .appName("Spark NLP")\
    .master("local[16]")\
    .config("spark.driver.memory","64G")\
    .config("spark.driver.maxResultSize", "0") \
    .config("spark.kryoserializer.buffer.max", "2000M")\
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:5.3.3")\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR") 

sparknlp.start()
print("Spark NLP version: ", sparknlp.version())
print("Apache Spark version: ", spark.version)

pipeline = PretrainedPipeline('entity_recognizer_lg', lang = 'it')

annotations =  pipeline.fullAnnotate("Salvatore Nicotra prova a spiegare SPARK agli studenti di TAP presso l'università di Catania")[0]
print(annotations)
