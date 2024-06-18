from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time


schema = T.StructType([
    T.StructField('application_name', T.StringType(), True),
    T.StructField('num_of_positive_sentiments', T.LongType(), True),
    T.StructField('num_of_neutral_sentiments', T.LongType(), True),
    T.StructField('num_of_negative_sentiments', T.LongType(), True),
    T.StructField('avg_sentiment_polarity', T.DoubleType(), True),
    T.StructField('avg_sentiment_subjectivity', T.DoubleType(), True),
    T.StructField('category', T.StringType(), True),
    T.StructField('rating', T.StringType(), True),
    T.StructField('reviews', T.FloatType(), True),
    T.StructField('size', T.StringType(), True),
    T.StructField('num_of_installs', T.DoubleType(), True),
    T.StructField('price', T.DoubleType(), True),
    T.StructField('age_limit', T.LongType(), True),
    T.StructField('genres', T.StringType(), True),
    T.StructField('version', T.StringType(), True)
])


spark = SparkSession\
    .builder\
    .master("local[*]")\
    .appName('store_results')\
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2')\
    .getOrCreate()


stream_df = spark\
    .readStream\
    .format('kafka')\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("topic", "gps-with-reviews-new")\
    .load()\
    .select(F.col('value').cast(T.StringType()))


