from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, TimestampType
from pyspark.sql.functions import from_json, window, when, count, max

alert_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("car_id", IntegerType(), True),
    StructField('driver_id', IntegerType(), True),
    StructField("car_brand", StringType(), True),
    StructField("car_model", StringType(), True),
    StructField("color_name", StringType(), True),
    StructField("speed", IntegerType(), True),
    StructField("rpm", IntegerType(), True),
    StructField("gear", IntegerType(), True),
    StructField('expected_gear', IntegerType(), True)
])

spark = SparkSession\
    .builder\
    .master("local[*]")\
    .appName('AlertCounter')\
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
    .getOrCreate()   

stream_df = spark\
    .readStream\
    .format('kafka')\
    .option("kafka.bootstrap.servers", "course-kafka:9092")\
    .option("subscribe", "alert-data")\
    .option('startingOffsets', 'latest')\
    .load()\
    .select(F.col('value').cast(T.StringType()))

parsed_df = stream_df.select(from_json(F.col('value'), alert_schema).alias('data')).select('data.*')

windowed_df = parsed_df \
    .withWatermark("event_time", "15 minutes") \
    .groupBy(window(parsed_df.event_time, "15 minutes")) \
    .agg(
        count("*").alias("num_of_rows"),
        count(when(parsed_df.color_name == "Black", 1)).alias("num_of_black"),
        count(when(parsed_df.color_name == "White", 1)).alias("num_of_white"),
        count(when(parsed_df.color_name == "Silver", 1)).alias("num_of_silver"),
        max("speed").alias("maximum_speed"),
        max("gear").alias("maximum_gear"),
        max("rpm").alias("maximum_rpm")
    )

query = windowed_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
