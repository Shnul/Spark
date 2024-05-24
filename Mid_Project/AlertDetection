from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, TimestampType


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
    .appName('AlertDetection')\
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
    .getOrCreate()   

stream_df = spark\
    .readStream\
    .format('kafka')\
    .option("kafka.bootstrap.servers", "course-kafka:9092")\
    .option("subscribe", "samples-enriched")\
    .option('startingOffsets', 'latest')\
    .load()\
    .select(F.col('value').cast(T.StringType()))

parsed_df = stream_df.withColumn('parsed_json', F.from_json(F.col('value'), alert_schema))\
                    .select(F.col('parsed_json.*'))


filterd_df = parsed_df.filter((F.col('speed') > 120) & 
                              (F.col('gear') != F.col('expected_gear')) & 
                              (F.col('rpm') > 6000))



kafka_query = filterd_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("topic", "alert-data") \
    .option("failOnDataLoss", "false") \
    .option("checkpointLocation", "s3a://mid.project/checkpoints2") \
    .outputMode('append') \
    .start()

kafka_query.awaitTermination()

spark.stop()
