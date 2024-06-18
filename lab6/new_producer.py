from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType

# Initialize Spark Session for Structured Streaming
spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName('calculate_reviews') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
    .getOrCreate()

# Define the schema for the streaming data
schema = StructType([
    StructField("application_name",  StringType(), True),
    StructField("translated_review", StringType(), True),
    StructField("sentiment_rank", LongType(), True),
    StructField("sentiment_polarity", FloatType(), True),
    StructField("sentiment_subjectivity", FloatType(), True)
])

try:
    # Read data from source in streaming mode
    streaming_df = spark.readStream \
        .schema(schema) \
        .parquet("s3a://lab5/googleplaystore_user_reviews_parquet/")

    # Convert data to JSON and write to Kafka
    streaming_df.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format('kafka') \
        .option("kafka.bootstrap.servers", "course-kafka:9092") \
        .option("topic", "gps-user-review-new") \
        .option('checkpointLocation', 's3a://lab6/checkpointLocation') \
        .outputMode('update') \
        .start()\
        .awaitTermination()
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # The Spark Session will be stopped when the stream is terminated
    spark.stop()
