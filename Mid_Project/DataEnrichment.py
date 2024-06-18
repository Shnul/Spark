from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType

car_models_schema =  StructType([
    StructField("model_id", IntegerType(), True),
    StructField("car_brand", StringType(), True),
    StructField("car_model", StringType(), True)
])

car_colors_schema = StructType([
    StructField("color_id", IntegerType(), True),
    StructField("color_name", StringType(), True)
])
 
cars_schema = StructType([
    StructField('car_id', IntegerType(), True),
    StructField('driver_id', LongType(), True),
    StructField('model_id', IntegerType(), True),
    StructField('color_id', IntegerType(), True)
])

main_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("car_id", IntegerType(), True),
    StructField("speed", IntegerType(), True),
    StructField("rpm", IntegerType(), True),
    StructField("gear", IntegerType(), True)
])

spark = SparkSession\
    .builder\
    .master("local[*]")\
    .appName('DataEnrichment')\
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
    .getOrCreate()   

car_colors_df = spark.read\
            .schema(car_colors_schema)\
            .parquet('s3a://mid.project/ex1_parquet/car_colors')


car_models_df = spark.read\
            .schema(car_models_schema)\
            .parquet('s3a://mid.project/ex1_parquet/car_models')

cars_df = spark.read\
            .schema(cars_schema)\
            .parquet('s3a://mid.project/ex1_parquet/CarsGenerator')

stream_df = spark\
    .readStream\
    .format('kafka')\
    .option("kafka.bootstrap.servers", "course-kafka:9092")\
    .option("subscribe", "sensors-sample")\
    .option('startingOffsets', 'latest')\
    .option('failOnDataLoss', 'false')\
    .load()\
    .select(F.col('value').cast(T.StringType()))

parsed_df = stream_df.select(F.from_json(F.col("value"), main_schema).alias("parsed_json"))
parsed_df = parsed_df.select(F.col("parsed_json.*"))

joined_df = parsed_df.join(F.broadcast(cars_df), parsed_df["car_id"] == cars_df["car_id"]) \
                     .join(F.broadcast(car_models_df), 'model_id') \
                     .join(F.broadcast(car_colors_df), 'color_id') \
                     .select(parsed_df['event_id'], parsed_df['event_time'], parsed_df['car_id'],'driver_id', 'car_brand', 'car_model',\
                        'color_name', parsed_df['speed'], parsed_df['rpm'], parsed_df['gear'])


enriched_df = joined_df.withColumn('expected_gear', F.ceil(F.col('speed')/ F.lit(30.0)).cast('int'))

kafka_query = enriched_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("topic", "samples-enriched") \
    .option("failOnDataLoss", "false") \
    .option("checkpointLocation", "s3a://mid.project/checkpoints") \
    .outputMode('append') \
    .start()

kafka_query.awaitTermination()

spark.stop()
