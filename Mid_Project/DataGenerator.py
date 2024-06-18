from time import sleep
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType, TimestampType
import random
import uuid

schema = StructType([
    StructField('car_id', IntegerType(), True),
    StructField('driver_id', LongType(), True),
    StructField('model_id', IntegerType(), True),
    StructField('color_id', IntegerType(), True)
])

spark = SparkSession\
    .builder\
    .master("local[*]")\
    .appName('DataGenerator')\
    .config("spark.driver.memory", "4g")\
    .config("spark.executor.memory", "4g")\
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
    .getOrCreate()   

car_data = spark.read.schema(schema) \
                .parquet('s3a://mid.project/ex1_parquet/CarsGenerator')
car_data.cache()

iterations = 200

while iterations:
    car_data = car_data.select(F.col('car_id'))\
        .withColumn('event_id' , F.lit(str(uuid.uuid4())))\
        .withColumn('event_time' , F.current_timestamp())\
        .withColumn('speed' , F.round(F.rand() * 200).cast("int"))\
        .withColumn('rpm' , F.round(F.rand() * 8000).cast("int"))\
        .withColumn('gear' ,  1 + F.round(F.rand() * 6).cast("int"))

   # car_data.show(truncate=False)

    car_data_json = car_data.select(F.to_json(F.struct([car_data[x] for x in car_data.columns])).alias("value"))
   
    car_data_json.write \
             .format("kafka") \
             .option("kafka.bootstrap.servers", "course-kafka:9092") \
             .option("topic", 'sensors-sample') \
             .option("checkpointLocation", "s3a://mid.project/checkpoints") \
             .option('failOnDataLoss', 'false')\
             .save()
    
    sleep(1)
    iterations -= 1 
    
print("C'est tout!!!")
