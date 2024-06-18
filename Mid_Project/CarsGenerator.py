from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from pyspark.sql.functions import expr, rand

schema = StructType([
    StructField("car_id", IntegerType(), True),
    StructField("driver_id", LongType(), True),
    StructField("model_id", IntegerType(), True),
    StructField("color_id", IntegerType(), True)
])

spark = SparkSession \
    .builder \
    .master("local")\
    .appName('CarsGenerator') \
    .getOrCreate()

df = spark.range(100000) 

cars_df = df.withColumn("car_id", (rand() * (9999999 - 1000000) + 1000000).cast(IntegerType())) \
            .withColumn("driver_id", (rand() * (999999999 - 100000000) + 100000000).cast(LongType())) \
            .withColumn("model_id", (rand() * (7 - 1) + 1).cast(IntegerType())) \
            .withColumn("color_id", (rand() * (7 - 1) + 1).cast(IntegerType()))

cars_df = cars_df.dropDuplicates(['car_id']).limit(20)

cars_df.write.parquet('s3a://mid.project/ex1_parquet/CarsGenerator/', mode='overwrite')
cars_df = cars_df.drop('id')
cars_df.show()

spark.stop()
