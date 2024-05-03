from pyspark.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder.master("local").appName('ex2_flights_raw').getOrCreate()

flights_raw_df = spark.read.csv('s3a://spark/lab2/source/task3/', header=True)

flights_raw_df = flights_raw_df.select(
            F.col('DayofMonth').cast(T.IntegerType()).alias('day_of_month'),
            F.col('DayOfWeek').cast(T.IntegerType()).alias('day_of_week'),
            F.col('Carrier').alias('carrier'),
            F.col('OriginAirportID').cast(T.IntegerType()).alias('origin_airport_id'),
            F.col('DestAirportID').cast(T.IntegerType()).alias('dest_airport_id'),
            F.col('ArrDelay').cast(T.IntegerType()).alias('arr_delay')
)

flights_raw_df.write.parquet('spark/lab2/target/task3/', mode='overwrite')
flights_raw_df.show()

spark.stop()
