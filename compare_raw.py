from pyspark.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder.master("local").appName('compare_raw').getOrCreate()

flights_df = spark.read.parquet('s3a://lab2/target/parquet_flights/')
flights_raw_df = spark.read.parquet('s3a://lab2/target/parquet_flights_raw/')

flights_df = flights_df.dropDuplicates()
flights_raw_df = flights_raw_df.dropDuplicates()

intersected_df = flights_df.intersect(flights_raw_df)

difference_df = flights_df.subtract(flights_raw_df)

intersected_df.write.parquet('s3a://lab3/flight_matched/', mode='overwrite')
difference_df.write.parquet('s3a://lab3/flight_unmatched/', mode='overwrite')

difference_df.show()
intersected_df.show()

spark.stop()