from pyspark.sql import SparkSession 
from pyspark.sql import functions as F 
from pyspark.sql import Window

spark = SparkSession.builder.master("local")\
        .appName('Lab4_Anomalies_Detection')\
        .config("spark.driver.memory", "4g")\
        .config("spark.executor.memory", "4g")\
        .getOrCreate()

sliding_range_window = Window.partitionBy(F.col('carrier')).orderBy(F.col('start_range'))

flights_df = spark.read.parquet('s3a://lab3/transformed/flights/')
flights_df = flights_df.cache()
if flights_df.is_cached:
    print("flights_df is cached")
else:
    print("flights_df is not cached")

flights_df \
    .groupBy(F.col('carrier'), F.window(F.col('flight_date'), '10 days', '1 day').alias('date_window')) \
    .agg(F.sum(F.col('arr_delay')).alias('total_delay')) \
    .select(F.col('carrier'),
            F.col('date_window.start').alias('start_range'),
            F.col('date_window.end').alias('end_range'),
            F.col('total_delay')) \
    .withColumn('last_window_delay', F.lag(F.col('total_delay')).over(sliding_range_window)) \
    .withColumn('change_percent', F.round(F.abs(F.lit(1.0) - (F.col('total_delay') / F.col('last_window_delay'))), 2)) \
    .where(F.col('change_percent') > F.lit(0.3)) \
    .show(100)

flights_df.unpersist()
if flights_df.is_cached:
    print("flights_df is still cached")
else:
    print("flights_df is not cached anymore")
    
spark.stop()
