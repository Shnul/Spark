from pyspark.sql import SparkSession 
from pyspark.sql import functions as F 
from pyspark.sql import Window

spark = SparkSession.builder.master("local")\
        .appName('Lab4_Anomalies_Detection')\
        .config("spark.driver.memory", "4g")\
        .config("spark.executor.memory", "4g")\
        .getOrCreate()


flights_df = spark.read.parquet('s3a://lab3/transformed/flights/')
flights_df = flights_df.cache()
if flights_df.is_cached:
    print("flights_df is cached")
else:
    print("flights_df is not cached")

window = Window.partitionBy('carrier').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)


flights_df = flights_df.fillna({'arr_delay': 0})

avg_flights_df = flights_df\
                .withColumn('avg_all_time', F.round(F.avg(flights_df['arr_delay']).over(window), 2))


epsilon = 1e-7
per_flights_df = avg_flights_df\
                .withColumn('avg_diff_percent', F.round((F.col('arr_delay')/(F.col('avg_all_time') + epsilon) - 1), 2))


per_flights_df = per_flights_df.filter("avg_diff_percent > 5")



per_flights_df.write.parquet('s3a://lab4/Delay_Anomalies_02/', mode='overwrite')
per_flights_df.show()



flights_df.unpersist()
if flights_df.is_cached:
    print("flights_df is still cached")
else:
    print("flights_df is not cached anymore")
    
spark.stop()
