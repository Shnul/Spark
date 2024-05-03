from pyspark.sql import SparkSession 
from pyspark.sql import functions as F 
from pyspark.sql import Window
from pyspark.sql.types import StringType

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

all_history_window = Window.partitionBy(F.col('carrier')).orderBy(F.col('flight_date')).rowsBetween(Window.unboundedPreceding, 0)

avg_flights_df = flights_df\
                .withColumn('avg_till_now', F.round(F.avg(flights_df['arr_delay']).over(all_history_window), 2))

per_flights_df = avg_flights_df\
                .withColumn('avg_diff_percent', F.round((F.col('arr_delay')/F.col('avg_till_now') - 1), 2))

per_flights_df = per_flights_df.filter("avg_diff_percent > 3")


per_flights_df.write.parquet('s3a://lab4/Delay_Anomalies/', mode='overwrite')
per_flights_df.show()


flights_df.unpersist()

if flights_df.is_cached:
    print("flights_df is still cached")
else:
    print("flights_df is not cached anymore")
    
spark.stop()
