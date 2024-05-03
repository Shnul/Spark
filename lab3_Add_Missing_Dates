from pyspark.sql import SparkSession, functions as F, types as T, Row
from pyspark.sql.functions import max


spark = SparkSession.builder.master("local").appName('Add Missing Dates').config("spark.driver.memory", "4g").config("spark.executor.memory", "4g").getOrCreate()

def get_dates_df(flight_date):
    dummy_df = spark.createDataFrame([Row(dummy='x')])
    date_sequence = F.sequence(F.lit("2020-01-01").cast(T.DateType()), F.lit("2020-12-31").cast(T.DateType()))
    exploded_dates = F.explode(date_sequence)
    in_dates_df = dummy_df.select(exploded_dates.alias("flight_date"))
    return in_dates_df

intersected_df = spark.read.parquet('s3a://lab3/flight_matched/')

flight_date = get_dates_df(intersected_df)

flight_date = flight_date.withColumn('day_of_week', F.dayofweek(flight_date['flight_date']))
flight_date = flight_date.withColumn('day_of_month', F.dayofmonth(flight_date['flight_date']))

max_date_df = flight_date.groupBy('day_of_week', 'day_of_month').agg(max('flight_date').alias('Max Date'))
max_date_df = max_date_df.withColumnRenamed('day_of_week', 'max_day_of_week')
max_date_df = max_date_df.withColumnRenamed('day_of_month', 'max_day_of_month')
max_date_df.show()


flights_df = spark.read.parquet('s3a://lab3/flight_matched/')

enriched_flights_df = flights_df.join(max_date_df, (flights_df.day_of_week == max_date_df.max_day_of_week) & (flights_df.day_of_month == max_date_df.max_day_of_month))

enriched_flights_df.write.parquet('s3a://lab3/Enriched_Flights(transformed)/', mode='overwrite')
enriched_flights_df.show()

spark.stop()
