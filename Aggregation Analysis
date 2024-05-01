from pyspark.sql import SparkSession, functions as F, types as T, Row
from pyspark.sql.functions import count, concat

spark = SparkSession.builder.master("local").appName('Add Missing Dates').config("spark.driver.memory", "4g").config("spark.executor.memory", "4g").getOrCreate()

enriched_Flights_df = spark.read.parquet('s3a://lab3/Enriched_Flights(transformed)/') 
airports_df = spark.read.parquet('s3a://lab2/parquet/airports/')

enriched_Flights_df.printSchema()
airports_df.printSchema()

departures_df = enriched_Flights_df.groupBy('origin_airport_id').agg(count('origin_airport_id').alias('number_of_departures'))
airports_df_renamed = airports_df.withColumnRenamed('airport_id', 'airport_id_airports')
departures_df = departures_df.join(airports_df_renamed, (airports_df_renamed.airport_id_airports == departures_df.origin_airport_id))
departures_df = departures_df.withColumnRenamed('origin_airport_id','airport_id')
departures_df = departures_df.orderBy(F.desc('number_of_departures')).limit(10)

arrivals_df = enriched_Flights_df.groupBy('dest_airport_id').agg(count('dest_airport_id').alias('number_of_arrivals'))
airports_df_renamed = airports_df.withColumnRenamed('airport_id', 'airport_id_airports')
arrivals_df = arrivals_df.join(airports_df_renamed, (airports_df_renamed.airport_id_airports == arrivals_df.dest_airport_id))
arrivals_df = arrivals_df.withColumnRenamed('dest_airport_id','airport_id')
arrivals_df = arrivals_df.orderBy(F.desc('number_of_arrivals')).limit(10)

routes_df = enriched_Flights_df.groupBy('origin_airport_id', 'dest_airport_id').agg(count('*').alias('route_count'))
routes_df = routes_df.withColumnRenamed('origin_airport_id', 'source_airport_id').withColumnRenamed('dest_airport_id', 'dest_airport_id')
airports_df_renamed_source = airports_df.withColumnRenamed('airport_id', 'source_airport_id_airports')
airports_df_renamed_dest = airports_df.withColumnRenamed('airport_id', 'dest_airport_id_airports')
routes_df = (routes_df
             .join(airports_df_renamed_source, routes_df.source_airport_id == airports_df_renamed_source.source_airport_id_airports)
             .withColumnRenamed('name', 'source_airport_name')
             .drop('source_airport_id_airports', 'city', 'state')
             .join(airports_df_renamed_dest, routes_df.dest_airport_id == airports_df_renamed_dest.dest_airport_id_airports)
             .withColumnRenamed('name', 'dest_airport_name')
             .drop('dest_airport_id_airports', 'city', 'state'))
routes_df = routes_df.withColumn('route', F.concat(F.col('source_airport_name'), F.lit(' -> '), F.col('dest_airport_name')))
routes_df = routes_df.orderBy(F.desc('route_count')).limit(10)

departures_df.write.parquet('s3a://lab3/Departures/', mode='overwrite')
arrivals_df.write.parquet('s3a://lab3/Arrivals/', mode='overwrite')
routes_df.write.parquet('s3a://lab3/Routes/', mode='overwrite')

departures_df.show()
arrivals_df.show()
routes_df.show()

spark.stop()
