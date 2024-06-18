from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession \
    .builder \
    .master("local")\
    .appName('colors') \
    .getOrCreate()


data = [
    Row(color_id=1, color_name='Black'),
    Row(color_id=2, color_name='Red'),
    Row(color_id=3, color_name='Gray'),
    Row(color_id=4, color_name='White'),
    Row(color_id=5, color_name='Green'),
    Row(color_id=6, color_name='Blue'),
    Row(color_id=7, color_name='Pink')
]

df_colors = spark.createDataFrame(data)
df_colors.show()
df_colors.printSchema()


df_colors.write.parquet('s3a://mid.project/ex1_parquet/car_colors/', mode='overwrite')


spark.stop()
