from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession \
    .builder \
    .master("local")\
    .appName('models') \
    .getOrCreate()

data = [
    Row(model_id=1, car_brand='Mazda', car_model='3'),
    Row(model_id=2, car_brand='Mazda', car_model='6'),
    Row(model_id=3, car_brand='Toyota', car_model='Corolla'),
    Row(model_id=4, car_brand='Hyundai', car_model='i20'),
    Row(model_id=5, car_brand='Kia', car_model='Sportage'),
    Row(model_id=6, car_brand='Kia', car_model='Rio'),
    Row(model_id=7, car_brand='Kia', car_model='Picanto')
]

df_models = spark.createDataFrame(data)
df_models.show()
df_models.printSchema()

df_models.write.parquet('s3a://mid.project/ex1_parquet/car_models/', mode='overwrite')
spark.stop()


