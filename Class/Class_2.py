from pyspark.sql import SparkSession
from pyspark.sql import types as T

spark = SparkSession.builder.appName('my_app').master('local').getOrCreate()
spark.sparkContext 

#### schema file

my_struct = T.StructType([T.StructField('f1',T.StringType(),True),
                            T.StructField('f2',T.IntegerType(),True),
                            T.StructField('f3',T.DataType(),True)])

#### text

df = spark.read.text('/data/my_data/')
df = spark.read.text('hdfs://my-hdfs:8020/data/my_data/')
df = spark.read.text('/data/my_data/*.txt')

#### CSV    
spark.read.csv('/path/to//csvs/*.csv')
spark.read.csv('/path/to//csvs/*.csv',header=True)       
spark.read.csv('/path/to//csvs/*.csv',schema=my_schema)
spark.read.csv('/path/to//csvs/*.csv', sep=':', lineSep = '&&')


#### JSON

spark.read.json('/path/to//jsons/*.json')
spark.read.json('/path/to//jsons/*.json',schema=my_schema)

#### AVRO

spark.read.format('avro').load('/path/to//avros/*.avro')
spark.read.format('avro').load('/path/to//avros/*.avro', schema=my_schema)


#### PARQUET

spark.read.parquet('/path/to//parquets/*.parquet')


######              Dataframe Operations

from pyspark.sql import functions as F
from pyspark.sql import types as T

df.select(F.col('col_a'),F.col('col_b').cast(T.StringType()).alias('new_col_b'))
                                                                    
                                                                    
                                                                    