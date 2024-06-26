from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql import types as T

spark = SparkSession.builder.master("local[*]").appName('ex5_google_reviews').getOrCreate()

sentiment_arr = [Row(Sentiment='Positive', sentiment_rank=1),
                 Row(Sentiment='Neutral', sentiment_rank=0),
                 Row(Sentiment='Negative', sentiment_rank=-1)]

google_reviews_df = spark.read.csv('s3a://lab5/googleplaystore_user_reviews/', header=True)
sentiments_df = spark.createDataFrame(sentiment_arr)

joined_df = google_reviews_df.join(F.broadcast(sentiments_df), ['Sentiment'])

selected_df = joined_df \
    .select(F.col('App').alias('application_name'),
            F.col('Translated_Review').alias('translated_review'),
            F.col('sentiment_rank'),
            F.col('Sentiment_Polarity').cast(T.FloatType()).alias('sentiment_polarity'),
            F.col('Sentiment_Subjectivity').cast(T.FloatType()).alias('sentiment_subjectivity'))

selected_df.write.parquet('s3a://lab5/task2/', mode='overwrite')
selected_df.show()

spark.stop()
