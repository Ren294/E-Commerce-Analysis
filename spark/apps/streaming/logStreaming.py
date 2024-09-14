from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("LogStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
    .getOrCreate()

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9094") \
    .option("subscribe", "login") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

df = df.selectExpr("CAST(value AS STRING)")

df_parsed = df.select(
    regexp_extract('value', r'(\d{1,3}\.){3}\d{1,3}', 0).alias('ip'),
    regexp_extract('value', r'\[(.*?)\]', 1).alias('timestamp'),
    regexp_extract(
        'value', r'\"(?:GET|POST|HEAD|PUT|DELETE|CONNECT|OPTIONS|TRACE|PATCH)?\s?(.*?)\s?HTTP/\d.\d\"?', 1).alias('endpoint'),
    regexp_extract('value', r'\"\s?(\d{3})\s?', 1).cast(
        'integer').alias('status'),
    regexp_extract('value', r'\s(\d+|-)\s*$', 1).alias('bytes')
)

df_parsed = df_parsed.withColumn("bytes", when(
    col("bytes") == "-", 0).otherwise(col("bytes").cast('integer')))

df_parsed = df_parsed.select(
    regexp_replace(col('ip'), "'", "").alias('ip'),
    regexp_replace(col('timestamp'), "'", "").alias('timestamp'),
    regexp_replace(col('endpoint'), "'", "").alias('endpoint'),
    regexp_replace(col('status').cast('string'), "'", "").alias('status'),
    regexp_replace(col('bytes').cast('string'), "'", "").alias('bytes')
)

df_csv = df_parsed.selectExpr(
    "concat_ws(',', ip, timestamp, endpoint, status, bytes) as value"
)


def write_to_kafka(batch_df, batch_id):
    batch_df \
        .selectExpr("CAST(value AS STRING)") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9094") \
        .option("topic", "logout") \
        .save()


query = df_csv.writeStream \
    .foreachBatch(write_to_kafka) \
    .option("checkpointLocation", "file:///opt/spark-data/checkpoint-log") \
    .start()

query.awaitTermination()
