from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

spark = SparkSession.builder.appName(
    "Process Order Reviews Data").getOrCreate()

schema = StructType([
    StructField("review_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("review_score", IntegerType(), True),
    StructField("review_comment_title", StringType(), True),
    StructField("review_comment_message", StringType(), True),
    StructField("review_creation_date", TimestampType(), True),
    StructField("review_answer_timestamp", TimestampType(), True),
    StructField("id", IntegerType(), True)
])

df_order_review_bronze = spark.read.csv(
    "hdfs://namenode:9000/data/bronze/ecom/order_reviews", schema=schema, header=False)

df_order_review_silver = df_order_review_bronze.drop("id")

df_order_review_silver = df_order_review_silver.dropDuplicates()

df_order_review_silver.show()

df_order_review_silver.write.mode("append").csv(
    "hdfs://namenode:9000/data/silver/ecom/order_reviews", header=True)
