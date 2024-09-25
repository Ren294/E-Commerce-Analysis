from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from hdfs import InsecureClient

spark = SparkSession.builder.appName(
    "Process Order Payments Data").getOrCreate()

silver_path = "/data/silver/ecom/order_payments"
bronze_path = "/data/bronze/ecom/order_payments"

client = InsecureClient('http://namenode:9870', user='hdfs')

if client.status(silver_path, strict=False) is None:
    max_timestamp = "1900-01-01 00:00:00"
else:
    df_existing_silver = spark.read.parquet(
        f"hdfs://namenode:9000{silver_path}")
    max_timestamp = df_existing_silver.agg(
        max("load_timestamp")).collect()[0][0]
    if max_timestamp is None:
        max_timestamp = "1900-01-01 00:00:00"

schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_sequential", IntegerType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DoubleType(), True),
    StructField("id", IntegerType(), True),
    StructField("load_timestamp", TimestampType(), True)
])

df_bronze = spark.read.csv(
    f"hdfs://namenode:9000{bronze_path}", schema=schema, header=False)

df_new_data = df_bronze.filter(col("load_timestamp") > max_timestamp)

df_silver = df_new_data.drop("id").dropDuplicates()


df_silver.write.mode("append").parquet(f"hdfs://namenode:9000{silver_path}")
