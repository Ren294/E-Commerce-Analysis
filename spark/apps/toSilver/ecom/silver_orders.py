from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from hdfs import InsecureClient

spark = SparkSession.builder.appName("Process Orders Data").getOrCreate()

silver_path = "/data/silver/ecom/orders"
bronze_path = "/data/bronze/ecom/orders"

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
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", TimestampType(), True),
    StructField("order_approved_at", TimestampType(), True),
    StructField("order_delivered_carrier_date", TimestampType(), True),
    StructField("order_delivered_customer_date", TimestampType(), True),
    StructField("order_estimated_delivery_date", TimestampType(), True),
    StructField("id", IntegerType(), True),
    StructField("load_timestamp", TimestampType(), True)
])

df_orders_bronze = spark.read.csv(
    f"hdfs://namenode:9000{bronze_path}", schema=schema, header=False)

df_new_data = df_orders_bronze.filter(col("load_timestamp") > max_timestamp)

df_orders_silver = df_new_data.drop("id").dropDuplicates()

df_orders_silver.write.mode("append").parquet(
    f"hdfs://namenode:9000{silver_path}")
