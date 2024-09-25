from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from hdfs import InsecureClient

spark = SparkSession.builder.appName("Process Geolocation Data").getOrCreate()

schema = StructType([
    StructField("geolocation_zip_code_prefix", StringType(), True),
    StructField("geolocation_lat", DoubleType(), True),
    StructField("geolocation_lng", DoubleType(), True),
    StructField("geolocation_city", StringType(), True),
    StructField("geolocation_state", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("load_timestamp", TimestampType(), True)
])

silver_path = "/data/silver/ecom/geolocation"
bronze_path = "/data/bronze/ecom/geolocation"

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

df_geolocation_bronze = spark.read.csv(
    f"hdfs://namenode:9000{bronze_path}", schema=schema, header=False)

df_new_data = df_geolocation_bronze.filter(
    col("load_timestamp") > max_timestamp)

df_geolocation_silver = df_new_data.drop("id").dropDuplicates(
    ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"])

df_geolocation_silver.write.mode("append").parquet(
    f"hdfs://namenode:9000{silver_path}")
