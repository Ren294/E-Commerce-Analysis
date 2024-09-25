from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from hdfs import InsecureClient

spark = SparkSession.builder.appName("Process Sellers").getOrCreate()

silver_path = "/data/silver/ecom/sellers"
bronze_path = "/data/bronze/ecom/sellers"

client = InsecureClient('http://namenode:9870', user='hdfs')

if client.status(silver_path, strict=False) is None:
    max_timestamp = "1900-01-01 00:00:00"
else:
    df_existing_silver = spark.read.csv(
        f"hdfs://namenode:9000{silver_path}", header=True)
    max_timestamp = df_existing_silver.agg(
        max("load_timestamp")).collect()[0][0]
    if max_timestamp is None:
        max_timestamp = "1900-01-01 00:00:00"

schema = StructType([
    StructField("seller_id", StringType(), True),
    StructField("seller_zip_code_prefix", StringType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("load_timestamp", TimestampType(), True)
])

df_sellers = spark.read.csv(
    f"hdfs://namenode:9000{bronze_path}", schema=schema, header=False)

df_new_data = df_sellers.filter(col("load_timestamp") > max_timestamp)

df_sellers_silver = df_new_data.drop("id").dropDuplicates(["seller_id"])


df_sellers_silver.write.mode("append").parquet(
    f"hdfs://namenode:9000{silver_path}")
