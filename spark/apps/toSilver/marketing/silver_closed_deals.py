from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from hdfs import InsecureClient

spark = SparkSession.builder \
    .appName("Process Closed Deals Data") \
    .getOrCreate()

client = InsecureClient('http://namenode:9870', user='hdfs')
silver_path = "/data/silver/marketing/closed_deals"
bronze_path = "/data/bronze/marketing/closed_deals"

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
    StructField("mql_id", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("sdr_id", StringType(), True),
    StructField("sr_id", StringType(), True),
    StructField("won_date", TimestampType(), True),
    StructField("business_segment", StringType(), True),
    StructField("lead_type", StringType(), True),
    StructField("lead_behaviour_profile", StringType(), True),
    StructField("has_company", StringType(), True),
    StructField("has_gtin", StringType(), True),
    StructField("average_stock", StringType(), True),
    StructField("business_type", StringType(), True),
    StructField("declared_product_catalog_size", DoubleType(), True),
    StructField("declared_monthly_revenue", DoubleType(), True),
    StructField("id", IntegerType(), True),
    StructField("load_timestamp", TimestampType(), True)
])

df_bronze = spark.read.csv(
    f"hdfs://namenode:9000{bronze_path}",
    schema=schema,
    header=False
)

df_new_data = df_bronze.filter(col("load_timestamp") > max_timestamp)

df_silver = df_new_data \
    .drop("id") \
    .dropna(subset=["mql_id", "seller_id", "won_date"]) \
    .withColumn("declared_product_catalog_size", col("declared_product_catalog_size").cast(DoubleType())) \
    .withColumn("declared_monthly_revenue", col("declared_monthly_revenue").cast(DoubleType()))

df_silver.write.mode("append").parquet(f"hdfs://namenode:9000{silver_path}")

spark.stop()
