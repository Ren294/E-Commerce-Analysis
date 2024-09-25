from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, TimestampType
from hdfs import InsecureClient

spark = SparkSession.builder \
    .appName("Process Marketing Qualified Leads Data") \
    .getOrCreate()

client = InsecureClient('http://namenode:9870', user='hdfs')
silver_path = "/data/silver/marketing/marketing_qualified_leads"
bronze_path = "/data/bronze/marketing/marketing_qualified_leads"

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
    StructField("first_contact_date", DateType(), True),
    StructField("landing_page_id", StringType(), True),
    StructField("origin", StringType(), True),
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
    .dropna(subset=["mql_id", "first_contact_date"]) \
    .withColumn("first_contact_date", col("first_contact_date").cast(DateType()))

df_silver.write.mode("append").parquet(f"hdfs://namenode:9000{silver_path}")

spark.stop()
