from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType

spark = SparkSession.builder \
    .appName("Process Closed Deals Data") \
    .getOrCreate()

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
    StructField("id", IntegerType(), True)
])

df_bronze = spark.read.csv(
    "hdfs://namenode:9000/data/bronze/marketing/closed_deals",
    schema=schema,
    header=False
)

df_silver = df_bronze \
    .drop("id") \
    .dropna(subset=["mql_id", "seller_id", "won_date"]) \
    .withColumn("declared_product_catalog_size", df_bronze["declared_product_catalog_size"].cast(DoubleType())) \
    .withColumn("declared_monthly_revenue", df_bronze["declared_monthly_revenue"].cast(DoubleType()))

df_silver.write.mode("append").csv(
    "hdfs://namenode:9000/data/silver/marketing/closed_deals",
    header=True
)

spark.stop()
