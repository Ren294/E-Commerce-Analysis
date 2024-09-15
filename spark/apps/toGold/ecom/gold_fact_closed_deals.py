from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, expr

spark = SparkSession.builder \
    .appName("Create Fact Closed Deals Table with Foreign Keys") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .getOrCreate()

base_path = "hdfs://namenode:9000/data/silver/marketing/"

df_closed_deals = spark.read.csv(
    base_path + "closed_deals/*.csv", header=True, inferSchema=True)

df_dim_mql = spark.read.csv(
    "hdfs://namenode:9000/data/silver/marketing/marketing_qualified_leads/*.csv", header=True, inferSchema=True) \
    .withColumnRenamed("mql_id", "dim_mql_id")

df_dim_dates = spark.table("ecom.dim_dates").withColumnRenamed(
    "date_id", "dim_date_id")

df_dim_orders = spark.read.csv(
    "hdfs://namenode:9000/data/silver/ecom/orders/*.csv", header=True, inferSchema=True) \
    .withColumnRenamed("order_id", "dim_order_id")

df_dim_customers = spark.read.csv(
    "hdfs://namenode:9000/data/silver/ecom/customers/*.csv", header=True, inferSchema=True) \
    .withColumnRenamed("customer_id", "dim_customer_id")

df_dim_sellers = spark.read.csv(
    "hdfs://namenode:9000/data/silver/ecom/sellers/*.csv", header=True, inferSchema=True) \
    .withColumnRenamed("seller_id", "dim_seller_id")

df_dim_products = spark.read.csv(
    "hdfs://namenode:9000/data/silver/ecom/products/*.csv", header=True, inferSchema=True) \
    .withColumnRenamed("product_id", "dim_product_id")

df_closed_deals = df_closed_deals \
    .withColumn("won_date", to_timestamp(col("won_date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))

df_fact_closed_deals = df_closed_deals \
    .join(df_dim_mql, df_closed_deals["mql_id"] == df_dim_mql["dim_mql_id"], "left") \
    .join(df_dim_dates, df_closed_deals["won_date"].cast("date") == df_dim_dates["dim_date_id"], "left") \
    .join(df_dim_orders.alias("do"), df_closed_deals["sdr_id"] == col("do.dim_order_id"), "left") \
    .join(df_dim_customers.alias("dc"), df_closed_deals["sdr_id"] == col("dc.dim_customer_id"), "left") \
    .join(df_dim_sellers.alias("ds"), df_closed_deals["seller_id"] == col("ds.dim_seller_id"), "left") \
    .join(df_dim_products.alias("dp"), expr("true"), "left") \
    .select(
        col("mql_id"),
        col("ds.dim_seller_id").alias("seller_id"),
        col("sdr_id"),
        col("sr_id"),
        col("won_date"),
        col("business_segment"),
        col("lead_type"),
        col("lead_behaviour_profile"),
        col("has_company"),
        col("has_gtin"),
        col("average_stock"),
        col("business_type"),
        col("declared_product_catalog_size"),
        col("declared_monthly_revenue"),
        col("dim_date_id").alias("date_id"),
        col("do.dim_order_id").alias("order_id"),
        col("dc.dim_customer_id").alias("customer_id"),
        col("dp.dim_product_id").alias("product_id")
    )

df_fact_closed_deals.write \
    .mode("append") \
    .format("parquet") \
    .saveAsTable("ecom.fact_closed_deals")


spark.stop()
