from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, expr

spark = SparkSession.builder \
    .appName("Merge Fact Closed Deals Table") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0") \
    .getOrCreate()

base_path = "hdfs://namenode:9000/data/silver/marketing/"

df_closed_deals = spark.read.parquet(base_path + "closed_deals/")
df_dim_mql = spark.read.parquet("hdfs://namenode:9000/data/silver/marketing/marketing_qualified_leads/") \
    .withColumnRenamed("mql_id", "dim_mql_id")

df_dim_dates = spark.table("ecom.dim_dates").withColumnRenamed(
    "date_id", "dim_date_id")

df_dim_orders = spark.read.parquet("hdfs://namenode:9000/data/silver/ecom/orders/") \
    .withColumnRenamed("order_id", "dim_order_id")

df_dim_customers = spark.read.parquet("hdfs://namenode:9000/data/silver/ecom/customers/") \
    .withColumnRenamed("customer_id", "dim_customer_id")

df_dim_sellers = spark.read.parquet("hdfs://namenode:9000/data/silver/ecom/sellers/") \
    .withColumnRenamed("seller_id", "dim_seller_id")

df_dim_products = spark.read.parquet("hdfs://namenode:9000/data/silver/ecom/products/") \
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

table_name = "ecom.fact_closed_deals"

if spark.catalog._jcatalog.tableExists(table_name):
    deltaTable = DeltaTable.forName(spark, table_name)

    deltaTable.alias("target") \
        .merge(
            df_fact_closed_deals.alias("source"),
            "target.mql_id = source.mql_id"
    ) \
        .whenMatchedUpdate(set={
            "seller_id": col("source.seller_id"),
            "sdr_id": col("source.sdr_id"),
            "sr_id": col("source.sr_id"),
            "won_date": col("source.won_date"),
            "business_segment": col("source.business_segment"),
            "lead_type": col("source.lead_type"),
            "lead_behaviour_profile": col("source.lead_behaviour_profile"),
            "has_company": col("source.has_company"),
            "has_gtin": col("source.has_gtin"),
            "average_stock": col("source.average_stock"),
            "business_type": col("source.business_type"),
            "declared_product_catalog_size": col("source.declared_product_catalog_size"),
            "declared_monthly_revenue": col("source.declared_monthly_revenue"),
            "date_id": col("source.date_id"),
            "order_id": col("source.order_id"),
            "customer_id": col("source.customer_id"),
            "product_id": col("source.product_id")
        }) \
        .whenNotMatchedInsert(values={
            "mql_id": col("source.mql_id"),
            "seller_id": col("source.seller_id"),
            "sdr_id": col("source.sdr_id"),
            "sr_id": col("source.sr_id"),
            "won_date": col("source.won_date"),
            "business_segment": col("source.business_segment"),
            "lead_type": col("source.lead_type"),
            "lead_behaviour_profile": col("source.lead_behaviour_profile"),
            "has_company": col("source.has_company"),
            "has_gtin": col("source.has_gtin"),
            "average_stock": col("source.average_stock"),
            "business_type": col("source.business_type"),
            "declared_product_catalog_size": col("source.declared_product_catalog_size"),
            "declared_monthly_revenue": col("source.declared_monthly_revenue"),
            "date_id": col("source.date_id"),
            "order_id": col("source.order_id"),
            "customer_id": col("source.customer_id"),
            "product_id": col("source.product_id")
        }) \
        .execute()
else:
    df_fact_closed_deals.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_name)

spark.stop()
