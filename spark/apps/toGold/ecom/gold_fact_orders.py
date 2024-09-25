from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder \
    .appName("Merge Fact Orders Table") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0") \
    .getOrCreate()

base_path = "hdfs://namenode:9000/data/silver/ecom/"

df_orders = spark.read.parquet(base_path + "orders/")
df_order_items = spark.read.parquet(base_path + "order_items/")
df_order_payments = spark.read.parquet(base_path + "order_payments/")
df_order_reviews = spark.read.parquet(base_path + "order_reviews/")
df_customers = spark.read.parquet(base_path + "customers/")
df_sellers = spark.read.parquet(base_path + "sellers/")
df_products = spark.read.parquet(base_path + "products/")

df_dim_dates = spark.table("ecom.dim_dates")

df_fact_orders = df_orders \
    .join(df_order_items, "order_id", "left") \
    .join(df_order_payments, "order_id", "left") \
    .join(df_order_reviews, "order_id", "left") \
    .join(df_customers, "customer_id", "left") \
    .join(df_sellers, "seller_id", "left") \
    .join(df_products, "product_id", "left") \
    .select(
        col("order_id"),
        col("customer_id"),
        col("seller_id"),
        col("product_id"),
        col("order_status"),
        col("order_purchase_timestamp"),
        col("order_approved_at"),
        col("order_delivered_carrier_date"),
        col("order_delivered_customer_date"),
        col("order_estimated_delivery_date"),
        col("payment_type"),
        col("payment_installments"),
        col("payment_value"),
        col("review_score"),
        to_date(col("order_purchase_timestamp")).alias("fact_date_id")
    )

df_fact_orders = df_fact_orders \
    .join(df_dim_dates, df_fact_orders["fact_date_id"] == df_dim_dates["date_id"], "left")\
    .select(
        col("order_id"),
        col("customer_id"),
        col("seller_id"),
        col("product_id"),
        col("order_status"),
        col("order_purchase_timestamp"),
        col("order_approved_at"),
        col("order_delivered_carrier_date"),
        col("order_delivered_customer_date"),
        col("order_estimated_delivery_date"),
        col("payment_type"),
        col("payment_installments"),
        col("payment_value"),
        col("review_score"),
        col("date_id")
    )

table_name = "ecom.fact_orders"

if spark.catalog._jcatalog.tableExists(table_name):
    deltaTable = DeltaTable.forName(spark, table_name)

    deltaTable.alias("target") \
        .merge(
            df_fact_orders.alias("source"),
            "target.order_id = source.order_id AND target.customer_id = source.customer_id"
    ) \
        .whenMatchedUpdate(set={
            "seller_id": col("source.seller_id"),
            "product_id": col("source.product_id"),
            "order_status": col("source.order_status"),
            "order_purchase_timestamp": col("source.order_purchase_timestamp"),
            "order_approved_at": col("source.order_approved_at"),
            "order_delivered_carrier_date": col("source.order_delivered_carrier_date"),
            "order_delivered_customer_date": col("source.order_delivered_customer_date"),
            "order_estimated_delivery_date": col("source.order_estimated_delivery_date"),
            "payment_type": col("source.payment_type"),
            "payment_installments": col("source.payment_installments"),
            "payment_value": col("source.payment_value"),
            "review_score": col("source.review_score"),
            "date_id": col("source.date_id")
        }) \
        .whenNotMatchedInsert(values={
            "order_id": col("source.order_id"),
            "customer_id": col("source.customer_id"),
            "seller_id": col("source.seller_id"),
            "product_id": col("source.product_id"),
            "order_status": col("source.order_status"),
            "order_purchase_timestamp": col("source.order_purchase_timestamp"),
            "order_approved_at": col("source.order_approved_at"),
            "order_delivered_carrier_date": col("source.order_delivered_carrier_date"),
            "order_delivered_customer_date": col("source.order_delivered_customer_date"),
            "order_estimated_delivery_date": col("source.order_estimated_delivery_date"),
            "payment_type": col("source.payment_type"),
            "payment_installments": col("source.payment_installments"),
            "payment_value": col("source.payment_value"),
            "review_score": col("source.review_score"),
            "date_id": col("source.date_id")
        }) \
        .execute()
else:
    df_fact_orders.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_name)

spark.stop()
