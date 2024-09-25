from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder \
    .appName("Merge Fact Order Item Table") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0") \
    .getOrCreate()

base_path = "hdfs://namenode:9000/data/silver/ecom/"

df_order_items = spark.read.parquet(base_path + "order_items/")
df_orders = spark.read.parquet(base_path + "orders/")
df_products = spark.read.parquet(base_path + "products/")
df_sellers = spark.read.parquet(base_path + "sellers/")
df_order_payments = spark.read.parquet(base_path + "order_payments/")
df_order_reviews = spark.read.parquet(base_path + "order_reviews/")

df_dim_dates = spark.table("ecom.dim_dates").withColumnRenamed(
    "date_id", "dim_date_id")

df_fact_order_item = df_order_items \
    .join(df_orders, "order_id", "left") \
    .join(df_products, "product_id", "left") \
    .join(df_sellers, "seller_id", "left") \
    .join(df_order_payments, "order_id", "left") \
    .join(df_order_reviews, "order_id", "left") \
    .select(
        col("order_id"),
        col("order_item_id"),
        col("product_id"),
        col("product_category_name"),
        col("seller_id"),
        col("seller_city"),
        col("seller_state"),
        col("customer_id"),
        col("order_status"),
        col("order_purchase_timestamp"),
        col("shipping_limit_date"),
        col("price"),
        col("freight_value"),
        col("payment_type"),
        col("payment_installments"),
        col("payment_value"),
        col("review_score"),
        col("review_comment_message"),
        to_date(col("order_purchase_timestamp")).alias("date_id")
    )

df_fact_order_item = df_fact_order_item \
    .join(df_dim_dates, df_fact_order_item["date_id"] == df_dim_dates["dim_date_id"], "left") \
    .select(
        col("order_id"),
        col("order_item_id"),
        col("product_id"),
        col("product_category_name"),
        col("seller_id"),
        col("seller_city"),
        col("seller_state"),
        col("customer_id"),
        col("order_status"),
        col("order_purchase_timestamp"),
        col("shipping_limit_date"),
        col("price"),
        col("freight_value"),
        col("payment_type"),
        col("payment_installments"),
        col("payment_value"),
        col("review_score"),
        col("review_comment_message"),
        col("dim_date_id").alias("date_id")
    )

table_name = "ecom.fact_order_item"

if spark.catalog._jcatalog.tableExists(table_name):
    deltaTable = DeltaTable.forName(spark, table_name)

    deltaTable.alias("target") \
        .merge(
            df_fact_order_item.alias("source"),
            "target.order_item_id = source.order_item_id AND target.order_id = source.order_id"
    ) \
        .whenMatchedUpdate(set={
            "product_id": col("source.product_id"),
            "product_category_name": col("source.product_category_name"),
            "seller_id": col("source.seller_id"),
            "seller_city": col("source.seller_city"),
            "seller_state": col("source.seller_state"),
            "customer_id": col("source.customer_id"),
            "order_status": col("source.order_status"),
            "order_purchase_timestamp": col("source.order_purchase_timestamp"),
            "shipping_limit_date": col("source.shipping_limit_date"),
            "price": col("source.price"),
            "freight_value": col("source.freight_value"),
            "payment_type": col("source.payment_type"),
            "payment_installments": col("source.payment_installments"),
            "payment_value": col("source.payment_value"),
            "review_score": col("source.review_score"),
            "review_comment_message": col("source.review_comment_message"),
            "date_id": col("source.date_id")
        }) \
        .whenNotMatchedInsert(values={
            "order_id": col("source.order_id"),
            "order_item_id": col("source.order_item_id"),
            "product_id": col("source.product_id"),
            "product_category_name": col("source.product_category_name"),
            "seller_id": col("source.seller_id"),
            "seller_city": col("source.seller_city"),
            "seller_state": col("source.seller_state"),
            "customer_id": col("source.customer_id"),
            "order_status": col("source.order_status"),
            "order_purchase_timestamp": col("source.order_purchase_timestamp"),
            "shipping_limit_date": col("source.shipping_limit_date"),
            "price": col("source.price"),
            "freight_value": col("source.freight_value"),
            "payment_type": col("source.payment_type"),
            "payment_installments": col("source.payment_installments"),
            "payment_value": col("source.payment_value"),
            "review_score": col("source.review_score"),
            "review_comment_message": col("source.review_comment_message"),
            "date_id": col("source.date_id")
        }) \
        .execute()
else:
    df_fact_order_item.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_name)

spark.stop()
