from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Add Foreign Key dim_dates to Fact_Orders") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .getOrCreate()

base_path = "hdfs://namenode:9000/data/silver/ecom/"

df_orders = spark.read.csv(base_path + "orders", header=True, inferSchema=True)
df_order_items = spark.read.csv(
    base_path + "order_items", header=True, inferSchema=True)
df_order_payments = spark.read.csv(
    base_path + "order_payments", header=True, inferSchema=True)
df_order_reviews = spark.read.csv(
    base_path + "order_reviews", header=True, inferSchema=True)
df_customers = spark.read.csv(
    base_path + "customers", header=True, inferSchema=True)
df_sellers = spark.read.csv(base_path + "sellers",
                            header=True, inferSchema=True)
df_products = spark.read.csv(
    base_path + "products", header=True, inferSchema=True)

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
        to_date(col("order_purchase_timestamp")).alias(
            "fact_date_id")
    )

df_fact_orders = df_fact_orders \
    .join(df_dim_dates, df_fact_orders["fact_date_id"] == df_dim_dates["date_id"], "left") \
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
        col("fact_date_id").alias("date_id")
    )

df_fact_orders.write \
    .mode("append") \
    .format("parquet") \
    .saveAsTable("ecom.fact_orders")


spark.stop()
