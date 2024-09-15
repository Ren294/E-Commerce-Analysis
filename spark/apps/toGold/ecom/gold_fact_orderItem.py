from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder \
    .appName("Add Foreign Key dim_dates to Fact_OrderItem") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .getOrCreate()

base_path = "hdfs://namenode:9000/data/silver/ecom/"

df_order_items = spark.read.csv(
    base_path + "order_items", header=True, inferSchema=True)
df_orders = spark.read.csv(base_path + "orders", header=True, inferSchema=True)
df_products = spark.read.csv(
    base_path + "products", header=True, inferSchema=True)
df_sellers = spark.read.csv(base_path + "sellers",
                            header=True, inferSchema=True)
df_order_payments = spark.read.csv(
    base_path + "order_payments", header=True, inferSchema=True)
df_order_reviews = spark.read.csv(
    base_path + "order_reviews", header=True, inferSchema=True)

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
        col("date_id")
    )

df_fact_order_item.write \
    .mode("append") \
    .format("parquet") \
    .saveAsTable("ecom.fact_order_item")


spark.stop()
