from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, expr

spark = SparkSession.builder \
    .appName("Create Dim Orders Table") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .getOrCreate()

base_path = "hdfs://namenode:9000/data/silver/ecom/"

df_orders = spark.read.csv(base_path + "orders", header=True, inferSchema=True)
df_order_items = spark.read.csv(
    base_path + "order_items", header=True, inferSchema=True)

df_dim_orders = df_orders \
    .join(df_order_items, "order_id", "left") \
    .select(
        col("order_id"),
        to_date(col("order_purchase_timestamp")).alias("order_date"),
        col("customer_id"),
        col("seller_id"),
        col("product_id"),
        col("order_item_id").cast("int").alias("quantity"),
        col("price")
    ) \
    .withColumn("total_amount", col("price") * col("quantity"))

df_dim_orders.write \
    .mode("overwrite") \
    .format("parquet") \
    .saveAsTable("ecom.dim_orders")


spark.stop()
