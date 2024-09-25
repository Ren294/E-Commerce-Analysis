from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder \
    .appName("Merge Dim Orders Table") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0") \
    .getOrCreate()

base_path = "hdfs://namenode:9000/data/silver/ecom/"

df_orders = spark.read.parquet(base_path + "orders")
df_order_items = spark.read.parquet(base_path + "order_items")

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

table_name = "ecom.dim_orders"

if spark.catalog._jcatalog.tableExists(table_name):
    deltaTable = DeltaTable.forName(spark, table_name)

    deltaTable.alias("target") \
        .merge(
            df_dim_orders.alias("source"),
            "target.order_id = source.order_id"
    ) \
        .whenMatchedUpdate(set={
            "order_date": col("source.order_date"),
            "customer_id": col("source.customer_id"),
            "seller_id": col("source.seller_id"),
            "product_id": col("source.product_id"),
            "quantity": col("source.quantity"),
            "price": col("source.price"),
            "total_amount": col("source.total_amount")
        }) \
        .whenNotMatchedInsert(values={
            "order_id": col("source.order_id"),
            "order_date": col("source.order_date"),
            "customer_id": col("source.customer_id"),
            "seller_id": col("source.seller_id"),
            "product_id": col("source.product_id"),
            "quantity": col("source.quantity"),
            "price": col("source.price"),
            "total_amount": col("source.total_amount")
        }) \
        .execute()
else:
    df_dim_orders.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_name)

spark.stop()
