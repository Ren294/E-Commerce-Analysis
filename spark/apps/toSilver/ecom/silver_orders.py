from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

spark = SparkSession.builder.appName("Process Orders Data").getOrCreate()

schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", TimestampType(), True),
    StructField("order_approved_at", TimestampType(), True),
    StructField("order_delivered_carrier_date", TimestampType(), True),
    StructField("order_delivered_customer_date", TimestampType(), True),
    StructField("order_estimated_delivery_date", TimestampType(), True),
    StructField("id", IntegerType(), True)
])

df_orders_bronze = spark.read.csv(
    "hdfs://namenode:9000/data/bronze/ecom/orders/", schema=schema, header=False)

df_orders_silver = df_orders_bronze.drop("id")

df_orders_silver = df_orders_silver.dropDuplicates()

df_orders_silver.show()

df_orders_silver.write.mode("append").csv(
    "hdfs://namenode:9000/data/silver/ecom/orders/", header=True)
