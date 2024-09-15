from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

spark = SparkSession.builder.appName("Process Order Items Data").getOrCreate()

schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_item_id", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("shipping_limit_date", TimestampType(), True),
    StructField("price", DoubleType(), True),
    StructField("freight_value", DoubleType(), True),
    StructField("id", StringType(), True)
])

df_order_items_bronze = spark.read.csv(
    "hdfs://namenode:9000/data/bronze/ecom/order_items", schema=schema, header=False)

df_order_items_silver = df_order_items_bronze.drop("id")

df_order_items_silver = df_order_items_silver.dropDuplicates()

df_order_items_silver.show()

df_order_items_silver.write.mode("append").csv(
    "hdfs://namenode:9000/data/silver/ecom/order_items", header=True)
