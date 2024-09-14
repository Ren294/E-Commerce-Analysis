from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder.appName(
    "Process Order Payments Data").getOrCreate()

schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_sequential", IntegerType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DoubleType(), True),
    StructField("id", IntegerType(), True)
])

df_order_payments_bronze = spark.read.csv(
    "hdfs://namenode:9000/data/bronze/ecom/order_payments", schema=schema, header=False)

df_order_payments_silver = df_order_payments_bronze.drop("id")

df_order_payments_silver = df_order_payments_silver.dropDuplicates()

df_order_payments_silver.show()

df_order_payments_silver.write.mode("append").csv(
    "hdfs://namenode:9000/data/silver/ecom/order_payments", header=True)
