from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("Process Ecom Data").getOrCreate()

schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_unique_id", StringType(), True),
    StructField("customer_zip_code_prefix", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True),
    StructField("id", IntegerType(), True)
])

df_bronze = spark.read.csv(
    "hdfs://namenode:9000/data/bronze/ecom/customers", schema=schema, header=False)

df_silver = df_bronze.drop("id")

df_silver = df_silver.withColumn(
    "customer_zip_code_prefix", df_silver["customer_zip_code_prefix"].cast("int"))

df_silver = df_silver.dropDuplicates(["customer_unique_id"])

df_silver.write.mode("append").csv(
    "hdfs://namenode:9000/data/silver/ecom/customers", header=True)
