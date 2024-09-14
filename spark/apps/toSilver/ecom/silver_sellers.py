from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("Process Sellers").getOrCreate()

schema = StructType([
    StructField("seller_id", StringType(), True),
    StructField("seller_zip_code_prefix", StringType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True),
    StructField("id", IntegerType(), True)
])

df_sellers = spark.read.csv(
    "hdfs://namenode:9000/data/bronze/ecom/sellers/", schema=schema, header=False)

df_sellers_silver = df_sellers.drop("id")

df_sellers_silver = df_sellers_silver.dropDuplicates(["seller_id"])

df_sellers_silver.show()

df_sellers_silver.write.mode("append").csv(
    "hdfs://namenode:9000/data/silver/ecom/sellers", header=True)
