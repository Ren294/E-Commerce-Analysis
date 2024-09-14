from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder.appName("Process Geolocation Data").getOrCreate()

schema = StructType([
    StructField("geolocation_zip_code_prefix", StringType(), True),
    StructField("geolocation_lat", DoubleType(), True),
    StructField("geolocation_lng", DoubleType(), True),
    StructField("geolocation_city", StringType(), True),
    StructField("geolocation_state", StringType(), True),
    StructField("id", IntegerType(), True)
])

df_geolocation_bronze = spark.read.csv(
    "hdfs://namenode:9000/data/bronze/ecom/geolocation", schema=schema, header=False)

df_geolocation_silver = df_geolocation_bronze.drop("id")

df_geolocation_silver = df_geolocation_silver.dropDuplicates(
    ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"])

df_geolocation_silver.show()

df_geolocation_silver.write.mode("append").csv(
    "hdfs://namenode:9000/data/silver/ecom/geolocation", header=True)
