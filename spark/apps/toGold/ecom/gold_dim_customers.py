from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Create Dim_Customers Table") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")\
    .config("spark.sql.catalogImplementation", "hive")\
    .getOrCreate()

base_path = "hdfs://namenode:9000/data/silver/ecom/"

df_customers = spark.read.csv(
    base_path + "customers", header=True, inferSchema=True)
df_geolocation = spark.read.csv(
    base_path + "geolocation", header=True, inferSchema=True)

df_dim_customers = df_customers \
    .join(df_geolocation, df_customers.customer_zip_code_prefix == df_geolocation.geolocation_zip_code_prefix, "left") \
    .select(
        col("customer_id"),
        col("customer_unique_id"),
        col("customer_zip_code_prefix"),
        col("customer_city"),
        col("customer_state"),
        col("geolocation_lat"),
        col("geolocation_lng")
    )

df_dim_customers.write\
    .mode("overwrite")\
    .format("parquet")\
    .saveAsTable("ecom.dim_customers")

spark.stop()
