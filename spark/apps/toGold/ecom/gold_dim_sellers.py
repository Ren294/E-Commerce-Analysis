from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Create Dim_Sellers Table") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")\
    .config("spark.sql.catalogImplementation", "hive")\
    .getOrCreate()

base_path = "hdfs://namenode:9000/data/silver/ecom/"

df_sellers = spark.read.csv(base_path + "sellers",
                            header=True, inferSchema=True)
df_geolocation = spark.read.csv(
    base_path + "geolocation", header=True, inferSchema=True)

df_dim_sellers = df_sellers \
    .join(df_geolocation, df_sellers.seller_zip_code_prefix == df_geolocation.geolocation_zip_code_prefix, "left") \
    .select(
        col("seller_id"),
        col("seller_zip_code_prefix"),
        col("seller_city"),
        col("seller_state"),
        col("geolocation_lat"),
        col("geolocation_lng")
    )

df_dim_sellers.write\
    .mode("overwrite")\
    .format("parquet")\
    .saveAsTable("ecom.dim_sellers")


spark.stop()
