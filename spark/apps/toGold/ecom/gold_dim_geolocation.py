from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Create Dim_Geolocation Table") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")\
    .config("spark.sql.catalogImplementation", "hive")\
    .getOrCreate()

base_path = "hdfs://namenode:9000/data/silver/ecom/"

df_geolocation = spark.read.csv(
    base_path + "geolocation", header=True, inferSchema=True)

df_dim_geolocation = df_geolocation.select(
    "geolocation_zip_code_prefix",
    "geolocation_lat",
    "geolocation_lng",
    "geolocation_city",
    "geolocation_state"
)

df_dim_geolocation.write\
    .mode("overwrite")\
    .format("parquet")\
    .saveAsTable("ecom.dim_geolocation")

spark.stop()
