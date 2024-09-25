from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Merge Dim Sellers Table") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0") \
    .getOrCreate()

base_path = "hdfs://namenode:9000/data/silver/ecom/"

df_sellers = spark.read.parquet(base_path + "sellers")
df_geolocation = spark.read.parquet(base_path + "geolocation")

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

table_name = "ecom.dim_sellers"

if spark.catalog._jcatalog.tableExists(table_name):
    deltaTable = DeltaTable.forName(spark, table_name)

    deltaTable.alias("target") \
        .merge(
            df_dim_sellers.alias("source"),
            "target.seller_id = source.seller_id"
    ) \
        .whenMatchedUpdate(set={
            "seller_zip_code_prefix": col("source.seller_zip_code_prefix"),
            "seller_city": col("source.seller_city"),
            "seller_state": col("source.seller_state"),
            "geolocation_lat": col("source.geolocation_lat"),
            "geolocation_lng": col("source.geolocation_lng")
        }) \
        .whenNotMatchedInsert(values={
            "seller_id": col("source.seller_id"),
            "seller_zip_code_prefix": col("source.seller_zip_code_prefix"),
            "seller_city": col("source.seller_city"),
            "seller_state": col("source.seller_state"),
            "geolocation_lat": col("source.geolocation_lat"),
            "geolocation_lng": col("source.geolocation_lng")
        }) \
        .execute()
else:
    df_dim_sellers.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_name)

spark.stop()
