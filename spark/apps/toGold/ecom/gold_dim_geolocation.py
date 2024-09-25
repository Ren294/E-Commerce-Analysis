from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("Merge Dim_Geolocation Table") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

base_path = "hdfs://namenode:9000/data/silver/ecom/"

df_geolocation = spark.read.parquet(base_path + "geolocation")

df_dim_geolocation = df_geolocation.select(
    "geolocation_zip_code_prefix",
    "geolocation_lat",
    "geolocation_lng",
    "geolocation_city",
    "geolocation_state"
)

table_name = "ecom.dim_geolocation"

if spark.catalog._jcatalog.tableExists(table_name):
    deltaTable = DeltaTable.forName(spark, table_name)

    deltaTable.alias("target") \
        .merge(
            df_dim_geolocation.alias("source"),
            "target.geolocation_zip_code_prefix = source.geolocation_zip_code_prefix"
    ) \
        .whenMatchedUpdate(set={
            "geolocation_lat": col("source.geolocation_lat"),
            "geolocation_lng": col("source.geolocation_lng"),
            "geolocation_city": col("source.geolocation_city"),
            "geolocation_state": col("source.geolocation_state")
        }) \
        .whenNotMatchedInsert(values={
            "geolocation_zip_code_prefix": col("source.geolocation_zip_code_prefix"),
            "geolocation_lat": col("source.geolocation_lat"),
            "geolocation_lng": col("source.geolocation_lng"),
            "geolocation_city": col("source.geolocation_city"),
            "geolocation_state": col("source.geolocation_state")
        }) \
        .execute()
else:
    df_dim_geolocation.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_name)

spark.stop()
