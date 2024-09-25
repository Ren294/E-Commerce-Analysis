from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("Merge Dim_Customers Table") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

base_path = "hdfs://namenode:9000/data/silver/ecom/"

df_customers = spark.read.parquet(base_path + "customers")
df_geolocation = spark.read.parquet(base_path + "geolocation")

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

table_name = "ecom.dim_customers"

if spark.catalog._jcatalog.tableExists(table_name):
    deltaTable = DeltaTable.forName(spark, table_name)

    deltaTable.alias("target") \
        .merge(
            df_dim_customers.alias("source"),
            "target.customer_id = source.customer_id"
    ) \
        .whenMatchedUpdate(set={
            "customer_unique_id": col("source.customer_unique_id"),
            "customer_zip_code_prefix": col("source.customer_zip_code_prefix"),
            "customer_city": col("source.customer_city"),
            "customer_state": col("source.customer_state"),
            "geolocation_lat": col("source.geolocation_lat"),
            "geolocation_lng": col("source.geolocation_lng")
        }) \
        .whenNotMatchedInsert(values={
            "customer_id": col("source.customer_id"),
            "customer_unique_id": col("source.customer_unique_id"),
            "customer_zip_code_prefix": col("source.customer_zip_code_prefix"),
            "customer_city": col("source.customer_city"),
            "customer_state": col("source.customer_state"),
            "geolocation_lat": col("source.geolocation_lat"),
            "geolocation_lng": col("source.geolocation_lng")
        }) \
        .execute()
else:
    df_dim_customers.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_name)

spark.stop()
