from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Merge Dim ProductCategory Table") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0") \
    .getOrCreate()

base_path = "hdfs://namenode:9000/data/silver/ecom/"

df_product_category = spark.read.parquet(
    base_path + "product_category_name_translations")

df_dim_product_category = df_product_category.select(
    col("product_category_name"),
    col("product_category_name_english")
)

table_name = "ecom.dim_product_category"

if spark.catalog._jcatalog.tableExists(table_name):
    deltaTable = DeltaTable.forName(spark, table_name)

    deltaTable.alias("target") \
        .merge(
            df_dim_product_category.alias("source"),
            "target.product_category_name = source.product_category_name"
    ) \
        .whenMatchedUpdate(set={
            "product_category_name_english": col("source.product_category_name_english")
        }) \
        .whenNotMatchedInsert(values={
            "product_category_name": col("source.product_category_name"),
            "product_category_name_english": col("source.product_category_name_english")
        }) \
        .execute()
else:
    df_dim_product_category.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_name)

spark.stop()
