from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Merge Dim Products Table") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0") \
    .getOrCreate()

base_path = "hdfs://namenode:9000/data/silver/ecom/"

df_products = spark.read.parquet(base_path + "products")
df_product_category = spark.read.parquet(
    base_path + "product_category_name_translations")

df_products_alias = df_products.alias("p")
df_product_category_alias = df_product_category.alias("c")

df_dim_products = df_products_alias \
    .join(df_product_category_alias, df_products_alias.product_category_name == df_product_category_alias.product_category_name, "left") \
    .select(
        col("p.product_id"),
        col("p.product_category_name"),
        col("c.product_category_name_english"),
        col("p.product_name_length"),
        col("p.description_length"),
        col("p.product_photos_qty"),
        col("p.product_weight_g"),
        col("p.product_length_cm"),
        col("p.product_height_cm"),
        col("p.product_width_cm")
    )

table_name = "ecom.dim_products"

if spark.catalog._jcatalog.tableExists(table_name):
    deltaTable = DeltaTable.forName(spark, table_name)

    deltaTable.alias("target") \
        .merge(
            df_dim_products.alias("source"),
            "target.product_id = source.product_id"
    ) \
        .whenMatchedUpdate(set={
            "product_category_name": col("source.product_category_name"),
            "product_category_name_english": col("source.product_category_name_english"),
            "product_name_length": col("source.product_name_length"),
            "description_length": col("source.description_length"),
            "product_photos_qty": col("source.product_photos_qty"),
            "product_weight_g": col("source.product_weight_g"),
            "product_length_cm": col("source.product_length_cm"),
            "product_height_cm": col("source.product_height_cm"),
            "product_width_cm": col("source.product_width_cm")
        }) \
        .whenNotMatchedInsert(values={
            "product_id": col("source.product_id"),
            "product_category_name": col("source.product_category_name"),
            "product_category_name_english": col("source.product_category_name_english"),
            "product_name_length": col("source.product_name_length"),
            "description_length": col("source.description_length"),
            "product_photos_qty": col("source.product_photos_qty"),
            "product_weight_g": col("source.product_weight_g"),
            "product_length_cm": col("source.product_length_cm"),
            "product_height_cm": col("source.product_height_cm"),
            "product_width_cm": col("source.product_width_cm")
        }) \
        .execute()
else:
    df_dim_products.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_name)

spark.stop()
