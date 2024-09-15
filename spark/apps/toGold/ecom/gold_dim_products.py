from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Create Dim_Products Table") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")\
    .config("spark.sql.catalogImplementation", "hive")\
    .getOrCreate()

base_path = "hdfs://namenode:9000/data/silver/ecom/"

df_products = spark.read.csv(
    base_path + "products", header=True, inferSchema=True)
df_product_category = spark.read.csv(
    base_path + "product_category_name_translations", header=True, inferSchema=True)

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

df_dim_products.write\
    .mode("overwrite")\
    .format("parquet")\
    .saveAsTable("ecom.dim_products")


spark.stop()
