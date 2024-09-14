from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Create Dim_ProductCategory Table") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")\
    .config("spark.sql.catalogImplementation", "hive")\
    .getOrCreate()

base_path = "hdfs://namenode:9000/data/silver/ecom/"

df_product_category = spark.read.csv(
    base_path + "product_category_name_translations", header=True, inferSchema=True)

df_dim_product_category = df_product_category.select(
    "product_category_name",
    "product_category_name_english"
)

df_dim_product_category.write\
    .mode("overwrite")\
    .format("parquet")\
    .saveAsTable("ecom.dim_product_category")


spark.stop()
