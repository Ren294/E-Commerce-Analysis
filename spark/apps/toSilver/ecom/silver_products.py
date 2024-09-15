from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("Process Products").getOrCreate()

schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_category_name", StringType(), True),
    StructField("product_name_length", IntegerType(), True),
    StructField("description_length", IntegerType(), True),
    StructField("product_photos_qty", IntegerType(), True),
    StructField("product_weight_g", IntegerType(), True),
    StructField("product_length_cm", IntegerType(), True),
    StructField("product_height_cm", IntegerType(), True),
    StructField("product_width_cm", IntegerType(), True),
    StructField("id", IntegerType(), True)
])

df_products = spark.read.csv(
    "hdfs://namenode:9000/data/bronze/ecom/products/", schema=schema, header=False)

df_products_silver = df_products.drop("id")

df_products_silver = df_products_silver.dropDuplicates(["product_id"])

df_products_silver.show()

df_products_silver.write.mode("append").csv(
    "hdfs://namenode:9000/data/silver/ecom/products/", header=True)
