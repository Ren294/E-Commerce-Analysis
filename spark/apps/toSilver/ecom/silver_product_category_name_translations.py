from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName(
    "Process Product Category Name Translations").getOrCreate()

schema = StructType([
    StructField("product_category_name", StringType(), True),
    StructField("product_category_name_english", StringType(), True),
    StructField("id", IntegerType(), True)
])

df_product_category = spark.read.csv(
    "hdfs://namenode:9000/data/bronze/ecom/product_category_name_translations", schema=schema, header=False)

df_product_category_silver = df_product_category.drop("id")

df_product_category_silver = df_product_category_silver.dropDuplicates()

df_product_category_silver.show()

df_product_category_silver.write.mode("append").csv(
    "hdfs://namenode:9000/data/silver/ecom/product_category_name_translations/", header=True)
