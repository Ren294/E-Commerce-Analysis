from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Create Dim MQL Table") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .getOrCreate()

base_path = "hdfs://namenode:9000/data/silver/marketing/"

df_closed_deals = spark.read.csv(
    base_path + "closed_deals", header=True, inferSchema=True)
df_marketing_qualified_leads = spark.read.csv(
    base_path + "marketing_qualified_leads", header=True, inferSchema=True)

df_dim_mql = df_marketing_qualified_leads \
    .join(df_closed_deals, "mql_id", "left") \
    .select(
        col("mql_id"),
        col("first_contact_date"),
        col("landing_page_id"),
        col("origin")
    )

df_dim_mql.write \
    .mode("overwrite") \
    .format("parquet") \
    .saveAsTable("ecom.dim_mql")


spark.stop()
