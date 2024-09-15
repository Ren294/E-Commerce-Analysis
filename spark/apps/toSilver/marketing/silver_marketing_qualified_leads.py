from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

spark = SparkSession.builder \
    .appName("Process Marketing Qualified Leads Data") \
    .getOrCreate()

schema = StructType([
    StructField("mql_id", StringType(), True),
    StructField("first_contact_date", DateType(), True),
    StructField("landing_page_id", StringType(), True),
    StructField("origin", StringType(), True),
    StructField("id", IntegerType(), True)
])

df_bronze = spark.read.csv(
    "hdfs://namenode:9000/data/bronze/marketing/marketing_qualified_leads",
    schema=schema,
    header=False
)

df_silver = df_bronze \
    .drop("id") \
    .dropna(subset=["mql_id", "first_contact_date"]) \
    .withColumn("first_contact_date", df_bronze["first_contact_date"].cast(DateType()))

df_silver.write.mode("append").csv(
    "hdfs://namenode:9000/data/silver/marketing/marketing_qualified_leads",
    header=True
)

spark.stop()
