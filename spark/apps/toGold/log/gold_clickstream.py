from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

spark = SparkSession.builder \
    .appName("Process Clickstream Log Data and Write to Hive") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .getOrCreate()

schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("log_level", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("session_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("outcome", StringType(), True)
])

clickstream_log_path = "hdfs://namenode:9000/data/silver/click"
df_clickstream = spark.read.csv(clickstream_log_path, schema=schema)

df_clickstream.write \
    .mode("append") \
    .format("parquet") \
    .saveAsTable("logs.Ecom_clickstream")

spark.stop()
