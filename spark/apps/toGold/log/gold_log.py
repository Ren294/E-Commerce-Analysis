from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import unix_timestamp, col

spark = SparkSession.builder \
    .appName("Process Log Data and Write to Hive") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .getOrCreate()

schema = StructType([
    StructField("ip_address", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("request_path", StringType(), True),
    StructField("status_code", IntegerType(), True),
    StructField("bytes_sent", IntegerType(), True)
])

log_path = "hdfs://namenode:9000/data/silver/log"
df_logs = spark.read.csv(log_path, schema=schema)

df_logs = df_logs.withColumn("timestamp", unix_timestamp(
    col("timestamp"), "dd/MMM/yyyy:HH:mm:ss Z").cast("timestamp"))

df_logs.write \
    .mode("append") \
    .format("parquet") \
    .saveAsTable("logs.Ecom_logs")


spark.stop()
