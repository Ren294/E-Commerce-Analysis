from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import unix_timestamp, col
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("Process Log Data and Write to Hive") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.extensions", "delta.sql.DeltaSparkSessionExtensions") \
    .getOrCreate()

schema = StructType([
    StructField("ip_address", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("request_path", StringType(), True),
    StructField("status_code", IntegerType(), True),
    StructField("bytes_sent", IntegerType(), True)
])

log_path = "hdfs://namenode:9000/data/silver/log"

df_logs = spark.read.parquet(log_path)

df_logs = df_logs.withColumn("timestamp", unix_timestamp(
    col("timestamp"), "dd/MMM/yyyy:HH:mm:ss Z").cast("timestamp"))

if not DeltaTable.isDeltaTable(spark, "logs.Ecom_log"):
    df_logs.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("logs.Ecom_log")
else:
    df_logs.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("logs.Ecom_log")

spark.stop()
