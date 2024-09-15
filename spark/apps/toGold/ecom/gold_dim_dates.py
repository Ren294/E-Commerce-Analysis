from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, dayofmonth, dayofweek, month, year, expr

spark = SparkSession.builder \
    .appName("Create Dim Dates Table") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .getOrCreate()


start_date = '2015-01-01'
end_date = '2018-12-31'

date_range = spark.sql(f"""
    SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS date_id
""")

df_dim_dates = date_range \
    .withColumn("year", year(col("date_id"))) \
    .withColumn("month", month(col("date_id"))) \
    .withColumn("day", dayofmonth(col("date_id"))) \
    .withColumn("day_of_week", date_format(col("date_id"), "EEEE")) \
    .withColumn("month_name", date_format(col("date_id"), "MMMM"))

df_dim_dates.write \
    .mode("overwrite") \
    .format("parquet") \
    .saveAsTable("ecom.dim_dates")

spark.stop()
