from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


builder = SparkSession.builder \
    .appName("Optimize Delta Table logs.Ecom_log") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

delta_table = DeltaTable.forName(spark, "logs.Ecom_log")

delta_table.optimize().executeCompaction()

delta_table.optimize().executeZOrderBy("timestamp")

spark.stop()
