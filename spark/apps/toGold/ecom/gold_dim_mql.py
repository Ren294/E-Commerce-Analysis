from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("Merge Dim MQL Table") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

base_path = "hdfs://namenode:9000/data/silver/marketing/"

df_closed_deals = spark.read.parquet(base_path + "closed_deals")
df_marketing_qualified_leads = spark.read.parquet(
    base_path + "marketing_qualified_leads")

df_dim_mql = df_marketing_qualified_leads \
    .join(df_closed_deals, "mql_id", "left") \
    .select(
        col("mql_id"),
        col("first_contact_date"),
        col("landing_page_id"),
        col("origin")
    )

table_name = "ecom.dim_mql"

if spark.catalog._jcatalog.tableExists(table_name):
    deltaTable = DeltaTable.forName(spark, table_name)

    deltaTable.alias("target") \
        .merge(
            df_dim_mql.alias("source"),
            "target.mql_id = source.mql_id"
    ) \
        .whenMatchedUpdate(set={
            "first_contact_date": col("source.first_contact_date"),
            "landing_page_id": col("source.landing_page_id"),
            "origin": col("source.origin")
        }) \
        .whenNotMatchedInsert(values={
            "mql_id": col("source.mql_id"),
            "first_contact_date": col("source.first_contact_date"),
            "landing_page_id": col("source.landing_page_id"),
            "origin": col("source.origin")
        }) \
        .execute()
else:
    df_dim_mql.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_name)

spark.stop()
