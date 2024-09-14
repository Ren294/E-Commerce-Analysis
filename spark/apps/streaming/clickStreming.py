from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("LogStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
    .getOrCreate()

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9094") \
    .option("subscribe", "clickin") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

df = df.selectExpr("CAST(value AS STRING)")

df_parsed = df.select(
    regexp_extract(
        'value', r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)', 1).alias('timestamp'),
    regexp_extract('value', r'\[(INFO|WARN|ERROR)\]', 1).alias('log_level'),
    regexp_extract('value', r'USER_ID: (\d+)', 1).alias('user_id'),
    regexp_extract('value', r'SESSION_ID: (\d+)', 1).alias('session_id'),
    regexp_extract('value', r'EVENT_TYPE: (\w+)', 1).alias('event_type'),
    regexp_extract(
        'value', r'PRODUCT_ID: ([\w-]+|N/A)', 1).alias('product_id'),
    regexp_extract('value', r'AMOUNT: ([\d.]+|N/A)', 1).alias('amount'),
    regexp_extract('value', r'OUTCOME: ([\w]+|N/A)', 1).alias('outcome')
)


def handle_na(col_name):
    return when(col(col_name).isin('N', 'N/A'), None).otherwise(col(col_name))


columns_to_handle = ['product_id', 'amount', 'outcome']
for col_name in columns_to_handle:
    df_parsed = df_parsed.withColumn(col_name, handle_na(col_name))

df_parsed = df_parsed.withColumn("amount",
                                 when(col("amount").isNotNull(),
                                      col("amount").cast('double'))
                                 .otherwise(None))

df_json = df_parsed.select(to_json(struct(*[
    when(col(c).isNull(), lit(None)).otherwise(col(c)).alias(c)
    for c in df_parsed.columns
])).alias("value"))


def write_to_kafka(batch_df, batch_id):
    batch_df \
        .selectExpr("CAST(value AS STRING)") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9094") \
        .option("topic", "clickout") \
        .save()


query = df_json.writeStream \
    .foreachBatch(write_to_kafka) \
    .option("checkpointLocation", "file:///opt/spark-data/checkpoint-click") \
    .start()

query.awaitTermination()
