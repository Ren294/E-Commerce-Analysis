spark-submit --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.apache.kafka:kafka-clients:3.2.0 \
    /opt/spark-apps/streaming/logStreaming.py &> /opt/spark/logs/logStreaming.log