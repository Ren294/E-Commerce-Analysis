airflow connections add spark_server \
    --conn-type ssh \
    --conn-host spark-master \
    --conn-login root \
    --conn-password ren294 \
    --conn-port 22

airflow connections add hdfs_server \
    --conn-type ssh \
    --conn-host namenode \
    --conn-login root \
    --conn-password ren294 \
    --conn-port 22

airflow connections add sqoop_server \
    --conn-type ssh \
    --conn-host sqoop \
    --conn-login root \
    --conn-password ren294 \
    --conn-port 22
