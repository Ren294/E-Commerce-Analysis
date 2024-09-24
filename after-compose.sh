#!/bin/bash

docker exec -it namenode bash -f /home/setup/setup.sh

docker exec -i db-ecom pg_restore -U postgres -d ecom /home/ecom_backup.sql

# docker exec -it spark-master bash -c "service ssh start"

docker exec -it cassandra1 /opt/cassandra/bin/cqlsh -f /home/setup/setup.cql

docker exec -it spark-master bash -c "start-thriftserver.sh"

docker exec -it airflow-webserver bash -f /opt/airflow/config/setup.sh
