#!/bin/bash

ENV_FILE=".env"

NEW_UID=$(id -u)

if [ -f "$ENV_FILE" ]; then
  sed -i '/^AIRFLOW_UID=/d' "$ENV_FILE"
  
  echo "AIRFLOW_UID=$NEW_UID" >> "$ENV_FILE"
else
  echo "AIRFLOW_UID=$NEW_UID" > "$ENV_FILE"
fi

if [ -f hiveconf/hiveserver2.pid ]; then
    rm hiveconf/hiveserver2.pid
fi

if [ -d spark/data/checkpoint ]; then
    rm -r spark/data/*
fi

kaggle datasets download -d ren294/ecommerce-clickstream-transactions && \
unzip ecommerce-clickstream-transactions.zip -d ecommerce-clickstream-transactions && \
mv ecommerce-clickstream-transactions/* nifi/data/ && \
rm -rf ecommerce-clickstream-transactions.zip ecommerce-clickstream-transactions

kaggle datasets download -d ren294/access-log-ecommerce && \
unzip access-log-ecommerce.zip -d access-log-ecommerce && \
mv access-log-ecommerce/* nifi/data/ && \
rm -rf access-log-ecommerce.zip access-log-ecommerce
