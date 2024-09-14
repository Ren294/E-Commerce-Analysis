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
