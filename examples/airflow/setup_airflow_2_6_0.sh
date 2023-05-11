#!/bin/bash

# Get airflow base docker compose
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.6.0/docker-compose.yaml'

# Make directories mounted to containers
mkdir ./dags ./plugins ./logs

# Set user/group for directories (for mac / linux)
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

# Initialize airflow
docker compose up airflow-init

# Startup airflow containers
docker compose up

