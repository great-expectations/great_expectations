#!/bin/bash

print_orange () {
    ORANGE='\033[38;5;208m'
    NC='\033[0m' # No Color
    echo -e "${ORANGE}$1${NC}"
}

print_orange "Downloading airflow base docker compose"
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.6.0/docker-compose.yaml'

print_orange "Make directories to mount to containers"
mkdir ./dags ./plugins ./logs

print_orange "Set user/group for directories (for mac / linux)"
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

print_orange "Initialize airflow"
docker compose up airflow-init

print_orange "Startup airflow containers"
docker compose up
