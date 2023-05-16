#!/bin/bash

print_orange () {
    ORANGE='\033[38;5;208m'
    NC='\033[0m' # No Color
    echo -e "${ORANGE}$1${NC}"
}

print_orange_line () {
    print_orange "================================================================================"
}

print_orange_header () {
    print_orange_line
    print_orange "$1"
    print_orange_line
}

print_orange_header "Downloading airflow base docker compose"
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.6.0/docker-compose.yaml'

print_orange_header "Making directories to mount to containers"
mkdir ./dags ./plugins ./logs

print_orange_header "Setting user/group for directories (for mac / linux)"
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

# TODO: Only initialize if not already initialized

print_orange_header "Initializing airflow"
docker compose up airflow-init

print_orange_header "Adding postgres setup files"
cp ../postgres/db_setup.Dockerfile .
cp ../postgres/load_data.py .

print_orange_header "Downloading example data"
curl -LfO 'https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv'

print_orange_header "Insert postgres_services.yaml in airflow docker-compose.yaml"
sed -e '$!N;P;/\nvolumes:/r postgres_services.yaml' -e D docker-compose.yaml > temp
mv temp docker-compose.yaml

print_orange_header "Starting airflow containers"
docker compose up
