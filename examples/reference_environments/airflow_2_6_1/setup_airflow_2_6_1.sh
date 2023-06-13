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


print_orange_header "Setting user/group for directories (for mac / linux)"
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

# TODO: Only initialize if not already initialized

print_orange_header "Initializing airflow"
docker compose up airflow-init --build

print_orange_header "Starting airflow containers"
docker compose up --build
# TODO: pass ARG PYTHON_BASE_IMAGE="python:3.8-slim-bullseye" to docker compose via build arg
