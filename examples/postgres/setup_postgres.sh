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

print_orange_header "Downloading example data"
curl -LfO 'https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv'

print_orange_header "Starting postgres containers"
docker compose up
