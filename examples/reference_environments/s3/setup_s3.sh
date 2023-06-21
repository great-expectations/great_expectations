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

print_orange_header "Shutting down open containers..."
docker-compose down

print_orange_header "Rebuilding containers..."
docker-compose build --no-cache

print_orange_header "Starting containers..."
docker-compose up -d

print_orange_header "Running container..."
docker-compose exec gx_sandbox bash
