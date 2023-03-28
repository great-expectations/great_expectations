#!/bin/bash

# Install great_expectations using the airflow constraints file for our minimum supported version

AIRFLOW_VERSION=2.2.3
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# 3.7 https://raw.githubusercontent.com/apache/airflow/constraints-2.5.2/constraints-3.7.txt

echo "Install gx dependencies using airflow constraint file"
echo "  ${CONSTRAINT_URL}\n"

pip install -e . --constraint "${CONSTRAINT_URL}"

echo "\nCall 'get_context()' to check for show-stopping errors\n"

echo "python ci/checks/get_context.py"
python ci/checks/get_context.py
