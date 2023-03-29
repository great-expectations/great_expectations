#!/bin/bash

# exit early if any command fails
set -e

# Install great_expectations using the airflow constraints file for our minimum supported version

AIRFLOW_VERSION=2.2.0  # TODO: this should be 2.0.2
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# 3.7 example
# https://raw.githubusercontent.com/apache/airflow/constraints-2.2.3/constraints-3.7.txt

echo "Install gx dependencies using airflow constraint file"
echo "Python:${PYTHON_VERSION}\nAirflow:${AIRFLOW_VERSION}\n${CONSTRAINT_URL}\n"

# Some issues may require uninstalling existing dependencies
# pip freeze | xargs pip uninstall -y
pip install . --constraint "${CONSTRAINT_URL}"

echo "\nCall 'get_context()' to check for show-stopping errors\n"

echo "python ci/checks/get_context.py"
python ci/checks/get_context.py
