#!/bin/bash

# Run tests defined in the Great Expectations Airflow Operator

git clone https://github.com/astronomer/airflow-provider-great-expectations.git

cd airflow-provider-great-expectations

echo "Using great expectations version vvv"
pip freeze | grep great-expectations
echo "Using great expectations version ^^^"

pip install -e '.[tests]'

echo "Using great expectations version vvv"
pip freeze | grep great-expectations
echo "Using great expectations version ^^^"

pytest
