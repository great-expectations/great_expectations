#!/bin/bash

# Run tests defined in the Great Expectations Airflow Operator

echo "Using great expectations version vvv"
pwd
pip freeze | grep great-expectations
ls
echo "Using great expectations version ^^^"

# Install the airflow provider from it's repo
cd ..
git clone https://github.com/astronomer/airflow-provider-great-expectations.git

cd airflow-provider-great-expectations

echo "Using great expectations version vvv"
pip freeze | grep great-expectations
echo "Using great expectations version ^^^"

pip install -e '.[tests]'

echo "Using great expectations version vvv"
pip freeze | grep great-expectations
echo "Using great expectations version ^^^"

echo "Uninstall Great Expectations installed by airflow provider"
pip uninstall -y great-expectations
echo "Using great expectations version vvv"
pip freeze | grep great-expectations
echo "Using great expectations version ^^^"

# Install GX from it's repo
cd ../great_expectations
pip install -c constraints-dev.txt -e ".[test]"

echo "Using great expectations version vvv"
pip freeze | grep great-expectations
echo "Using great expectations version ^^^"

pytest
