#!/bin/bash

# Run tests defined in the Great Expectations Airflow Operator
REPO_ROOT=$(pwd)

# Install the airflow provider from it's repo
cd ..
git clone https://github.com/astronomer/airflow-provider-great-expectations.git
cd airflow-provider-great-expectations
pip install -e '.[tests]'

echo "Uninstall Great Expectations installed by Airflow Provider and install from PR commit"
pip uninstall -y great-expectations
cd $REPO_ROOT
pip install -c constraints-dev.txt -e ".[test]"

echo "Run Great Expectations Airflow Provider Tests"
cd ../airflow-provider-great-expectations
pytest
