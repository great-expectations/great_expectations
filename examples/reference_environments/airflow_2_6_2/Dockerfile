# Modify the line below to install a different version of python or airflow
# E.g. for python 3.8 and airflow 2.6.1 use the line below:
# FROM apache/airflow:2.6.1-python3.8
# Please note that you may need a different compose.yaml file for different versions of airflow, see:
# https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#fetching-docker-compose-yaml
# for more details.
FROM apache/airflow:2.6.2-python3.10


# Set the permissions on the data docs folder to be owned by airflow and available to all users.
USER root
RUN mkdir -p /gx/gx_stores/data_docs/
RUN chown -R airflow:0 /gx && chmod -R 777 /gx
USER airflow

WORKDIR /gx

# modify the line below to install a different version of GX
# E.g. for GX 0.17.0 use the line below:
# RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" great_expectations==0.17.0
# Note, you can also install GX from the develop branch (requires a `--rebuild`),
# or replace develop with the branch name you want to install from:
# RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" git+https://github.com/great-expectations/great_expectations.git@develop

# Note, also pinning to <2.0.0 for sqlalchemy since airflow is not yet compatible: https://github.com/apache/airflow/issues/28723
# This line can be changed to `RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" great_expectations` e.g. remove "sqlalchemy<2.0.0" "pandas<2.0.0" once they are compatible.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" great_expectations "sqlalchemy<2.0.0" "pandas<2.0.0"
