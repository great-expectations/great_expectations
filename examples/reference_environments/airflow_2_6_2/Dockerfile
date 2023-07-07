# Modify the line below to install a different version of python or airflow
# E.g. for python 3.8 and airflow 2.6.1 use the line below:
# FROM apache/airflow:2.6.1-python3.8
# Please note that you may need a different compose.yaml file for different versions of airflow, see:
# https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#fetching-docker-compose-yaml
# for more details.
FROM apache/airflow:2.6.2-python3.10

# modify the line below to install a different version of GX
# E.g. for GX 0.17.0 use the line below:
# RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" great_expectations==0.17.0
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" great_expectations
