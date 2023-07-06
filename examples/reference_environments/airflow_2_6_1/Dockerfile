FROM apache/airflow:2.6.1

# Install gcc as root, not available in airflow image by default:
USER root
RUN apt-get update && apt-get -y install sudo
RUN sudo apt-get -y install build-essential
USER airflow

RUN pip install --no-cache-dir "apache-airflow==2.6.1" great_expectations
