---
title: How to test integrations
---

### Introduction
As the data stack ecosystem grows and expands in usage and tooling, so does the need to integrate with 3rd party
products or services. [Superconductive](https://superconductive.com) as drivers and ushers
of [Great Expectations](https://greatexpectations.io), we want to make the process to integrating with Great Expectations
as low friction as possible. We are committed to work and iterate in the process and greatly value any feedback you may have.
The aim of this document is to provide guidance for vendors or community partners which wish to integrate with us as to how to test said integrations within our testing infrastructure.

### Scope
While test requirements vary wildly, we made some decisions to help us narrow down the cases we cover while keeping developer friction at a minimum.
To this end we have created a taxonomy of the types of tests that our self-service integration tests framework supports.

#### Type 1
Type 1 integration tests are those which are fully self-contained and can be or are already containerized. Our framework supports running tests in docker containers and(optionally) orchestrated via `docker compose`.

#### Type 2
Type 2 integration tests are those which depend on outside/third party services. For example, your integration depends on AWS Redshift, Snowflake, Databricks, etc. In this case
credentials for those will have to be provided to us (off-band). More about the specifics below.

## Steps for Adding Type 1 Tests

### 0. Create directory structure
Your test and pipeline definitions must live inside `great_expectations/assets/partners` in a folder named after the service or tool you're integrating. For example, you're contributing an integration to your new cloud-based database service AnthonyDB, thus the testing code and pipeline definition should live in `great_expectations/assets/partners/anthonydb` directry (note all lowercase names by convention).

### 1. Create a `Dockerfile`
This type of integration (Type 1) is for integrating products or services that can be containerized and are self-contained so that
they can be run without needing to access to outside services. For this example we're calling AnthonyDB, we're containerizing and running tests against MSSQL, which is notoriously difficult to set up and run in containers. The reason we chose this example is as a "worst case", but we expect most integrations contributed will be much simpler to set up and run against. So bearing that in mind, your Dockerfile would look something as follows:
```
FROM --platform=linux/amd64 python:3.9-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
RUN apt-get update
RUN apt-get install -y git build-essential

# deps for mssql
RUN apt-get install -y curl apt-transport-https debconf-utils
# Add mssql repo
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
# mssql driver
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17

# used by requirements.txt: pyodbc, and enables mssql dialect
RUN apt-get install -y unixodbc-dev
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . /app/
RUN great_expectations init -y
CMD pytest test_ge.py -vvv
```

### 2. Add a `docker-compose.yml`
In order to orchestrate the tests and the provisioning, we make use of `docker-compose`. In this file we define the service
dependencies among them. Please note you may not need one or many of the services in the file below, we include them as illustration:
```
## Docker Compose file for running e2e tests
version: "3"

services:
  db:
    image: mcr.microsoft.com/mssql/server:2019-latest
    environment:
      ACCEPT_EULA: Y
      SA_PASSWORD: "$DB_PASS"
    ports:
      - 1433:1433
  integration_test:
    build:
      context: .
      dockerfile: Dockerfile
    command: pytest -vvv test_ge.py
    environment:
      DB_URL: ${DB_URL}
    depends_on:
      - provision
  provision:
    build:
      context: .
      dockerfile: provision.Dockerfile
    command: cat provision.mssql | /opt/mssql-tools/bin/sqlcmd -S db -U sa -P "$DB_PASS" -i /dev/stdin
    depends_on:
      - db
```

### 3. (Optional) Add provisioning instrumentation `Dockerfile`
If your tests rely on certain data being present in a database or such, you must provide provisioning instrumentation for it. In the step above we see a service definition called `provision` which is in charge to populate the database. Of note, in the service definition we create an inter service dependency so that this service isn't run until after the `db` has successfully completed.
```
FROM --platform=linux/amd64 python:3.9-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
RUN apt-get update
RUN apt-get install -y git build-essential

# deps for mssql
RUN apt-get install -y curl apt-transport-https debconf-utils gcc
# Add mssql repo
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
# mssql driver
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17 mssql-tools

# used by requirements.txt: pyodbc, and enables mssql dialect
RUN apt-get install -y unixodbc-dev
COPY . /app/
```

### 4. (Optional) Add provisioning script
Together with the above defined container, this script is what will define the database, schema and (potentially) data:
```
    IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = 'integration')
  BEGIN
    CREATE DATABASE [integration]
    END
    GO
           USE [integration]
    GO

    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='taxi_data' and xtype='U')
BEGIN
    CREATE TABLE taxi_data (
    vendor_id double precision,
    pickup_datetime text,
    dropoff_datetime text,
    passenger_count double precision,
    trip_distance double precision,
    rate_code_id double precision,
    store_and_fwd_flag text,
    pickup_location_id bigint,
    dropoff_location_id bigint,
    payment_type double precision,
    fare_amount double precision,
    extra double precision,
    mta_tax double precision,
    tip_amount double precision,
    tolls_amount double precision,
    improvement_surcharge double precision,
    total_amount double precision,
    congestion_surcharge double precision
);
END
GO
```

### 5. Add `requirements.txt`
This `pip` requirements file should contain any and all library dependencies needed to run the tests:
```
great_expectations
pyodbc>=4.0.30
pyarrow
pytest
```
## Steps for Adding Type 2 Tests

### 0. Create directory structure
Your test and pipeline definitions must live inside `great_expectations/assets/partners` in a folder named after the service or tool you're integrating. For example, you're contributing an integration to your new cloud-based database service DBShift, thus the testing code and pipeline definition should live in `great_expectations/assets/partners/dbshift` directry (note all lowercase names by convention).

### 1. Add `requirements.txt` in the directory above 
This file should contain any and all dependencies needed to run the tests, for example:
```
great_expectations
psycopg2-binary
dbshift>=0.7.7
pyarrow
pytest==7.0.1
```
### 2. Add your test file
This file should be named `test_ge.py` and should contain all relevant pytests. For example,
```
import os

from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest

def test_ge():


    CONNECTION_STRING = os.environ.get("DB_URL")

    context = ge.get_context()

    # Test setup code

    # One or more `assert` ions
```

### 3. Add your tests to the partner integration pipeline
In the root `great_expectations` directory, add your test defintion to `azure-pipelines-partner-integration.yml`.
For this example, it should look something like this:
```
trigger:
  branches:
    include:
    - pre_pr_docs-*

stages:
  - stage: scope_check
    pool:
      vmImage: 'ubuntu-20.04'
    jobs:
      - job: partner_integration_dbshift
        steps:
          - bash: cd assets/partners/dbshift; ../common/run-1.sh 
            name: RunDockerComposeUp
            env:
              DB_URL: $(DBSHIFT_URL) # must be provided to us in advance
```

### 4. Provide credentials/connection string
In order for these tests to run, they'll need credentials and or connection string information. Due to the sensitive nature of shared
credentials, we highly recommend you make the with very limited scope with only enough privileges for what's needed to run the tests and nothing else. Once you have such credentials and or connection string, reach to us via slack or email and we will add them to our azure secure vault where the CI pipelines can make use of it.

## Further assistance
 *  GE Slack #integrations channel
 *  Github issue (mention @devrel)
