---
title: Use Great Expectations in EMR Serverless
description: "Use Great Expectations in EMR Serverless"
sidebar_label: "EMR Serverless"
sidebar_custom_props: { icon: 'img/integrations/page_icon.svg' }
---
import Prerequisites from './components/deployment_pattern_prerequisites.jsx'
import Congratulations from '../guides/connecting_to_your_data/components/congratulations.md'

import InProgress from '/docs/components/warnings/_in_progress.md'

<InProgress />

This Guide demonstrates how to set up, initialize and run validations against your data on AWS EMR Serverless.
We will cover case with RuntimeDataConnector and use S3 as metadata store.

### 0. Pre-requirements

- Configure great_expectations.yaml and upload to your S3 bucket or generate it dynamically from code, notice critical moment, that you need to add endpoint_url to data_doc section
```yaml name="tests/integration/docusaurus/deployment_patterns/aws_emr_serverless_deployment_patterns_great_expectations.yaml"
```


### 1. Install Great Expectations
Create a Dockerfile and build it to generate virtualenv archive and upload this tar.gz output to S3 bucket.
At requirements.txt you should have great_expectations package and everything else what you want to install 
```dockerfile
FROM --platform=linux/amd64 amazonlinux:2 AS base

RUN yum install -y python3

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
COPY ./requirements.txt /
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install -r requirements.txt --no-cache-dir

RUN mkdir /output && venv-pack -o /output/pyspark_ge.tar.gz

FROM scratch AS export
COPY --from=base /output/pyspark_ge.tar.gz /
```
When you will configure a job, it's necessary to define additional params to Spark properties:
```bash
--conf spark.archives=s3://bucket/folder/pyspark_ge.tar.gz#environment 
--conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python 
--conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python 
--conf spark.emr-serverless.executorEnv.PYSPARK_PYTHON=./environment/bin/python 
--conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory
```

Then import necessary libs:
```python name="tests/integration/docusaurus/deployment_patterns/aws_emr_serverless_deployment_patterns.py imports"
```

### 2. Set up Great Expectations
Here we initialize a Spark, and read great_expectations.yaml
```python name="tests/integration/docusaurus/deployment_patterns/aws_emr_serverless_deployment_patterns.py initialize spark"
```

### 3. Connect to your data
```python name="tests/integration/docusaurus/deployment_patterns/aws_emr_serverless_deployment_patterns.py connect to data"
```

### 4. Create Expectations
```python name="tests/integration/docusaurus/deployment_patterns/aws_emr_serverless_deployment_patterns.py create expectations"
```

### 5. Validate your data
```python name="tests/integration/docusaurus/deployment_patterns/aws_emr_serverless_deployment_patterns.py validate"
```

### 6. Congratulations!
Your data docs built on S3 and you can see index.html at the bucket


<details>
  <summary>This documentation has been contributed by Bogdan Volodarskiy from Provectus</summary>
  <div>
    <p>
      Our links:
    </p>
    <ul>
      <li> <a href="https://www.linkedin.com/in/bogdan-volodarskiy-652498108/">Author's Linkedin</a> </li>
      <li> <a href="https://medium.com/@bvolodarskiy">Author's Blog</a> </li>
      <li> <a href="https://provectus.com/">About Provectus</a> </li>
      <li> <a href="https://provectus.com/data-quality-assurance/">About Provectus Data QA Expertise</a> </li>
</ul>
  </div>
</details>