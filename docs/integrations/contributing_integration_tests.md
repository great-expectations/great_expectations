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
While test requirements vary widly, we made some decisions to help us narrow down the cases we cover while keeping developer friction at a minimum.
To this end we have created a taxonomy of the types of tests that our self-service integration tests framework supports.

#### Type 1
Type 1 integration tests are those which are fully self-contained and can be or are already containerized. Our framework supports running tests in docker containers and(optionally) orchestrated via `docker compose`.

#### Type 2
Type 2 integration tests are those which depend on outside/third party services. For example, your integration depends on AWS Redshift, Snowflake, Databricks, etc. In this case
credentials for those will have to be provided to us (off-band). More about the specifics below.

## Steps for Adding Type 1 Tests

### 0. Reach out to our Developer Relations team
Before you embark in this journey, drop by and introduce yourself in the #integrations channel in our [Great Expectations Slack](https://greatexpectationstalk.slack.com)
to let us know. We're big believers of building strong relationships with ecosystem partners. And thus we believe
opening communication channels early in the process is essential.

### 1. Copy the template
Create a copy of `integration_template.md` and name it `integration_<my_product>.md`. This file is located in `great_expectations/docs/integrations/` directory.
This file is in markdown format and supports basic [docusaurus admonitions](https://docusaurus.io/docs/markdown-features/admonitions).

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
