---
title: How to Use Great Expectations with Meltano
---
import Prerequisites from './components/deployment_pattern_prerequisites.jsx'

This guide will help you get Great Expectations installed, configured, and running in your Meltano project.

[Meltano](https://meltano.com/) is an Open Source DataOps OS that's used to install and configure data applications (Great Expectations, Singer, dbt, Airflow, etc.) that your team's data platform is built on top of, all in one central repository.
Using Meltano enables teams to easily implement DataOps best practices like configuration as code, code reviews, isolated test environments, CI/CD, etc.
A common use case is to manage ELT pipelines with Meltano and as part of ensuring the quality of the data in those pipelines, teams bring in Great Expectations.

Meltano uses the concept of [plugins](https://docs.meltano.com/concepts/plugins) to manage external package like Great Expectations.
In this case Great Expectations is supported as a Utility plugin.


## Install Meltano

If you don't already have a Meltano project set up, you can follow these steps to get one setup.
Refer to the Meltano [Getting Started Guide](https://docs.meltano.com/getting-started) for more detail or join us in the [Meltano Slack](https://meltano.com/slack).

```bash
# Install Meltano
pip install meltano
# Create a project directory
mkdir meltano-projects
cd meltano-projects
# Initialize your project
meltano init meltano-great-expectations-project
cd meltano-great-expectations-project
```

## Add Great Expectations

Add Great Expectations to your Meltano project and configure any additional python requirements based on your data sources.
Refer to the Great Expectations [connecting to your data source](https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/connect_to_data_overview) docs for more details.

```bash
# Add utility plugin
meltano add utility great_expectations
# Run a command to ensure installation was successful
meltano invoke great_expectations --help
# Add any additional python requirements (e.g. Snowflake Database requirements)
meltano config great_expectations set _pip_url "great_expectations; sqlalchemy; snowflake-connector-python; snowflake-sqlalchemy"
# Refresh install based on requirement updates
meltano install utility great_expectations
```

This installation process adds all packages and files needed.

If you already have an existing Great Expectation project you can copy it into the `./utilities/` directory where it will be automatically detected by Meltano.

If not, initialize your project and continue.

```bash
cd utilities
meltano invoke great_expectations init
```


## Add Data Source, Expectation Suite, and Checkpoint

If you haven't already done so, follow the Great Expectations [documentation](https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/connect_to_data_overview) to get a datasource, expectation suite, and checkpoint configured.

You can run the commands through the Meltano CLI, for example:

```bash
meltano invoke great_expectations datasource new
meltano invoke great_expectations suite new
meltano invoke great_expectations checkpoint new <checkpoint_name>
```

:::tip

Using the Meltano [environments feature](https://docs.meltano.com/concepts/environments) you can parameterize your Datasource to allow you to toggle between a local, development, or production Datasource.
For example a snippet of a Snowflake configured Datasource is below.
```yaml
class_name: Datasource
execution_engine:
    credentials:
    host: ${GREAT_EXPECTATIONS_HOST}
    username: ${GE_USERNAME}
    database: ${GE_PROD_DATABASE}
    query:
        schema: GREAT_EXPECTATIONS
        warehouse: ${GE_WAREHOUSE}
        role: ${GE_ROLE}
    password: ${GREAT_EXPECTATIONS_PASSWORD}
    drivername: snowflake
    module_name: great_expectations.execution_engine
    class_name: SqlAlchemyExecutionEngine
```
Part of Meltano's benefit is wrapping installed packages and injecting configurations to enable isolation and test environments.

:::

## Run your Expectations using Meltano

Now that your expectations are created you can run them using the following commands:

```bash
meltano invoke great_expectations checkpoint run <checkpoint_name>
```

## Common Meltano x Great Expectation Use Cases

Commonly Meltano is used for ELT pipelines and Great Expectations is a perfect complement to take pipelines to the next level of quality and stability.
In the context of ELT pipelines with Meltano there are a few common implementation patterns for Great Expectations:

1. **Transformation Boundaries**

       Expectations for the entry and exit points of the transformation steps.
       Does the data meet expectations before I transform?
       Do the dbt consumption models (i.e. fact and dimension tables) meet expectations?

1. **Source Validation Prior to Replication**

       Expectations for the source data in the source system.
       Does my Postgres DB (or any source) data meet expectations before I replicate it to my warehouse?
       Are their source data problems I should be aware of?

1. **Profiling For Migration**

       As part of a migration between warehouses, profiling can give confidence that the data in the new warehouse meets expectations by matching the profile of the original warehouse.
       Am I confident that my new warehouse has all my data before switching over?

1. **Transformation Between Steps**

       Expectations between each transformation before continuing on to the next step.
       Does the data meet expectations after each dbt model is created in my transformation pipeline?

Of course, there's plenty of other ways to implement expectations in your project but it's always helpful to hear common patterns for the ELT context.

## Summary

Meltano is a great way to install, configure, and run Great Expectations in your data platform.
It allows you to configure all your code in one central git repository and enables DataOps best practices like configuration as code, code reviews, isolated test environments, CI/CD, etc.

If you have any questions join us in the [Meltano Slack](https://meltano.com/slack)!
