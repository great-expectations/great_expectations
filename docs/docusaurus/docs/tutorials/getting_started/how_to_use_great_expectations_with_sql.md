---
title: Get Started with GX and SQL
---

import Prerequisites from '../../deployment_patterns/components/deployment_pattern_prerequisites.jsx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

Use the information provided here to learn how you can use Great Expectations (GX) with a SQL Datasource. The following examples use a [PostgreSQL Database](https://www.postgresql.org/).

To use GX with PostgreSQL Database, you'll complete the following tasks:

- Load data
- Instantiate a <TechnicalTag tag="data_context" text="Data Context" />
- Create a <TechnicalTag tag="datasource" text="Datasource" /> & <TechnicalTag tag="data_asset" text="Data Asset" />
- Create an <TechnicalTag tag="expectation_suite" text="Expectation Suite" />
- Validate data using a <TechnicalTag tag="checkpoint" text="Checkpoint" />

The full code used in the following examples is available on GitHub:

- [postgres_deployment_patterns.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py)

## Prerequisites

<Prerequisites>

- A working PostgreSQL Database
- A working Python environment

</Prerequisites>

## 1. Install GX

1. Run the following command to install GX in your Python environment:

  ```bash
  pip install great-expectations
  ```

2. Run the following command to import configuration information that you'll use in the following steps:

  ```python name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py imports"
  ```

## 2. Set up GX

To avoid configuring external resources, you'll use your local filesystem for your Metadata Stores and <TechnicalTag tag="data_docs" text="Data Docs"/> store.

Run the following code to create a <TechnicalTag tag="data_context" text="Data Context"/> with the default settings:

```python name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py set up context"
```

## 3. Connect to your data

1. Use a `connection_string` to securely connect to your PostgreSQL instance. For example

  ```python
  PG_CONNECTION_STRING = "postgresql+psycopg2://postgres:@localhost/taxi_db"
  ```

  Replace the connection string with the connection string for your database. For additional information about other connection methods, see [How to configure credentials](https://docs.greatexpectations.io/docs/guides/setup/configuring_data_contexts/how_to_configure_credentials/). In this example, existing New York City taxi cab data is being used.

2. Run the following command to create a <TechnicalTag tag='datasource' text='Datasource' /> to represent the data available in your PostgreSQL database:

  ```python name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py add_datasource"
  ```

3. Run the following command to create a <TechnicalTag tag="data_asset" text="Data Asset" /> to represent a discrete set of data: 

  ```python name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py add_asset"
  ```

  In this example, the name of a specific table within your database is used.

4. Run the following command to build a <TechnicalTag tag="batch_request" text="Batch Request" /> using the <TechnicalTag tag="data_asset" text="Data Asset" /> you configured previously:

  ```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py build batch request"
  ```

## 4. Create Expectations

You'll use a <TechnicalTag tag="validator" text="Validator" /> to interact with your batch of data and generate an <TechnicalTag tag="expectation_suite" text="Expectation Suite" />.

Every time you evaluate an Expectation with `validator.expect_*`, it is immediately Validated against your data. This instant feedback helps you identify unexpected data and removes the guesswork from data exploration. The Expectation configuration is stored in the Validator. When you are finished running the Expectations on the dataset, you can use `validator.save_expectation_suite()` to save all of your Expectation configurations into an Expectation Suite for later use in a checkpoint.

1. Run the following command to create the suite and get a `Validator`:

  ```python name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py get validator"
  ```

2. Run the following command to use the `Validator` to add a few Expectations:

  ```python name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py add expectations"
  ```

3. Run the following command to save your Expectation Suite (all the unique Expectation Configurations from each run of `validator.expect_*`) to your Expectation Store:

  ```python name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py save suite"
  ```
## 5. Validate your data

You'll create and store a <TechnicalTag tag="checkpoint" text="Checkpoint"/> for your batch, which you can use to validate and run post-validation actions.

1. Run the following command to create the Checkpoint configuration that uses your Data Context:

  ```python name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py checkpoint config"
  ```

2. Run the following command to save the Checkpoint:

  ```python name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py add checkpoint config"
  ```

3. Run the following command to run the Checkpoint and pass in your Batch Request (your data) and your Expectation Suite (your tests):

  ```python name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py run checkpoint"
  ```

  Your Checkpoint configuration includes the `store_validation_result` and `update_data_docs` actions. The `store_validation_result` action saves your validation results from the Checkpoint run and allows the results to be persisted for future use. The  `update_data_docs` action builds Data Docs files for the validations run in the Checkpoint.

  To learn more about Data validation and customizing Checkpoints, see [Validate Data: Overview ](https://docs.greatexpectations.io/docs/guides/validation/validate_data_overview).

  To view the full Checkpoint configuration, run `print(my_checkpoint.get_substituted_config().to_yaml_str())`.

## 6. Build and view Data Docs

Your Checkpoint contained an `UpdateDataDocsAction`, so your <TechnicalTag tag="data_docs" text="Data Docs" /> have already been built from the validation you ran and your Data Docs store contains a new rendered validation result.

Run the following command to open your Data Docs and review the results of your Checkpoint run:

```python
context.open_data_docs()
```

## Next steps

Now that you've created and saved a Data Context, Datasource, Data Asset, Expectation Suite, and Checkpoint, see [Validate data by running a Checkpoint](https://docs.greatexpectations.io/docs/guides/validation/how_to_validate_data_by_running_a_checkpoint) 
to create a script to run the Checkpoint without the need to recreate your Data Assets and Expectations.