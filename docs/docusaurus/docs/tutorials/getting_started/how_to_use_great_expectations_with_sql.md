---
title: Get Started with GX and SQL
---

import Prerequisites from '../../deployment_patterns/components/deployment_pattern_prerequisites.jsx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import Congratulations from '../../guides/connecting_to_your_data/components/congratulations.md'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will help you run an end-to-end workflow with Great Expectations with a SQL Datasource. 

For this guide, we will use a [PostgreSQL Database](https://www.postgresql.org/).

You will:
  - Load data
  - Instantiate a <TechnicalTag tag="data_context" text="Data Context" />
  - Create a <TechnicalTag tag="datasource" text="Datasource" /> & <TechnicalTag tag="data_asset" text="Data Asset" />
  - Create an <TechnicalTag tag="expectation_suite" text="Expectation Suite" />
  - Validate data using a <TechnicalTag tag="checkpoint" text="Checkpoint" />

## Prerequisites

<Prerequisites>

- Have a working PostgreSQL Database
- Have a working Python environment

</Prerequisites>

### 1. Install Great Expectations

Install Great Expectations in your Python environment:
```bash
pip install great-expectations
```

After that we will take care of some imports that will be used later:

```python name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py imports"
```

### 2. Set up Great Expectations

In this guide, we will be using your local filesystem for your Metadata Stores and <TechnicalTag tag="data_docs" text="Data Docs"/> store. This is a simple way to get up and running without configuring external resources. For other options for storing data see our "Metadata Stores" and "Data Docs" sections in the "How to Guides" for "Setting up Great Expectations."

Run the following code to set up a <TechnicalTag tag="data_context" text="Data Context"/> in code using the appropriate defaults:

```python name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py set up context"
```

### 3. Connect to your data

We will use a `connection_string` to securely connect to our PostgreSQL instance. Replace the following connection string 
with the correct connection string for your database. For more on other methods of connecting GX to your database, see [our guide on configuring credentials](https://docs.greatexpectations.io/docs/guides/setup/configuring_data_contexts/how_to_configure_credentials/).

```python
PG_CONNECTION_STRING = "postgresql+psycopg2://postgres:@localhost/taxi_db"
```

For this guide, we will be using using our familiar NYC taxi yellow cab data.

First, we create a <TechnicalTag tag='datasource' text='Datasource' />, representing the data available in our PostgreSQL database:

```python name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py add_datasource"
```

Then, we create a <TechnicalTag tag="data_asset" text="Data Asset" />, representing the discrete set of data we will be using. 

In this case, we are giving the name of a specific table within our database:

```python name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py add_asset"
```

Then we build a <TechnicalTag tag="batch_request" text="Batch Request" /> using the <TechnicalTag tag="data_asset" text="Data Asset" /> we configured earlier to use as a sample of data when creating Expectations:
```python name="tests/integration/docusaurus/deployment_patterns/databricks_deployment_patterns_file_python_configs.py build batch request"
```

<Congratulations />

Now let's keep going to create an Expectation Suite and validate our data.

### 5. Create Expectations

Here we will use a <TechnicalTag tag="validator" text="Validator" /> to interact with our batch of data and generate an <TechnicalTag tag="expectation_suite" text="Expectation Suite" />.

Each time we evaluate an Expectation (e.g. via `validator.expect_*`), it will immediately be Validated against your data. This instant feedback helps you zero in on unexpected data very quickly, taking a lot of the guesswork out of data exploration. Also, the Expectation configuration will be stored in the Validator. When you have run all of the Expectations you want for this dataset, you can call `validator.save_expectation_suite()` to save all of your Expectation configurations into an Expectation Suite for later use in a checkpoint.

First we create the suite and get a `Validator`:
```python name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py get validator"
```

Then we use the `Validator` to add a few Expectations:
```python name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py add expectations"
```

Finally we save our Expectation Suite (all of the unique Expectation Configurations from each run of `validator.expect_*`) to our Expectation Store:
```python name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py save suite"
```

### 6. Validate your data

Here we will create and store a <TechnicalTag tag="checkpoint" text="Checkpoint"/> for our batch, which we can use to validate and run post-validation actions. Check out our docs on "Validating your data" for more info on how to customize your Checkpoints.

First, we create the Checkpoint configuration utilizing our data context, passing in our Batch Request (our data) and our Expectation Suite (our tests):
```python name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py checkpoint config"
```

Next, we save the Checkpoint:
```python name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py add checkpoint config"
```

Finally, we run the Checkpoint:
```python name="tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py run checkpoint"
```

<details>
<summary>Checkpoint actions?</summary>

  In our Checkpoint configuration, we've included two important actions: `store_validation_result` & `update_data_docs`.

  `store_validation_result` saves your validation results from this Checkpoint run, allowing these results to be persisted for further use.

  `update_data_docs` builds Data Docs files for the validations run in this Checkpoint.

  Check out [our docs on Validating your data](https://docs.greatexpectations.io/docs/guides/validation/validate_data_overview) for more info on how to customize your Checkpoints.

  Also, to see the full Checkpoint configuration, you can run: <code>print(my_checkpoint.get_substituted_config().to_yaml_str())</code>
</details>

### 7. Build and view Data Docs

Since our Checkpoint contained an `UpdateDataDocsAction`, our <TechnicalTag tag="data_docs" text="Data Docs" /> have already been built from the validation we just ran. That means our Data Docs store will contain a new rendered validation result.

We can now open our Data Docs and review the results of our Checkpoint run:

```python
context.open_data_docs()
```

You've successfully validated your data with Great Expectations using SQL and viewed the resulting Data Docs. Check out our other guides for more customization options and happy validating!

### 8. What's next?

Now that you've created and saved a Data Context, Datasource, Data Asset, Expectation Suite, and Checkpoint, you can follow [our documentation on Checkpoints](https://docs.greatexpectations.io/docs/guides/validation/how_to_validate_data_by_running_a_checkpoint) 
to create a script to run this checkpoint without having to re-create your Assets & Expectations.

View the full script used in this page on GitHub:

- [postgres_deployment_patterns.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/deployment_patterns/postgres_deployment_patterns.py)