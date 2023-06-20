---
title: Dynamically load evaluation parameters from a database
---

import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will help you create an <TechnicalTag tag="expectation" text="Expectation" /> that loads part of its Expectation configuration from a database at runtime. Using a dynamic <TechnicalTag tag="evaluation_parameter" text="Evaluation Parameter" /> makes it possible to maintain part of an <TechnicalTag tag="expectation_suite" text="Expectation Suite" /> in a shared database.

## Prerequisites

<Prerequisites>

- [A working deployment of Great Expectations](/docs/guides/setup/setup_overview).
- Credentials for a database to query for dynamic values.
- A SQL query to return values for your expectation configuration.

</Prerequisites>

## Add a new SqlAlchemy Query Store to your Data Context

A SqlAlchemy Query <TechnicalTag tag="store" text="Store" /> acts as a bridge that can query a SqlAlchemy-connected database and return the result of the query to be available for an evaluation parameter.

Find the ``stores`` section in your ``great_expectations.yml`` file, and add the following configuration for a new store called "my_query_store". You can add and reference multiple Query Stores with different names.

By default, query results will be returned as a list. If instead you need a scalar for your expectation, you can specify the return_type

```yaml name="tests/integration/fixtures/query_store/great_expectations/great_expectations.yml my_query_store"
```

Ensure you have added valid credentials to the ``config-variables.yml`` file (replacing the values with your database credentials):

```yaml name="tests/integration/fixtures/query_store/great_expectations/uncommitted/config_variables.yml my_query_store_creds"
```

## In a notebook, get a test Batch of data to use for Validation

```python name="tests/integration/docusaurus/expectations/advanced/how_to_dynamically_load_evaluation_parameters_from_a_database.py get_validator"
```

## Define an Expectation that relies on a dynamic query

Great Expectations recognizes several types of Evaluation Parameters that can use advanced features provided by the <TechnicalTag tag="data_context" text="Data Context" />. To dynamically load data, we will be using a store-style URN, which starts with `urn:great_expectations:stores`. The next component of the URN is the name of the store we configured above (``my_query_store``), and the final component is the name of the query we defined above (``unique_passenger_counts``):

```python name="tests/integration/docusaurus/expectations/advanced/how_to_dynamically_load_evaluation_parameters_from_a_database.py define expectation"
```

The SqlAlchemyQueryStore that you configured above will execute the defined query and return the results as the value of the ``value_set`` parameter to evaluate your Expectation:

```python name="tests/integration/docusaurus/expectations/advanced/how_to_dynamically_load_evaluation_parameters_from_a_database.py expected_validator_results"
```
