---
title: How to get one or more Batches from a Datasource configured with the block-config method
---
import Prerequisites from '../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will help you load a <TechnicalTag tag="batch" text="Batch" /> for validation using an active <TechnicalTag tag="data_connector" text="Data Connector" />. For guides on loading batches of data from specific <TechnicalTag tag="datasource" text="Datasources" /> using a Data Connector see the [Datasource specific guides in the "Connecting to your data" section](./index.md).

A <TechnicalTag tag="validator" text="Validator" /> knows how to <TechnicalTag tag="validation" text="Validate" /> a particular Batch of data on a particular <TechnicalTag tag="execution_engine" text="Execution Engine" /> against a particular <TechnicalTag tag="expectation_suite" text="Expectation Suite" />. In interactive mode, the Validator can store and update an Expectation Suite while conducting Data Discovery or Exploratory Data Analysis.

<Prerequisites>

- [Configured and loaded a Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context)
- [Configured a Datasource and Data Connector](../../terms/datasource.md)
  
</Prerequisites>

## Steps: Loading one or more Batches of data

To load one or more `Batch(es)`, the steps you will take are the same regardless of the type of `Datasource` or `Data Connector` you have set up. To learn more about `Datasources`, `Data Connectors` and `Batch(es)` see our [Datasources Guide](../../terms/datasource.md). 

### 1. Construct a BatchRequest

:::note
As outlined in the `Datasource` and `Data Connector` docs mentioned above, this `Batch Request` must reference a previously configured `Datasource` and `Data Connector`.
:::

```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_get_one_or_more_batches_of_data_from_a_configured_datasource.py all batches"
```

Since a `BatchRequest` can return multiple `Batch(es)`, you can optionally provide additional parameters to filter the retrieved `Batch(es)`. See [Datasources Guide](../../terms/datasource.md) for more info on filtering besides `batch_filter_parameters` and `limit` including custom filter functions and sampling. The example `BatchRequest`s below shows several non-exhaustive possibilities. 

```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_get_one_or_more_batches_of_data_from_a_configured_datasource.py index data_connector_query"
```

```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_get_one_or_more_batches_of_data_from_a_configured_datasource.py twelve batches from 2020"
```

```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_get_one_or_more_batches_of_data_from_a_configured_datasource.py first 5 batches from 2020"
```

```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_get_one_or_more_batches_of_data_from_a_configured_datasource.py data_connector_query"
```

You may also wish to list available batches to verify that your `BatchRequest` is retrieving the correct `Batch(es)`, or to see which are available. You can use `context.get_batch_list()` for this purpose by passing it your `BatchRequest`:

```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_get_one_or_more_batches_of_data_from_a_configured_datasource.py get_batch_list"
```

### 2. Get access to your Batches via a Validator

```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_get_one_or_more_batches_of_data_from_a_configured_datasource.py get_validator"
```

### 3. Check your data

You can check that the `Batch(es)` that were loaded into your `Validator` are what you expect by running:
```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_get_one_or_more_batches_of_data_from_a_configured_datasource.py print(validator.batches)"
```

You can also check that the first few lines of the `Batch(es)` you loaded into your `Validator` are what you expect by running:

```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_get_one_or_more_batches_of_data_from_a_configured_datasource.py print(validator.head())"
```

Now that you have a `Validator`, you can use it to create `Expectations` or validate the data.


To view the full script used in this page, see it on GitHub:

- [how_to_get_one_or_more_batches_of_data_from_a_configured_datasource.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/how_to_get_one_or_more_batches_of_data_from_a_configured_datasource.py)
