---
title: Datasource
id: datasource
hoverText: Provides a standard API for accessing and interacting with data from a wide variety of source systems.
---
import TechnicalTag from '../term_tags/_tag.mdx';

A Datasource provides a standard API for accessing and interacting with data from a wide variety of source systems.

Datasources provide a standard API across multiple backends: the Datasource API remains the same for PostgreSQL, CSV Filesystems, and all other supported data backends.
:::note Important: 

Datasources do not modify your data.

:::

## Relationship to other objects

Datasources function by bringing together a way of interacting with Data (an <TechnicalTag relative="../" tag="execution_engine" text="Execution Engine" />) with a definition of the data to access (a Data Asset).  <TechnicalTag relative="../" tag="batch_request" text="Batch Requests" /> utilize a Datasources' Data Assets to return a <TechnicalTag relative="../" tag="batch" text="Batch" /> of data.

## Use Cases

When connecting to data the Datasource is your primary tool. At this stage, you will create Datasources to define how Great Expectations can find and access your <TechnicalTag relative="../" tag="data_asset" text="Data Assets" />.  Under the hood, each Datasource uses an Execution Engine (ex: SQLAlchemy, Pandas, and Spark) to connect to and query data. Once a Datasource is configured you will be able to operate with the Datasource's API rather than needing a different API for each possible data backend you may be working with.

When creating <TechnicalTag relative="../" tag="expectation" text="Expectations" /> you will use your Datasources to obtain <TechnicalTag relative="../" tag="batch" text="Batches" /> for <TechnicalTag relative="../" tag="profiler" text="Profilers" /> to analyze.  Datasources also provide Batches for  <TechnicalTag relative="../" tag="expectation_suite" text="Expectation Suites" />, such as when you use [the interactive workflow](../guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data.md) to create new Expectations.

Datasources are also used to obtain Batches for <TechnicalTag relative="../" tag="validator" text="Validators" /> to run against when you are validating data.

## Standard API

Datasources support connecting to a variety of different data backends. No matter which source data system you employ, the Datasource's API will remain the same.

## No unexpected modifications

Datasources do not modify your data during profiling or validation, but they may create temporary artifacts to optimize computing Metrics and Validation (this behavior can be configured).

## Create and access

Datasources can be created and accessed using Python code, which can be executed from a script, a Python console, or a Jupyter Notebook. To access a Datasource all you need is a <TechnicalTag relative="../" tag="data_context" text="Data Context" /> and the name of the Datasource. The below snippet shows how to create a Pandas Datasource for local files:

```python name="tests/integration/docusaurus/connecting_to_your_data/connect_to_your_data_overview add_datasource"
```

This next snippet shows how to retrieve the Datasource from the Data Context.

```python name="tests/integration/docusaurus/connecting_to_your_data/connect_to_your_data_overview config"
```

For detailed instructions on how to create Datasources that are configured for various backends, see [our documentation on Connecting to Data](../guides/connecting_to_your_data/index.md).

