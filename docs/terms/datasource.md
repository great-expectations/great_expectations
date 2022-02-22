---
title: Datasource
id: datasource
hoverText: Provides a standard API for accessing and interacting with data from a wide variety of source systems.
---

import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import ConnectHeader from '/docs/images/universal_map/_um_connect_header.mdx';
import CreateHeader from '/docs/images/universal_map/_um_create_header.mdx';
import ValidateHeader from '/docs/images/universal_map/_um_validate_header.mdx';
import TechnicalTag from '../term_tags/_tag.mdx';

<UniversalMap setup='inactive' connect='active' create='active' validate='active'/>

### Definition

A Datasource provides a standard API for accessing and interacting with data from a wide variety of source systems.

### Features and promises

Datasources provide a unified API across multiple backends: the Datasource API remains the same for PostgreSQL, CSV Filesystems, and all other supported data backends.  
:::note Important: 

Datasources do not modify your data.

:::

### Relationship to other objects

Datasources function by bringing together a way of interacting with Data (an <TechnicalTag relative="../" tag="execution_engine" text="Execution Engine" />) with a way of accessing that data (a <TechnicalTag relative="../" tag="data_connector" text="Data Connector." />).  <TechnicalTag relative="../" tag="batch_request" text="Batch Requests" /> utilize Datasources in order to return a <TechnicalTag relative="../" tag="batch" text="Batches" /> of data.

## Use Cases

<ConnectHeader/>

When connecting to data the Datasource is primary tool.  At this stage, you will create Datasources to define how Great Expectations can find and access your <TechnicalTag relative="../" tag="data_asset" text="Data Assets" />.  Under the hood, each Datasource must have an Execution Engine and one or more Data Connectors configured.  Once a Datasource is configured you will be able to operate with the Datasource's API rather than needing a different API for each possible data backend you may be working with.

<CreateHeader/>

When creating <TechnicalTag relative="../" tag="expectation" text="Expectations" /> you will use your Datasources to obtain <TechnicalTag relative="../" tag="batch" text="Batches" /> for <TechnicalTag relative="../" tag="profiler" text="Profilers" /> to analyze.  Datasources also provide Batches for  <TechnicalTag relative="../" tag="expectation_suite" text="Expectation Suites" />, such as when you use [the interactive workflow](../guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data.md) to create new Expectations.

<ValidateHeader/>

Datasources are also used to obtain Batches for <TechnicalTag relative="../" tag="validator" text="Validators" /> to run against when you are validating data.

## Features

### Unified API

Datasources support connecting to a variety of different data backends.  No matter which source data system you employ, the Datasource's API will remain the same.

### No Unexpected Modifications

Datasources do not modify your data during profiling or validation, but they may create temporary artifacts to optimize computing Metrics and Validation.  This behaviour can be configured at the Data Connector level.

### API Basics

### How to access

You will typically only access your Datasource directly through Python code, which can be executed from a script, a Python console, or a Jupyter Notebook.  To access a Datasource all you need is a <TechnicalTag relative="../" tag="data_context" text="Data Context." /> and the name of the Datasource you want to access, as shown below:

```python title="Python console:"
import great_expectations as ge

context = ge.get_context()
datasource = context.get_datasource("my_datasource_name")
```

### How to create and configure

Creating a Datasource is quick and easy, and can be done from the <TechnicalTag relative="../" tag="cli" text="CLI" /> or through Python code.  Configuring the Datasource may differ between backends, according to the given backend's requirements, but the process of creating one will remain the same.

To create a new  Datasource through the CLI, run `great_expectations datasource new`.

To create a new Datasource through Python code, obtain a data context and call its `add_datasource` method.

Advanced users may also create a Datasource directly through a YAML config file.

For detailed instructions on how to create Datasources that are configured for various backends, see [our documentation on Connecting to Data](../guides/connecting_to_your_data/index.md).

