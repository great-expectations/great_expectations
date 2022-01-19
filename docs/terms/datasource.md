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

A Datasource provides a standard API for accessing and interacting with data from a wide variety of source systems.

Datasources provide a unified API across multiple backends: the Datasource API remains the same for PostgreSQL, MongoDB, CSV Filesystems, and all other supported data backends.  Datasources also permit CRUD (Create/Read/Update/Delete) operations on your data.

Datasources function by bringing together a way of interacting with Data (an <TechnicalTag relative="../" tag="execution_engine" text="Execution Engine" />) with a way of accessing that data (a <TechnicalTag relative="../" tag="data_connector" text="Data Connector." />).

## Use Cases

<ConnectHeader/>

When connecting to data the Datasource is your go-to.  At this stage, you will create Datasources to define how Great Expectations can find and access your <TechnicalTag relative="../" tag="data_asset" text="Data Assets." />.  From that point forward you will be able to operate with the Datasource's API rather than needing a different API for each possible data backend you may be working with.

<CreateHeader/>

When creating <TechnicalTag relative="../" tag="expectation" text="Expectations" />, you will use your Datasources to obtain <TechnicalTag relative="../" tag="batch" text="Batches" /> for <TechnicalTag relative="../" tag="profiler" text="Profilers" /> to analyze.

<ValidateHeader/>

Datasources are also used to obtain Batches for <TechnicalTag relative="../" tag="expectation_suite" text="Expectation Suites" /> and <TechnicalTag relative="../" tag="validator" text="Validators" /> to run against when you are validating data.

## Features

### Unified API

Datasources support connecting to a variety of different data backends.  No matter which source data system you employ, the Datasource's API will remain the same.

### CRUD Operations


## Creating a Datasource

Creating a Datasource is quick and easy, and can be done from the <TechnicalTag relative="../" tag="cli" text="CLI" /> or through Python code.  Configuring the Datasource may differ between backends, according to the given backend's requirements, but the process of creating one is the same.

For detailed instructions on how to create Datasources for various backends, see our documentation on Connecting to Data.

## Accessing your Datasource

You will typically only access your Datasource directly through Python code, which can be executed from a script, a Python console, or a Jupyter Notebook.  To access a Datasource all you need is a <TechnicalTag relative="../" tag="data_context" text="Data Context." /> and the name of the Datasource you want to access, as shown below:

```python title="Python console:"
import great_expectations as ge

context = ge.get_context()
datasource = context.get_datasource("my_datasource_name")
```

<!---

NOTES: TEMPORARY
-----------------
Provides a unified API for using a Data Connector and an Execution Engine to access data, regardless of the native methods required to access the data associated with the Data Connector.
Brings together a way of interacting with data (an Execution Engine) and a way of accessing that data (a Data Connector). Data Assets live within a Datasource; Datasources are used to obtain Batches for Validators, Expectation Suites, and Profilers.

-->
