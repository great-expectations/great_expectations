---
title: "Technical Term"
---
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import TechnicalTag from '../term_tags/_tag.mdx';
import ConnectHeader from '/docs/images/universal_map/_um_connect_header.mdx';
import CreateHeader from '/docs/images/universal_map/_um_create_header.mdx';
import ValidateHeader from '/docs/images/universal_map/_um_validate_header.mdx';


<UniversalMap setup='inactive' connect='active' create='active' validate='active'/> 

## Overview

### Definition

A Data Connector provides the configuration details based on the source data system which are needed by a <TechnicalTag relative="../" tag="datasource" text="Datasource" /> to define <TechnicalTag relative="../" tag="data_asset" text="Data Assets" />.

### Features and promises

A Data Connector facilitates access to an external source data system, such as a database, filesystem, or cloud storage. The Data Connector can inspect an external source data system to:
- identify available Batches
- build Batch Definitions using Batch Identifiers
- translate Batch Definitions to Execution Engine-specific Batch Specs

### Relationship to other objects

A Data Connector is an integral element of a Datasource.  When a Batch Request is passed to a Datasource, the Datasource's <TechnicalTag relative="../" tag="execution_engine" text="Execution Engine" /> will pass the <TechnicalTag relative="../" tag="batch_request" text="Batch Request" /> to its Data Connector, which will then query the source data system it is configured for.  This will result in the return of a <TechnicalTag relative="../" tag="batch" text="Batch" /> of data.

Data Connectors provide Batches to <TechnicalTag relative="../" tag="expectation_suite" text="Expectation Suites" />, <TechnicalTag relative="../" tag="profiler" text="Profilers" />, and <TechnicalTag relative="../" tag="checkpoint" text="Checkpoints" />.

## Use cases

<ConnectHeader/>

The only time when you will need to explicitly work with a Data Connector is when you specify one in the configuration of a Datasource.  There are several types of Data Connectors in Great Expectations, such as the `ConfiguredAssetFilesystemDataConnector`, `DatabaseDataConnector`, and `RuntimeDataConnector`.

Each Data Connector holds configuration for connecting to a different type of external data source, and can connect to and inspect that data source.

**For example**, a `ConfiguredAssetFilesystemDataConnector` could be configured with the root directory for files on a filesystem or bucket and prefix used to access files from a cloud storage environment. In contrast, the simplest `RuntimeDataConnector` may simply store lookup information about Data Assets to facilitate running in a pipeline where you already have a DataFrame in memory or available in a cluster.

In addition to those examples, Great Expectations makes it possible to configure Data Connectors that offer stronger guarantees about reproducibility, sampling, and compatibility with other tools.

<CreateHeader/>

When creating Expectations, Datasources will use their Data Connectors behind the scenes as part of the process of providing Batches to Expectation Suites and Profilers.

<ValidateHeader/>

Likewise, when validating Data, Datasources will use their Data Connectors behind the scenes as part of the process of providing Batches to Checkpoints.

## Features

### Identifying Batches and building Batch References

To maintain the guarantees for the relationships between Batches and Batch Requests, Data Connectors provide configuration options that allow them to divide Data Assets into different Batches of data, which Batch Requests reference in order to specify Batches for retrieval. We use the term "Data Reference" below to describe a general pointer to data, like a filesystem path or database view. Batch Identifiers then define a conversion process:

1. Convert a Data Reference to a Batch Request
2. Convert a Batch Request back into a Data Reference (or Wildcard Data Reference, when searching)

The main thing that makes dividing Data Assets into Batches complicated is that converting from a Batch Request to a
Data Reference can be lossy.

It’s pretty easy to construct examples where no regex can reasonably capture enough information to allow lossless
conversion from a Batch Request to a unique Data Reference:

#### Example 1

For example, imagine a daily logfile that includes a random hash:

`YYYY/MM/DD/log-file-[random_hash].txt.gz`

The regex for this naming convention would be something like:

`(\d{4})/(\d{2})/(\d{2})/log-file-.*\.txt\.gz`

with capturing groups for YYYY, MM, and DD, and a non-capturing group for the random hash.

As a result, the Batch Identifiers keys will be Y, M, D. Given specific Batch Identifiers:

```python
{
    "Y" : 2020,
    "M" : 10,
    "D" : 5
}
```

we can reconstruct *part* of the filename, but not the whole thing:

`2020/10/15/log-file-[????].txt.gz`

#### Example 2

A slightly more subtle example: imagine a logfile that is generated daily at about the same time, but includes the exact
time stamp when the file was created.

`log-file-YYYYMMDD-HHMMSS.ssssssss.txt.gz`

The regex for this naming convention would be something like

`log-file-(\d{4})(\d{2})(\d{2})-.*\..*\.txt\.gz`

With capturing groups for YYYY, MM, and DD, but not the HHMMSS.sssssss part of the string. Again, we can only specify
part of the filename:

`log-file-20201015-??????.????????.txt.gz`

#### Example 3

Finally, imagine an S3 bucket with log files like so:

`s3://some_bucket/YYYY/MM/DD/log_file_YYYYMMDD.txt.gz`

In that case, the user would probably specify regex capture groups with something
like `some_bucket/(\d{4})/(\d{2})/(\d{2})/log_file_\d+.txt.gz`.


The Wildcard Data Reference is how Data Connectors deal with that problem, making it easy to search external stores and understand data.

Under the hood, when processing a Batch Request, the Data Connector may find multiple matching Batches. Generally, the Data Connector will simply return a list of all matching Batch Identifiers.

### Translating Batch Definitions to Batch Specs

A **Batch Definition** includes all the information required to precisely identify a set of data in a source data system.

A **Batch Spec** is an Execution Engine-specific description of the Batch defined by a Batch Definition.

A Data Connector is responsible for working with an Execution Engine to translate Batch Definitions into a Batch Spec that enables Great Expectations to access the data using that Execution Engine.



## API basics

:::info API note
In the updated V3 Great Expectations API, Data Connectors replace the Batch Kwargs Generators from the V2 Great Expectations API.
:::

### How to access

Other than specifying a Data Connector when you configure a Datasource, you will not need to directly interact with one.  Great Expectations will handle using them behind the scenes.

### How to create

Data Connectors are automatically created when a Datasource is initialized, based on the Datasource's configuration.  

For a general overview of this process, please see [our documentation on configuring your Datasource's Data Connectors](../guides/connecting_to_your_data/connect_to_data_overview.md#configuring-your-datasources-data-connectors).

### Configuration

A Data Connector is configured as part of a Datasource's configuration.  The specifics of this configuration can vary depending on the requirements for connecting to the source data system that the Data Connector is intended to interface with.  For example, this might be a path to files that might be loaded into the Pandas Execution Engine, or the connection details for a database to be used by the SQL Alchemy Execution Engine.

For specific guidance on how to configure a Data Connector for a given source data system, please see [our how-to guides on connecting to data](../guides/connecting_to_your_data/index.md).
