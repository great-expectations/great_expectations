---
title: Store
id: store
hoverText: A connector to store and retrieve information about metadata in Great Expectations.
---
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import TechnicalTag from '../term_tags/_tag.mdx';
import SetupHeader from '/docs/images/universal_map/_um_setup_header.mdx'
import CreateHeader from '/docs/images/universal_map/_um_create_header.mdx';
import ValidateHeader from '/docs/images/universal_map/_um_validate_header.mdx';


<UniversalMap setup='active' connect='inactive' create='active' validate='active'/> 

## Overview

### Definition

A Store is a connector to store and retrieve information about metadata in Great Expectations.

### Features and promises

Great Expectations supports a variety of Stores for different purposes, but the most common Stores are Expectation Stores, Validations Stores, Checkpoint Stores, and Evaluation Parameter Stores (or Metric Stores).  Data Docs Stores can also be configured for Data Doc Sites.  Each of these Stores is tailored to a specific type of information.

- [**Expectation Store:**](./expectation_store.md) a connector to store and retrieve information about collections of verifiable assertions about data.  These are Stores for Expectation Suites.
- [**Validation Result Store:**](./validation_result_store.md) a connector to store and retrieve information about objects generated when data is Validated against an Expectation Suite.
- [**Checkpoint Store:**](./checkpoint_store.md) a connector to store and retrieve information about means for validating data in a production deployment of Great Expectations.
- [**Evaluation Parameter Store:**](./evaluation_parameter_store.md) a connector to store and retrieve information about parameters used during Validation of an Expectation which reference simple expressions or previously generated Metrics.
- [**Data Docs Store:**](./data_docs_store.md) a connector to store and retrieve information pertaining to Human readable documentation generated from Great Expectations metadata detailing Expectations, Validation Results, etc.
- [**Metric Stores:**](./metric_store.md) a connector to store and retrieve information about computed attributes of data, such as the mean of a column.  These differ from the Evaluation Parameter Store in how they are formatted.  The data in a Metrics Store is intended to be used for generating reports and analyzing trends, rather than as Evaluation Parameter values.


### Relationship to other objects

Each type of Store is designed to interact with a specific subset of information, and thus interacts with a specific subset of objects in Great Expectations.  However, all Stores can be listed and modified through your Data Context.  For further information on how a given type of Store relates to other objects, please see the corresponding Store type's technical term page:

- Expectation Store technical term page: Relationship to other objects
- Checkpoint Store technical term page: Relationship to other objects
- Validation Result Store technical term page: Relationship to other objects
- Evaluation Parameter Store (or Metric Store) technical term page: Relationship to other objects
- Data Docs Store technical term page: Relationship to other objects
- Metric Store technical term page: Relationship to other objects

## Use cases

<SetupHeader/>

**All Stores** are configured during the Setup step of working with Great Expectations.  For the most part, the default configurations will permit you to work with Great Expectations in a local environment.  You can, however, configure your Stores to be hosted elsewhere.

<CreateHeader/>

When creating Expectations, you will use **Expectation Stores** to store your Expectation Suites.  You may also use **Validation Result Stores** or **Evaluation Parameter Stores** to configure some Expectations that require Evaluation Parameters as part of their definition, provided you have previously stored relevant Metrics in those Stores and then come back to create a new Expectation Suite that references them.

<ValidateHeader/>

When Validating data, you may store new Checkpoints (or retrieve existing ones) from your **Checkpoint Store**.  Checkpoints may also use Expectation Suites retrieved by an **Expectation Store**, and Expectations that are run by a Checkpoint may retrieve Metrics as input (Evaluation Parameters) from Validation Results in a **Validation Results Store**, or from values stored in an **Evaluation Parameter Store**.  Checkpoints may also write information about Validation Results into a **Validation Results Store**, or information about Metrics into an **Evaluation Parameter Store** (which is why they are also known as **Metric Stores**).

## Features

### Metadata storage and retrieval

Stores permit the configuration of where certain types of data and metadata will be stored and retrieved from.

### Centralized access and convenient API

Stores provide an API for Great Expectations to perform the retrieval and storage of information.  This API (e.g. the Stores themselves) is always available from your Data Context.

## API Basics

### How to access

You can find a list of almost all of your Stores in your `great_expectations.yml` file under the `stores` key.  Alternatively, from the root folder of your Data Context you can retrieve a list of Stores with a CLI command:

```markdown title="Console command"
great_expectations store list
```

Data Docs Stores are the one exception to the above.  They are instead configured under the `data_docs_sites` key in the `great_expectations.yml` file.  Each entry under `data_docs_sites` will have a `store_backend` key.  The information under `store_backend` will correspond to the Data Doc Store used by that site. Data Doc Stores are not listed by the `great_expectations store list` command.

### Configuration

Configuration of your Stores is located in your `great_expectations.yml` file under the `stores` key.  Three stores are required: an Expectation Store, a Validation Result Store, and an Evaluation Parameter Store must always have a valid entry in the `great_expectations.yml` file.  Additional Stores can be configured for uses such as Data Docs and Checkpoints (and a Checkpoint Store configuration entry will automatically be added if one does not exist when you attempt to save a Checkpoint for the first time.)

Note that unlike other Stores, your Data Docs Stores are configured under each individual site under the `data_docs_sites` key in the `great_expectations.yml` file.

## More details

For more information about a specific type of Store, please see the Store type's technical term definition page:

- <TechnicalTag relative="../" tag="expectation_store" text="Expectation Store technical term page" />
- <TechnicalTag relative="../" tag="checkpoint_store" text="Checkpoint Store technical term page" />
- <TechnicalTag relative="../" tag="validation_result_store" text="Validation Result Store technical term page" />
- <TechnicalTag relative="../" tag="evaluation_parameter_store" text="Evaluation Parameter Store technical term page" />
- <TechnicalTag relative="../" tag="data_docs_store" text="Data Docs Store technical term page" />
- <TechnicalTag relative="../" tag="metric_store" text="Metric Store technical term page" />













NOTES: TEMPORARY
--------------
Location where your Data Context stores information about your Expectations, Validation Results, and Metrics.
A connector to store and retrieve information about metadata in Great Expectations., such as Expectations, Validation Results, and Metrics.

By default, newly profiled Expectations are stored in JSON format in the `expectations/` subdirectory of your great_expectations/ folder.

By default, Validation results are stored in JSON format in the uncommitted/validations/ subdirectory of your great_expectations/ folder. Since Validations may include examples of data (which could be sensitive or regulated) they should not be committed to a source control system.