---
title: "Data Docs Store"
---
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import TechnicalTag from '../term_tags/_tag.mdx';
import SetupHeader from '/docs/images/universal_map/_um_setup_header.mdx'
import CreateHeader from '/docs/images/universal_map/_um_create_header.mdx';
import ValidateHeader from '/docs/images/universal_map/_um_validate_header.mdx';


<UniversalMap setup='active' connect='active' create='active' validate='active'/> 

## Overview

### Definition

A Data Docs Store is a connector to store and retrieve information pertaining to human-readable documentation generated from Great Expectations metadata detailing Expectations, Validation Results, etc.

### Features and promises

The Data Docs Store provides an easy way to configure where and how to have your <TechnicalTag relative="../" tag="data_docs" text="Data Docs" /> rendered.

### Relationship to other objects

Your Data Docs Store will be used behind the scenes by any Action that updates your Data Docs.  Great Expectations includes the `UpdateDataDocsAction` subclass of the `ValidationAction` class for this express purpose.  Including the `UpdateDataDocsAction` in the `action_list` of a <TechnicalTag relative="../" tag="checkpoint" text="Checkpoint" /> will cause your Data Docs to be updated accordingly.

## Use cases

<SetupHeader/>

When you configure your Data Docs, one of the items you will need to include is a `data_docs_store_backend` for each entry in the `data_docs_sites` key located in the `great_expectations.yml` file.  This will provide Great Expectations with the necessary information to access the Data Docs being rendered for each given site.  That process will generally be handled behind the scenes; after configuring your Data Docs sites to include a Data Docs Store, you will generally not have to directly interact with the Data Docs Store yourself.

<CreateHeader/>

When <TechnicalTag relative="../" tag="profiler" text="Profilers" /> are run to create <TechnicalTag relative="../" tag="expectation" text="Expectations" /> their results are made available through Data Docs.  Additionally, most Profiler workflows include a step where the Profiler's generated <TechnicalTag relative="../" tag="expectation_suite" text="Expectation Suite" /> is <TechnicalTag relative="../" tag="validation" text="Validated" /> against a <TechnicalTag relative="../" tag="batch" text="Batch" /> of data.  These processes use a Data Docs Store behind the scenes.

<ValidateHeader/>

When <TechnicalTag relative="../" tag="checkpoint" text="Checkpoints" /> are run to Validate data, their results are generally updated into Data Docs.  Any Checkpoint that is configured to do so will use a Data Docs Store behind the scenes.  Including the `UpdateDataDocsAction` in the `action_list` of a Checkpoint will cause your Data Docs to be updated accordingly.  This update will only include the <TechnicalTag relative="../" tag="validation_result" text="Validation Results" /> from the Checkpoint itself and will not re-render all of your existing Data Docs.  If a Checkpoint does not include an `UpdateDataDocsAction` the Validation Results will not be rendered into Data Docs.


## Features

### Ease of use

Once your Data Docs sites are configured with a Data Docs Store backend, the Great Expectations Objects that access Data Docs will handle the rest: you won't have to directly interact with the Data Docs Store.

## API basics

### How to access

Your Data Context and other objects that interact with Data Docs will access your Data Docs Stores behind the scenes without you needing to directly manage them yourself beyond configuring your Data Docs sites during Setup.  

In the Validate Data step, including `UpdateDataDocsAction` in the `action_list` of a Checkpoint will cause your Data Docs to be updated with the Checkpoint's Validation Results; this process will use your Data Docs Stores behind the scenes.

:::note
- To ensure that the Validation Results are included in the updated Data Docs, `UpdateDataDocsAction` should be present *after* `StoreValidationResultAction` in the Checkpoint's `action_list`.
:::

You can also render your Data Docs outside a Checkpoint by utilizing the `build_data_docs()` method of your Data Context.  This will re-render all of your Data Docs according to the contents of your Data Docs Store.

### Configuration

Unlike other Stores, Data Docs stores are configured in the `store_backend` section under `data_doc_sites` in the `great_expectations.yml` config file.  For detailed information on how to configure a Data Docs Store for a given backend, please see the corresponding how-to guide:

- [How to host and share Data Docs on a filesystem](../guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_a_filesystem.md)
- [How to host and share Data Docs on Azure Blob Storage](../guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_azure_blob_storage.md)
- [How to host and share Data Docs on GCS](../guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.md)
- [How to host and share Data Docs on Amazon S3](../guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_amazon_s3.md)
