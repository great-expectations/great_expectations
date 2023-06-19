---
title: "Deploy Great Expectations in hosted environments without a file system"
sidebar_label: "Deploy Great Expectations in hosted environments without a file system"
description: "Use Great Expectations in hosted environments."
id: how_to_instantiate_a_data_context_hosted_environments
sidebar_custom_props: { icon: 'img/integrations/aws-icon.png' }
---

The components in the ``great_expectations.yml`` file define the Validation Results Stores, Datasource connections, and Data Docs hosts for a Data Context. These components might be inaccessible in hosted environments, such as Databricks, Amazon EMR, and Google Cloud Composer. The information provided here is intended to help you use Great Expectations in hosted environments.

## Configure your Data Context
 
To use code to create a Data Context, see [How to instantiate an Ephemeral Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_explicitly_instantiate_an_ephemeral_data_context).

To configure a Data Context for a specific environment, see one of the following resources:

- [How to instantiate a Data Context on an EMR Spark cluster](./how_to_instantiate_a_data_context_on_an_emr_spark_cluster.md)
- [How to use Great Expectations in Databricks](./how_to_use_great_expectations_in_databricks.md)

## Create Expectation Suites and add Expectations

To add a Datasource and an Expectation Suite, see [How to connect to a PostgreSQL database](/docs/0.15.50/guides/connecting_to_your_data/database/postgres#5-configure-your-datasource).

To add Expectations to your Suite individually, use the following code:

```
validator.expect_column_values_to_not_be_null("my_column")
validator.save_expectation_suite(discard_failed_expectations=False)
```

To configure your Expectation store to load a Suite at a later time, see one of the following resources:

- [How to configure an Expectation store to use Amazon S3](../guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_amazon_s3.md)
- [How to configure an Expectation store to use Azure Blob Storage](../guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_azure_blob_storage.md)
- [How to configure an Expectation store to use GCS](../guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.md)
- [How to configure an Expectation store to use a filesystem](../guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_on_a_filesystem.md)
- [How to configure an Expectation store to use PostgreSQL](../guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_to_postgresql.md)

## Run validation

To use an Expectation Suite you've created to validate data, see [How to validate data without a Checkpoint](../guides/validation/advanced/how_to_validate_data_without_a_checkpoint.md).

## Use Data Docs

To build and view Data Docs in your environment, see [Options for hosting Data Docs](../reference/customize_your_deployment.md#options-for-hosting-data-docs).
