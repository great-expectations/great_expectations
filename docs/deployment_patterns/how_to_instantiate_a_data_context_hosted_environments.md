---
title: Deploying Great Expectations in a hosted environment without file system or CLI
---

If you follow the steps of the [Getting Started](../tutorials/getting_started/intro.md) tutorial, you create a standard deployment of Great Expectations. By default, this relies on two components:

1. The Great Expectations [CLI](../guides/miscellaneous/how_to_use_the_great_expectations_cli.md) to initialize a Data Context, create Expectation Suites, add Datasources, etc.
2. The ``great_expectations.yml`` file to configure your Data Context, e.g. to point at different Stores for Validation Results, etc.


However, you might not have these components available in hosted environments, such as Databricks, AWS EMR, Google Cloud Composer, and others. This workflow guide will outline the main steps required to successfully use Great Expectations in a hosted environment.


Step 1: Configure your Data Context
-------------------------------------
 Instead of using the Great Expectations CLI, you can create a Data Context directly in code. Your Data Context also manages the following components described in this guide:

- Datasources to connect to data
- Stores to save Expectations and Validation Results
- Data Docs hosting

The following guide gives an overview of creating an in-code Data Context including defaults to help you more quickly set one up for common configurations:

- [How to instantiate a DataContext without a YML file](../guides/setup/configuring_data_contexts/how_to_instantiate_a_data_context_without_a_yml_file.md)

The following guides will contain examples for each environment we have tested out:

- [How to instantiate a Data Context on an EMR Spark cluster](./how_to_instantiate_a_data_context_on_an_emr_spark_cluster.md)
- [How to use Great Expectations in Databricks](/docs/deployment_patterns/how_to_use_great_expectations_in_databricks)

Step 2: Create Expectation Suites and add Expectations
-------------------------------------------------------

If you want to create an Expectation Suite in your environment without using the CLI, you can follow this guide from step 5 onward to add a Datasource and an Expectation Suite: [How to connect to a PostgreSQL database](/docs/guides/connecting_to_your_data/database/postgres/#5-configure-your-datasource)

You can then add Expectations to your Suite one at a time like this example:

```
validator.expect_column_values_to_not_be_null("my_column")
validator.save_expectation_suite(discard_failed_expectations=False)
```

In order to load the Suite at a later time, you will need to ensure that you have an Expectation store configured:

- [How to configure an Expectation store to use Amazon S3](../guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_amazon_s3.md)
- [How to configure an Expectation store to use Azure Blob Storage](../guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_azure_blob_storage.md)
- [How to configure an Expectation store to use GCS](../guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.md)
- [How to configure an Expectation store to use a filesystem](../guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_on_a_filesystem.md)
- [How to configure an Expectation store to use PostgreSQL](../guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_to_postgresql.md)

Step 3: Run validation
--------------------------------

In order to use an Expectation Suite you've created to validate data, follow this guide: [How to validate data without a Checkpoint](../guides/validation/advanced/how_to_validate_data_without_a_checkpoint.md)

Step 4: Use Data Docs
----------------------

Finally, if you would like to build and view Data Docs in your environment, please follow the guides for configuring Data Docs: [Options for hosting Data Docs](../tutorials/getting_started/customize_your_deployment.md#options-for-hosting-data-docs)

Additional notes
----------------

If you have successfully deployed Great Expectations in a hosted environment other than the ones listed above, we would love to hear from you. Please reach out to us on [Slack](https://greatexpectations.io/slack)
