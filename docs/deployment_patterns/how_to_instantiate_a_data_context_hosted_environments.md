---
title: Deploying Great Expectations in a hosted environment without file system or CLI
---

If you follow the steps of the [Getting Started](../tutorials/getting_started/intro) tutorial, you create a standard deployment of Great Expectations. By default, this relies on two components:

1. The Great Expectations [CLI](../guides/miscellaneous/how_to_use_the_great_expectations_cli) to initialize a Data Context, create Expectation Suites, add Datasources, etc.
2. The ``great_expectations.yml`` file to configure your Data Context, e.g. to point at different Stores for Validation Results, etc.


However, you might not have these components available in hosted environments, such as Databricks, AWS EMR, Google Cloud Composer, and others. This workflow guide will outline the main steps required to successfully use Great Expectations in a hosted environment.


Step 1: Configure your Data Context
-------------------------------------
 Instead of using the Great Expectations CLI, you can create a Data Context directly in code. Your Data Context also manages the following components described in this guide:

- Datasources to connect to data
- Stores to save Expectations and Validation Results
- Data Docs hosting

The following guide gives an overview of creating an in-code Data Context including defaults to help you more quickly set one up for common configurations:

- [How to instantiate a DataContext without a YML file](../guides/setup/configuring_data_contexts/how_to_instantiate_a_data_context_without_a_yml_file)

The following guides will contain examples for each environment we have tested out:

- [How to instantiate a Data Context on an EMR Spark cluster](./how_to_instantiate_a_data_context_on_an_emr_spark_cluster)
- [How to instantiate a Data Context on Databricks Spark cluster](./how_to_instantiate_a_data_context_on_databricks_spark_cluster)
- [Deploying Great Expectations with Google Cloud Composer (Hosted Airflow)](./)
- [Deploying Great Expectations with Astronomer](./)


Step 2: Create Expectation Suites and add Expectations
-------------------------------------------------------

If you want to create an Expectation Suite in your environment without using the CLI, you can simply follow this guide: [How to Create a New Expectation Suite Without the CLI](./).

In order to store your Expectation Suites so you can load them for validation at a later point, you will need to ensure that you have an Expectation Store configured: [Configuring Metadata Stores](./).

Step 3: Run validation
--------------------------------

In order to use an Expectation Suite you've created to validate data, follow this guide: [How to validate data without a Checkpoint](../guides/validation/advanced/how_to_validate_data_without_a_checkpoint).

Step 4: Use Data Docs
----------------------

Finally, if you would like to build and view Data Docs in your environment, please follow the guides for configuring Data Docs: [Options for hosting Data Docs](../tutorials/getting_started/customize_your_deployment#options-for-hosting-data-docs).

Additional notes
----------------

If you have successfully deployed Great Expectations in a hosted environment other than the ones listed above, we would love to hear from you. Please comment on the Discuss post below, or reach out to us on [Slack](https://greatexpectations.io/slack).
