---
title: Deploying Great Expectations in a hosted environment without file system or CLI
---

If you follow the steps of the [Getting Started](../tutorials/getting_started/intro) tutorial, you create a standard deployment of Great Expectations. By default, this relies on two components:

#. The Great Expectations [CLI](../guides/miscellaneous/how_to_use_the_great_expectations_cli) to initialize a Data Context, create Expectation Suites, add Datasources, etc.
#. The ``great_expectations.yml`` file to configure your Data Context, e.g. to point at different Stores for validation results, etc.


However, you might not have these components available in hosted environments, such as [Databricks](./how_to_instantiate_a_data_context_on_databricks_spark_cluster), [AWS EMR](./how_to_instantiate_a_data_context_on_an_emr_spark_cluster), Google Cloud Composer, and others. This workflow guide will outline the main steps required to successfully use Great Expectations in a hosted environment.


Step 1: Configure your Data Context
-------------------------------------
 Instead of using the Great Expectations CLI, you can create a Data Context directly in code. Your Data Context also manages the following components described in this guide:

- Datasources to connect to data
- Stores to save Expectations and validation results
- Data Docs hosting

The following guide gives an overview of creating an in-code Data Context including defaults to help you more quickly set one up for common configurations:

- [How to instantiate a DataContext without a YML file](../guides/setup/configuring_data_contexts/how_to_instantiate_a_data_context_without_a_yml_file)

The following guides will contain examples for each environment we have tested out:

- [AWS EMR](./how_to_instantiate_a_data_context_on_an_emr_spark_cluster)
- [Databricks](./how_to_instantiate_a_data_context_on_databricks_spark_cluster)
- :ref:`deployment_google_cloud_composer` <TODO: Convert reference to "Google Cloud Composer" to docusaurus format.>
- :ref:`deployment_astronomer` <TODO: Convert reference to "Astronomer" to docusaurus format.>


Step 2: Create Expectation Suites and add Expectations
-------------------------------------------------------

If you want to create an Expectation Suite in your environment without using the CLI, you can simply follow this guide: [TODO: Add content and reference to "How to Create a New Expectation Suite Without the CLI"].

In order to store your Expectation Suites so you can load them for validation at a later point, you will need to ensure that you have an Expectation Store configured: [TODO: Add content and reference to "Configuring Metadata Stores"].

Step 3: Run validation
--------------------------------

In order to use an Expectation Suite you've created to validate data, follow this guide: [How to validate data without a Checkpoint](../guides/validation/how_to_validate_data_without_a_checkpoint).

Step 4: Use Data Docs
----------------------

Finally, if you would like to build and view Data Docs in your environment, please follow the guides for configuring Data Docs: [Options for hosting Data Docs](../tutorials/getting_started/customize_your_deployment#options_for_hosting_data_docs).

Additional notes
----------------

If you have successfully deployed Great Expectations in a hosted environment other than the ones listed above, we would love to hear from you. Please comment on the Discuss post below, or reach out to us on [Slack](https://greatexpectations.io/slack).
