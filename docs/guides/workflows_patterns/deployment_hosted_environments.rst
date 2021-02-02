.. _deployment_hosted_enviroments:

#############################################################################################
Deploying Great Expectations in a hosted environment without file system or CLI
#############################################################################################

If you follow the steps of the :ref:`Getting started tutorial <tutorials__getting_started>`, you create a standard deployment of Great Expectations. By default, this relies on two components:

#. The Great Expectations :ref:`command-line interface (CLI)<command_line>` to initialize a Data Context, create Expectation Suites, add Datasources, etc.
#. The ``great_expectations.yml`` file to configure your Data Context, e.g. to point at different Stores for validation results, etc.


However, you might not have these components available in hosted environments, such as Databricks, AWS EMR, Google Cloud Composer, and others. This workflow guide will outline the main steps required to successfully use Great Expectations in a hosted environment.


Step 1: Configure your Data Context
-------------------------------------
 Instead of using the Great Expectations CLI, you can create a Data Context directly in code. Your Data Context also manages the following components described in this guide:

- Datasources to connect to data
- Stores to save Expectations and validation results
- Data Docs hosting

The following guide gives an overview of creating an in-code Data Context including defaults to help you more quickly set one up for common configurations:

- :ref:`how_to_guides__configuring_data_contexts__how_to_instantiate_a_data_context_without_a_yml_file`

The following guides will contain examples for each environment we have tested out:

- :ref:`how_to_instantiate_a_data_context_on_an_emr_spark_cluster`
- :ref:`how_to_instantiate_a_data_context_on_a_databricks_spark_cluster`
- :ref:`deployment_google_cloud_composer`
- :ref:`deployment_astronomer`


Step 2: Create Expectation Suites and add Expectations
-------------------------------------------------------

If you want to create an Expectation Suite in your environment without using the CLI, you can simply follow this guide: :ref:`how_to_guides__creating_and_editing_expectations__how_to_create_a_new_expectation_suite_without_the_cli`.

In order to store your Expectation Suites so you can load them for validation at a later point, you will need to ensure that you have an Expectation Store configured: :ref:`how_to_guides__configuring_metadata_stores`.

Step 3: Run validation
--------------------------------

In order to use an Expectation Suite you've created to validate data, follow this guide: :ref:`how_to_guides__validation__how_to_validate_data_without_a_checkpoint`

Step 4: Use Data Docs
----------------------

Finally, if you would like to build and view Data Docs in your environment, please follow the guides for configuring Data Docs: :ref:`how_to_guides__configuring_data_docs`.

Additional notes
----------------

If you have successfully deployed Great Expectations in a hosted environment other than the ones listed above, we would love to hear from you. Please comment on the Discuss post below, or reach out to us on `Slack <https://greatexpectations.io/slack>`_.

.. discourse::
   :topic_identifier: 395
