---
title: Optional - Customize your deployment
---

At this point, you have your first, working local deployment of Great Expectations. You’ve also been introduced to the foundational concepts in the library: [Data Contexts](/docs/reference/data_context), [Datasources](/docs/reference/datasources), [Expectations](/docs/reference/expectations/expectations), [Profilers](/docs/reference/profilers), [Data Docs](/docs/reference/data_docs), [Validation](/docs/reference/validation), and [Checkpoints](/docs/reference/checkpoints_and_actions).

Congratulations! You’re off to a very good start.

The next step is to customize your deployment by upgrading specific components of your deployment. Data Contexts make this modular, so that you can add or swap out one component at a time. Most of these changes are quick, incremental steps—so you can upgrade from a basic demo deployment to a full production deployment at your own pace and be confident that your Data Context will continue to work at every step along the way.

This last section of this tutorial is designed to present you with clear options for upgrading your deployment. For specific implementation steps, please check out the linked How-to guides.

## Components

Here’s an overview of the components of a typical Great Expectations deployment:

* Great Expectations configs and metadata
  * [Options for storing Great Expectations configuration](/docs/tutorials/getting_started/customize_your_deployment#options-for-storing-great-expectations-configuration)
  * [Options for storing Expectations](/docs/tutorials/getting_started/customize_your_deployment#options-for-storing-expectations)
  * [Options for storing Validation Results](/docs/tutorials/getting_started/customize_your_deployment#options-for-storing-validation-results)
  * [Options for customizing generated notebooks](/docs/tutorials/getting_started/customize_your_deployment#options-for-customizing-generated-notebooks)

* Integrations to related systems
  * [Connecting to Data](/docs/tutorials/getting_started/customize_your_deployment#connecting-to-data)
  * [Options for hosting Data Docs](/docs/tutorials/getting_started/customize_your_deployment#options-for-hosting-data-docs)
  * [Additional Checkpoints and Actions](/docs/tutorials/getting_started/customize_your_deployment#additional-checkpoints-and-actions)
* [How to update Data Docs as a Validation Action](/docs/guides/validation/validation_actions/how_to_update_data_docs_as_a_validation_action)

## Options for storing Great Expectations configuration
The simplest way to manage your Great Expectations configuration is usually by committing great_expectations/great_expectations.yml to Git. However, it’s not usually a good idea to commit credentials to source control. In some situations, you might need to deploy without access to source control (or maybe even a file system).

Here’s how to handle each of those cases:

* [How to configure credentials](/docs/guides/setup/configuring_data_contexts/how_to_configure_credentials)
* [How to instantiate a Data Context without a yml file](/docs/guides/setup/configuring_data_contexts/how_to_instantiate_a_data_context_without_a_yml_file)

## Options for storing Expectations
Many teams find it convenient to store Expectations in Git. Essentially, this approach treats Expectations like test fixtures: they live adjacent to code and are stored within version control. Git acts as a collaboration tool and source of record.

Alternatively, you can treat Expectations like configs, and store them in a blob store. Finally, you can store them in a database.

* [How to configure an Expectation store in Amazon S3](/docs/guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_amazon_s3)
* [How to configure an Expectation store in GCS](/docs/guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs)
* [How to configure an Expectation store in Azure Blob Storage](/docs/guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_azure_blob_storage)
* [How to configure an Expectation store to PostgreSQL](/docs/guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_to_postgresql)
* [How to configure an Expectation store on a filesystem](/docs/guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_to_postgresql)

## Options for storing Validation Results
By default, Validation Results are stored locally, in an uncommitted directory. This is great for individual work, but not good for collaboration. The most common pattern is to use a cloud-based blob store such as S3, GCS, or Azure blob store. You can also store Validation Results in a database.

* [How to configure a Validation Result store on a filesystem](/docs/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_on_a_filesystem)
* [How to configure a Validation Result store in Amazon S3](/docs/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_amazon_s3)
* [How to configure a Validation Result store in GCS](/docs/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs)
* [How to configure a Validation Result store in Azure Blob Storage](/docs/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_azure_blob_storage)
* [How to configure a Validation Result store to PostgreSQL](/docs/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_to_postgresql)

## Options for customizing generated notebooks
Great Expectations generates and provides notebooks as interactive development environments for Expectation Suites. You might want to customize parts of the notebooks to add company-specific documentation, or change the code sections to suit your use-cases.

* [How to configure notebooks generated by “suite edit”](/docs/guides/miscellaneous/how_to_configure_notebooks_generated_by_suite_edit)

## Reference Architectures

* [How to instantiate a Data Context on an EMR Spark cluster](/docs/deployment_patterns/how_to_instantiate_a_data_context_on_an_emr_spark_cluster)
* [How to use Great Expectations in Databricks](/docs/deployment_patterns/how_to_use_great_expectations_in_databricks)

## Connecting to Data
Great Expectations allows you to connect to data in a wide variety of sources, and the list is constantly getting longer. If you have an idea for a source not listed here, please speak up in the public discussion forum.

* [How to connect to a Athena database](/docs/guides/connecting_to_your_data/database/athena)
* [How to connect to a BigQuery database](/docs/guides/connecting_to_your_data/database/bigquery)
* [How to connect to a MSSQL database](/docs/guides/connecting_to_your_data/database/mssql)
* [How to connect to a MySQL database](/docs/guides/connecting_to_your_data/database/mysql)
* [How to connect to a Postgres database](/docs/guides/connecting_to_your_data/database/postgres)
* [How to connect to a Redshift database](/docs/guides/connecting_to_your_data/database/redshift)
* [How to connect to a Snowflake database](/docs/guides/connecting_to_your_data/database/snowflake)
* [How to connect to a SQLite database](/docs/guides/connecting_to_your_data/database/sqlite)
* [How to connect to data on a filesystem using Spark](/docs/guides/connecting_to_your_data/filesystem/spark)
* [How to connect to data on S3 using Spark](/docs/guides/connecting_to_your_data/cloud/s3/spark)
* [How to connect to data on GCS using Spark](/docs/guides/connecting_to_your_data/cloud/gcs/spark)

## Options for hosting Data Docs
By default, Data Docs are stored locally, in an uncommitted directory. This is great for individual work, but not good for collaboration. A better pattern is usually to deploy to a cloud-based blob store (S3, GCS, or Azure Blob Storage), configured to share a static website.

* [How to host and share Data Docs on a filesystem](/docs/guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_a_filesystem)
* [How to host and share Data Docs on Azure Blob Storage](/docs/guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_azure_blob_storage)
* [How to host and share Data Docs on GCS](/docs/guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs)
* [How to host and share Data Docs on Amazon S3](/docs/guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_amazon_s3)

## Additional Checkpoints and Actions
Most teams will want to configure various Checkpoints and Validation Actions as part of their deployment. There are two primary patterns for deploying Checkpoints. Sometimes Checkpoints are executed during data processing (e.g. as a task within Airflow). From this vantage point, they can control program flow. Sometimes Checkpoints are executed against materialized data. Great Expectations supports both patterns. There are also some rare instances where you may want to validate data without using a Checkpoint.

* [How to trigger Slack notifications as a Validation Action](/docs/guides/validation/validation_actions/how_to_trigger_slack_notifications_as_a_validation_action)
* [How to trigger Opsgenie notifications as a Validation Action](/docs/guides/validation/validation_actions/how_to_trigger_opsgenie_notifications_as_a_validation_action)
* [How to trigger Email as a Validation Action](/docs/guides/validation/validation_actions/how_to_trigger_email_as_a_validation_action)
* [How to deploy a scheduled Checkpoint with cron](/docs/guides/validation/advanced/how_to_deploy_a_scheduled_checkpoint_with_cron)
* [How to implement custom notifications](/docs/guides/validation/advanced/how_to_implement_custom_notifications)
* [How to validate data without a Checkpoint](/docs/guides/validation/advanced/how_to_validate_data_without_a_checkpoint)
* [How to run a Checkpoint in Airflow](/docs/deployment_patterns/how_to_use_great_expectations_with_airflow)

## Not interested in managing your own configuration or infrastructure?
Learn more about Great Expectations Cloud — our fully managed SaaS offering. Sign up for [our weekly cloud workshop](https://greatexpectations.io/cloud)! You’ll get to see our newest features and apply for our private Alpha program!
