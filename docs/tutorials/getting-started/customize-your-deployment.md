---
title: Optional - Customize your deployment
---

At this point, you have your first, working local deployment of Great Expectations. You’ve also been introduced to the foundational concepts in the library: [Data Contexts](/docs/reference/data-context), [Datasources](/docs/reference/datasources), [Expectations](/docs/reference/expectations/expectations), [Profilers](/docs/reference/profilers), [Data Docs](/docs/reference/data-docs), [Validation](/docs/reference/validation), and [Checkpoints](/docs/reference/checkpoints-and-actions).

Congratulations! You’re off to a very good start.

The next step is to customize your deployment by upgrading specific components of your deployment. Data Contexts make this modular, so that you can add or swap out one component at a time. Most of these changes are quick, incremental steps—so you can upgrade from a basic demo deployment to a full production deployment at your own pace and be confident that your Data Context will continue to work at every step along the way.

This last section of this tutorial is designed to present you with clear options for upgrading your deployment. For specific implementation steps, please check out the linked How-to guides.

## Components

Here’s an overview of the components of a typical Great Expectations deployment:

* Great Expectations configs and metadata
  * [Options for storing Great Expectations configuration](/docs/tutorials/getting-started/customize-your-deployment#options-for-storing-great-expectations-configuration)
  * [Options for storing Expectations](/docs/tutorials/getting-started/customize-your-deployment#options-for-storing-expectations)
  * [Options for storing Validation Results](/docs/tutorials/getting-started/customize-your-deployment#options-for-storing-validation-results)
  * [Options for customizing generated notebooks](/docs/tutorials/getting-started/customize-your-deployment#options-for-customizing-generated-notebooks)

* Integrations to related systems
  * [Connecting to Data](/docs/tutorials/getting-started/customize-your-deployment#connecting-to-data)
  * [Options for hosting Data Docs](/docs/tutorials/getting-started/customize-your-deployment#options-for-hosting-data-docs)
  * [Additional Checkpoints and Actions](/docs/tutorials/getting-started/customize-your-deployment#additional-checkpoints-and-actions)

## Options for storing Great Expectations configuration
The simplest way to manage your Great Expectations configuration is usually by committing great_expectations/great_expectations.yml to git. However, it’s not usually a good idea to commit credentials to source control. In some situations, you might need to deploy without access to source control (or maybe even a file system).

Here’s how to handle each of those cases:

* [How to use a YAML file or environment variables to populate credentials](/docs/guides/setup/configuring-data-contexts/how-to-configure-credentials-using-a-yaml-file-or-environment-variables)
* [How to populate credentials from a secrets store](/docs/guides/setup/configuring-data-contexts/how-to-configure-credentials-using-a-secrets-store)
* [How to instantiate a Data Context without a yml file](/docs/guides/setup/configuring-data-contexts/how-to-instantiate-a-data-context-without-a-yml-file)

## Options for storing Expectations
Many teams find it convenient to store Expectations in git. Essentially, this approach treats Expectations like test fixtures: they live adjacent to code and are stored within version control. git acts as a collaboration tool and source of record.

Alternatively, you can treat Expectations like configs, and store them in a blob store. Finally, you can store them in a database.

* [How to configure an Expectation store in Amazon S3](/docs/guides/setup/configuring-metadata-stores/how-to-configure-an-expectation-store-in-amazon-s3)
* [How to configure an Expectation store in GCS](/docs/guides/setup/configuring-metadata-stores/how-to-configure-an-expectation-store-in-gcs)
* [How to configure an Expectation store in Azure blob storage](/docs/guides/setup/configuring-metadata-stores/how-to-configure-an-expectation-store-in-azure-blob-storage)
* [How to configure an Expectation store to PostgreSQL](/docs/guides/setup/configuring-metadata-stores/how-to-configure-an-expectation-store-to-postgresql)
* [How to configure an Expectation store on a filesystem](/docs/guides/setup/configuring-metadata-stores/how-to-configure-an-expectation-store-to-postgresql)

## Options for storing Validation Results
By default, Validation Results are stored locally, in an uncommitted directory. This is great for individual work, but not good for collaboration. The most common pattern is to use a cloud-based blob store such as S3, GCS, or Azure blob store. You can also store Validation Results in a database.

* [How to configure a Validation Result store on a filesystem](/docs/guides/setup/configuring-metadata-stores/how-to-configure-a-validation-result-store-on-a-filesystem)
* [How to configure a Validation Result store in Amazon S3](/docs/guides/setup/configuring-metadata-stores/how-to-configure-a-validation-result-store-in-amazon-s3)
* [How to configure a Validation Result store in GCS](/docs/guides/setup/configuring-metadata-stores/how-to-configure-a-validation-result-store-in-gcs)
* [How to configure a Validation Result store in Azure blob storage](/docs/guides/setup/configuring-metadata-stores/how-to-configure-a-validation-result-store-in-azure-blob-storage)
* [How to configure a Validation Result store to PostgreSQL](/docs/guides/setup/configuring-metadata-stores/how-to-configure-a-validation-result-store-to-postgresql)

## Options for customizing generated notebooks
Great Expectations generates and provides notebooks as interactive development environments for expectation suites. You might want to customize parts of the notebooks to add company-specific documentation, or change the code sections to suit your use-cases.

* [How to configure notebooks generated by “suite edit”](/docs/guides/miscellaneous/how-to-configure-notebooks-generated-by-suite-edit)

## Deployment Patterns

* [How to instantiate a Data Context on an EMR Spark cluster](/docs/deployment_patterns/how-to-instantiate-a-data-context-on-an-emr-spark-cluster)
* [How to instantiate a Data Context on Databricks Spark cluster](/docs/deployment_patterns/how-to-instantiate-a-data-context-on-databricks-spark-cluster)

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
* [How to connect to data on Azure using Spark](/docs/guides/connecting_to_your_data/cloud/azure/spark)

## Options for hosting Data Docs
By default, Data Docs are stored locally, in an uncommitted directory. This is great for individual work, but not good for collaboration. A better pattern is usually to deploy to a cloud-based blob store (S3, GCS, or Azure blob store), configured to share a static website.

* [How to host and share Data Docs on a filesystem](/docs/guides/setup/configuring-data-docs/how-to-host-and-share-data-docs-on-a-filesystem)
* [How to host and share Data Docs on Azure Blob Storage](/docs/guides/setup/configuring-data-docs/how-to-host-and-share-data-docs-on-azure-blob-storage)
* [How to host and share Data Docs on GCS](/docs/guides/setup/configuring-data-docs/how-to-host-and-share-data-docs-on-gcs)
* [How to host and share Data Docs on Amazon S3](/docs/guides/setup/configuring-data-docs/how-to-host-and-share-data-docs-on-amazon-s3)

## Additional Checkpoints and Actions
Most teams will want to configure various Checkpoints and Validation Actions as part of their deployment. There are two primary patterns for deploying Checkpoints. Sometimes Checkpoints are executed during data processing (e.g. as a task within Airflow). From this vantage point, they can control program flow. Sometimes Checkpoints are executed against materialized data. Great Expectations supports both patterns. There are also some rare instances where you may want to validate data without using a Checkpoint.

* [How to update Data Docs as a Validation Action](/docs/guides/validation/validation_actions/how-to-update-data-docs-as-a-validation-action)
* [How to store Validation Results as a Validation Action](/docs/guides/validation/validation_actions/how-to-store-validation-results-as-a-validation-action)
* [How to trigger Slack notifications as a Validation Action](/docs/guides/validation/validation_actions/how-to-trigger-slack-notifications-as-a-validation-action)
* [How to trigger Opsgenie notifications as a Validation Action](/docs/guides/validation/validation_actions/how-to-trigger-opsgenie-notifications-as-a-validation-action)
* [How to trigger Email as a Validation Action](/docs/guides/validation/validation_actions/how-to-trigger-email-as-a-validation-action)
* [How to deploy a scheduled Checkpoint with cron](/docs/guides/validation/advanced/how-to-deploy-a-scheduled-checkpoint-with-cron)
* [How to implement custom notifications](/docs/guides/validation/advanced/how-to-implement-custom-notifications)
* [How to validate data without a Checkpoint](/docs/guides/validation/advanced/how-to-validate-data-without-a-checkpoint)
* [How to run a Checkpoint in Airflow](/docs/deployment_patterns/how-to-run-a-checkpoint-in-airflow)
