---
sidebar_label: "Configure Validation Result Stores"
title: "Configure Validation Result Stores"
id: configure_result_stores
description: Configure storage locations for Validation Results.
toc_min_heading_level: 2
toc_max_heading_level: 2
---

import Preface from './components_how_to_configure_a_validation_result_store_in_amazon_s3/_preface.mdx'
import ConfigureBotoToConnectToTheAmazonSBucketWhereValidationResultsWillBeStored from './components/_install_boto3_with_pip.mdx'
import VerifyYourAwsCredentials from './components/_verify_aws_credentials_are_configured_properly.mdx'
import IdentifyYourDataContextValidationResultsStore from './components_how_to_configure_a_validation_result_store_in_amazon_s3/_identify_your_data_context_validation_results_store.mdx'
import UpdateYourConfigurationFileToIncludeANewStoreForValidationResultsOnS from './components_how_to_configure_a_validation_result_store_in_amazon_s3/_update_your_configuration_file_to_include_a_new_store_for_validation_results_on_s.mdx'
import CopyExistingValidationResultsToTheSBucketThisStepIsOptional from './components_how_to_configure_a_validation_result_store_in_amazon_s3/_copy_existing_validation_results_to_the_s_bucket_this_step_is_optional.mdx'
import ConfirmThatTheValidationsResultsStoreHasBeenCorrectlyConfigured from './components_how_to_configure_a_validation_result_store_in_amazon_s3/_confirm_that_the_validations_results_store_has_been_correctly_configured.mdx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

A Validation Results Store is a connector that is used to store and retrieve information about objects generated when data is Validated against an Expectation. By default, Validation Results are stored in JSON format in the ``uncommitted/validations/`` subdirectory of your ``great_expectations/`` folder. Use the information provided here to configure a store for your Validation Results.

:::caution

Validation Results can include sensitive or regulated data that should not be committed to a source control system.

:::


<Tabs
  groupId="configure-result-stores"
  defaultValue='amazon'
  values={[
  {label: 'Amazon S3', value:'amazon'},
  {label: 'Microsoft Azure Blob Storage', value:'azure'},
  {label: 'Google Cloud Service', value:'gcs'},
  {label: 'Filesystem', value:'filesystem'},
  {label: 'PostgreSQL', value:'postgresql'},
  ]}>
<TabItem value="amazon">

## Amazon S3

<Preface />

### Install boto3 in your local environment
<ConfigureBotoToConnectToTheAmazonSBucketWhereValidationResultsWillBeStored />

### Verify your AWS credentials are properly configured
<VerifyYourAwsCredentials />

### Identify your Data Context Validation Results Store
<IdentifyYourDataContextValidationResultsStore />

### Update your configuration file to include a new Store for Validation Results
<UpdateYourConfigurationFileToIncludeANewStoreForValidationResultsOnS />

### Copy existing Validation results to the S3 bucket (Optional)
<CopyExistingValidationResultsToTheSBucketThisStepIsOptional />

### Confirm the configuration
<ConfirmThatTheValidationsResultsStoreHasBeenCorrectlyConfigured />

</TabItem>
<TabItem value="azure">

## Microsoft Azure Blob Storage

Use the information provided here to configure a new storage location for Validation Results in Azure Blob Storage.

### Prerequisites

<Prerequisites>

- [A Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context).
- [An Expectations Suite](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data).
- [A Checkpoint](/docs/guides/validation/checkpoints/how_to_create_a_new_checkpoint).
- [An Azure Storage account](https://docs.microsoft.com/en-us/azure/storage) and get the [connection string](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal).
- An Azure Blob container. If you want to [host and share Data Docs on Azure Blob Storage](../../../guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_azure_blob_storage.md), you can set this up first and then use the ``$web`` existing container to store your <TechnicalTag tag="expectation" text="Expectations" />.
- A prefix (folder) to store Validation Results. You don't need to create the folder, the prefix is just part of the Blob name.

</Prerequisites>

### Configure the ``config_variables.yml`` file with your Azure Storage credentials

GX recommends that you store Azure Storage credentials in the ``config_variables.yml`` file, which is located in the ``uncommitted/`` folder by default, and is not part of source control. The following code adds Azure Storage credentials under the key ``AZURE_STORAGE_CONNECTION_STRING``: 

```yaml
AZURE_STORAGE_CONNECTION_STRING: "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>"
```

To learn more about the additional options for configuring the ``config_variables.yml`` file, or additional environment variables, see [How to configure credentials](../../setup/configuring_data_contexts/how_to_configure_credentials.md)

### Identify your Validation Results Store

Your <TechnicalTag tag="validation_result_store" text="Validation Results Store" /> configuration is provided in your <TechnicalTag tag="data_context" text="Data Context" />. Open ``great_expectations.yml`` and find the following entry: 

```yaml
validations_store_name: validations_store

stores:
  validations_store:
      class_name: ValidationsStore
      store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: uncommitted/validations/
```
This configuration tells Great Expectations to look for Validation Results in a Store named ``validations_store``. The default ``base_directory`` for ``validations_store`` is ``uncommitted/validations/``.

### Update your configuration file to include a new Store for Validation Results on Azure Storage account

In the following example, `validations_store_name` is set to ``validations_AZ_store``, but it can be personalized.  You also need to change the ``store_backend`` settings.  The ``class_name`` is ``TupleAzureBlobStoreBackend``, ``container`` is the name of your blob container where Validation Results are stored, ``prefix`` is the folder in the container where Validation Result files are located, and ``connection_string`` is ``${AZURE_STORAGE_CONNECTION_STRING}``to reference the corresponding key in the ``config_variables.yml`` file.

```yaml
validations_store_name: validations_AZ_store

stores:
  validations_AZ_store:
      class_name: ValidationsStore
      store_backend:
          class_name: TupleAzureBlobStoreBackend
          container: <blob-container>
          prefix: validations
          connection_string: ${AZURE_STORAGE_CONNECTION_STRING}
```

:::note
If the container for [hosting and sharing Data Docs on Azure Blob Storage](../../setup/configuring_data_docs/how_to_host_and_share_data_docs_on_azure_blob_storage.md) is named ``$web``, use ``container: \$web`` to allow access to the ``$web``container.
:::

Additional authentication and configuration options are available. See [Hosting and sharing Data Docs on Azure Blob Storage](../../setup/configuring_data_docs/how_to_host_and_share_data_docs_on_azure_blob_storage.md).

### Copy existing Validation Results JSON files to the Azure blob (Optional)

You can use the ``az storage blob upload`` command to copy Validation Results into Azure Blob Storage. The following command copies one Validation Result from a local folder to the Azure blob: 

```bash
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>"
az storage blob upload -f <local/path/to/validation.json> -c <GREAT-EXPECTATION-DEDICATED-AZURE-BLOB-CONTAINER-NAME> -n <PREFIX>/<validation.json>
example with a validation related to the exp1 expectation:
az storage blob upload -f great_expectations/uncommitted/validations/exp1/20210306T104406.877327Z/20210306T104406.877327Z/8313fb37ca59375eb843adf388d4f882.json -c <blob-container> -n validations/exp1/20210306T104406.877327Z/20210306T104406.877327Z/8313fb37ca59375eb843adf388d4f882.json
Finished[#############################################################]  100.0000%
{
"etag": "\"0x8D8E09F894650C7\"",
"lastModified": "2021-03-06T12:58:28+00:00"
}
```
To learn more about other methods that are available to copy Validation Result JSON files into Azure Blob Storage, see [Quickstart: Upload, download, and list blobs with the Azure portal](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal).

### Reference the new configuration

To make Great Expectations look for Validation Results on the Azure store, set the ``validations_store_name`` variable to the name of your Azure Validations Store. In the previous example this was `validations_AZ_store`.

### Confirm that the Validation Results Store has been correctly configured

[Run a Checkpoint](/docs/guides/validation/checkpoints/how_to_create_a_new_checkpoint) to store results in the new Validation Results Store on Azure Blob and then visualize the results by [re-building Data Docs](../../../terms/data_docs.md).

</TabItem>
<TabItem value="gcs">

## GCS

Use the information provided here to configure a new storage location for Validation Results in GCS.

To view all the code used in this topic, see [how_to_configure_a_validation_result_store_in_gcs.py](https://github.com/great-expectations/great_expectations/tree/develop/tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py).

### Prerequisites

<Prerequisites>

- [A Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context).
- [An Expectations Suite](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data).
- [A Checkpoint](/docs/guides/validation/checkpoints/how_to_create_a_new_checkpoint).
- A GCP [service account](https://cloud.google.com/iam/docs/service-accounts) with credentials that allow access to GCP resources such as Storage Objects.
- A GCP project, GCS bucket, and prefix to store Validation Results.

</Prerequisites>

### Configure your GCP credentials

Confirm that your environment is configured with the appropriate authentication credentials needed to connect to the GCS bucket where Validation Results will be stored. This includes the following:

- A GCP service account.
- Setting the ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable.
- Verifying authentication by running a [Google Cloud Storage client](https://cloud.google.com/storage/docs/reference/libraries) library script.

For more information about validating your GCP authentication credentials, see [Authenticate to Cloud services using client libraries](https://cloud.google.com/docs/authentication/getting-started).

### Identify your Data Context Validation Results Store

The configuration for your <TechnicalTag tag="validation_result_store" text="Validation Results Store" /> is available in your <TechnicalTag tag="data_context" text="Data Context" />. Open ``great_expectations.yml``and find the following entry: 

```yaml name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py expected_existing_validations_store_yaml"
```
This configuration tells Great Expectations to look for Validation Results in the ``validations_store`` Store. The default ``base_directory`` for ``validations_store`` is ``uncommitted/validations/``.

### Update your configuration file to include a new Store for Validation Results

In the following example, `validations_store_name` is set to ``validations_GCS_store``, but it can be personalized.  You also need to change the ``store_backend`` settings. The ``class_name`` is ``TupleGCSStoreBackend``, ``project`` is your GCP project, ``bucket`` is the address of your GCS bucket, and ``prefix`` is the folder on GCS where Validation Result files are stored.

```yaml name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py configured_validations_store_yaml"
```

:::warning
If you are also storing [Expectations in GCS](../configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.md) or [DataDocs in GCS](../configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.md), make sure that the ``prefix`` values are disjoint and one is not a substring of the other.
:::

### Copy existing Validation Results to the GCS bucket (Optional)

Use the ``gsutil cp`` command to copy Validation Results into GCS. For example, the following command copies the Validation results ``validation_1`` and ``validation_2``into a GCS bucket: 

```bash name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py copy_validation_command"
```
The following confirmation message is returned:

```bash name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py copy_validation_output"
```
Additional methods for copying Validation Results into GCS are available. See [Upload objects from a filesystem](https://cloud.google.com/storage/docs/uploading-objects).

### Reference the new configuration

To make Great Expectations look for Validation Results on the GCS store, set the ``validations_store_name`` variable to the name of your GCS Validations Store. In the previous example this was `validations_GCS_store`.

### Confirm that the Validation Results Store has been correctly configured

[Run a Checkpoint](/docs/guides/validation/how_to_validate_data_by_running_a_checkpoint) to store results in the new Validation Results Store on GCS, and then visualize the results by [re-building Data Docs](/docs/terms/data_docs).

</TabItem>
<TabItem value="filesystem">

## Filesystem

Use the information provided here to configure a new storage location for Validation Results in your filesystem. You'll learn how to use an <TechnicalTag tag="action" text="Action" /> to update <TechnicalTag tag="data_docs" text="Data Docs" /> sites with new Validation Results from <TechnicalTag tag="checkpoint" text="Checkpoint" /> runs.

### Prerequisites

<Prerequisites>

- [A Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context).
- [An Expectation Suite ](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data).
- [A Checkpoint](../../../guides/validation/checkpoints/how_to_create_a_new_checkpoint.md).
- A new storage location to store Validation Results. This can be a local path, or a path to a secure network filesystem.

</Prerequisites>

### Create a new folder for Validation Results

Run the following command to create a new folder for your Validation Results and move your existing Validation Results to the new folder:

```bash
# in the great_expectations/ folder
mkdir shared_validations
mv uncommitted/validations/npi_validations/ uncommitted/shared_validations/
```
In this example, the name of the Validation Result is ``npi_validations`` and the path to the new storage location is ``shared_validations/``.

### Identify your Data Context Validation Results Store

The configuration for your <TechnicalTag tag="validation_result_store" text="Validation Results Store" /> is available in your <TechnicalTag tag="data_context" text="Data Context" />.  Open ``great_expectations.yml``and find the following entry: 

```yaml
validations_store_name: validations_store

stores:
   validations_store:
       class_name: ValidationsStore
       store_backend:
           class_name: TupleFilesystemStoreBackend
           base_directory: uncommitted/validations/
```

This configuration tells Great Expectations to look for Validation Results in the ``validations_store`` Store. The default ``base_directory`` for ``validations_store`` is ``uncommitted/validations/``.

### Update your configuration file to include a new Store for Validation results

In the following example, `validations_store_name` is set to ``shared_validations_filesystem_store``, but it can be personalized.  Also, ``base_directory`` is set to ``uncommitted/shared_validations/``, but you can set it to another path that is accessible by Great Expectations.

```yaml
validations_store_name: shared_validations_filesystem_store

stores:
   shared_validations_filesystem_store:
       class_name: ValidationsStore
       store_backend:
           class_name: TupleFilesystemStoreBackend
           base_directory: uncommitted/shared_validations/
```

### Confirm that the Validation Results Store has been correctly configured

Run a [Checkpoint](/docs/guides/validation/how_to_validate_data_by_running_a_checkpoint) to store results in the new Validation Results Store in your new location, and then visualize the results by re-building [Data Docs](../../../terms/data_docs.md).

</TabItem>
<TabItem value="postgresql">

## PostgreSQL

Use the information provided here to configure Great Expectations to store Validation Results in a PostgreSQL database.

### Prerequisites

<Prerequisites>

- [A Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context).
- [An Expectations Suite](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data).
- [A Checkpoint](/docs/guides/validation/checkpoints/how_to_create_a_new_checkpoint).
- [A PostgreSQL database](https://www.postgresql.org/) with appropriate credentials.

</Prerequisites>

### Configure the ``config_variables.yml`` file with your database credentials

GX recommends storing database credentials in the ``config_variables.yml`` file, which is located in the ``uncommitted/`` folder by default, and not part of source control. 

1. To add database credentials, open ``config_variables.yml`` and add the following entry below the ``db_creds`` key: 

    ```yaml
    db_creds:
      drivername: postgresql
      host: '<your_host_name>'
      port: '<your_port>'
      username: '<your_username>'
      password: '<your_password>'
      database: '<your_database_name>'
    ```
    To configure the ``config_variables.yml`` file, or additional environment variables, see [How to configure credentials](../configuring_data_contexts/how_to_configure_credentials.md).

2. Optional. To use a specific schema as the backend, specify `schema` as an additional keyword argument. For example:

    ```yaml
    db_creds:
      drivername: postgresql
      host: '<your_host_name>'
      port: '<your_port>'
      username: '<your_username>'
      password: '<your_password>'
      database: '<your_database_name>'
      schema: '<your_schema_name>'
    ```

### Identify your Data Context Validation Results Store

The configuration for your <TechnicalTag tag="validation_result_store" text="Validation Results Store" /> is available in your <TechnicalTag tag="data_context" text="Data Context" />.  Open ``great_expectations.yml``and find the following entry:

```yaml
validations_store_name: validations_store

stores:
  validations_store:
      class_name: ValidationsStore
      store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: uncommitted/validations/
```
This configuration tells Great Expectations to look for Validation Results in the ``validations_store`` Store. The default ``base_directory`` for ``validations_store`` is ``uncommitted/validations/``.

### Update your configuration file to include a new Validation Results Store

Add the following entry to your ``great_expectations.yml``: 

```yaml
validations_store_name: validations_postgres_store

stores:
  validations_postgres_store:
      class_name: ValidationsStore
      store_backend:
          class_name: DatabaseStoreBackend
          credentials: ${db_creds}
```

In the previous example, `validations_store_name` is set to ``validations_postgres_store``, but it can be personalized.  Also, ``class_name`` is set to ``DatabaseStoreBackend``, and ``credentials`` is set to ``${db_creds}``, which references the corresponding key in the ``config_variables.yml`` file.  

### Confirm the addition of the new Validation Results Store

In the previous example, a ``validations_store`` on the local filesystem and a ``validations_postgres_store`` are configured.  Great Expectations looks for Validation Results in PostgreSQL when the ``validations_store_name`` variable is set to ``validations_postgres_store``. Run the following command to remove ``validations_store`` and confirm the ``validations_postgres_store`` configuration:

```bash
great_expectations store list

- name: validations_store
class_name: ValidationsStore
store_backend:
  class_name: TupleFilesystemStoreBackend
  base_directory: uncommitted/validations/

- name: validations_postgres_store
class_name: ValidationsStore
store_backend:
  class_name: DatabaseStoreBackend
  credentials:
      database: '<your_db_name>'
      drivername: postgresql
      host: '<your_host_name>'
      password: ******
      port: '<your_port>'
      username: '<your_username>'
```

### Confirm the Validation Results Store is configured correctly

[Run a Checkpoint](/docs/guides/validation/how_to_validate_data_by_running_a_checkpoint) to store results in the new Validation Results store in PostgreSQL, and then visualize the results by [re-building Data Docs](../../../terms/data_docs.md).

Great Expectations creates a new table in your database named ``ge_validations_store``, and populates the fields with information from the Validation Results.

</TabItem>
</Tabs>