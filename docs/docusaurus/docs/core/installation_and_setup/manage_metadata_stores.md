---
title: Manage Metadata Stores
---

import CopyExistingValidationResultsToTheSBucketThisStepIsOptional from './components_how_to_configure_a_validation_result_store_in_amazon_s3/_copy_existing_validation_results_to_the_s_bucket_this_step_is_optional.mdx'
import ConfirmThatTheValidationsResultsStoreHasBeenCorrectlyConfigured from './components_how_to_configure_a_validation_result_store_in_amazon_s3/_confirm_that_the_validations_results_store_has_been_correctly_configured.mdx'

import Preface from './components_how_to_configure_an_expectation_store_in_amazon_s3/_preface.mdx'
import InstallBoto3 from './components/_install_boto3_with_pip.mdx'
import VerifyAwsCredentials from './components/_verify_aws_credentials_are_configured_properly.mdx'
import IdentifyYourDataContextExpectationsStore from './components_how_to_configure_an_expectation_store_in_amazon_s3/_identify_your_data_context_expectations_store.mdx'
import UpdateYourConfigurationFileToIncludeANewStoreForExpectationsOnS from './components_how_to_configure_an_expectation_store_in_amazon_s3/_update_your_configuration_file_to_include_a_new_store_for_expectations_on_s.mdx'
import CopyExistingExpectationJsonFilesToTheSBucketThisStepIsOptional from './components_how_to_configure_an_expectation_store_in_amazon_s3/_copy_existing_expectation_json_files_to_the_s_bucket_this_step_is_optional.mdx'
import ConfirmList from './components_how_to_configure_an_expectation_store_in_amazon_s3/_confirm_list.mdx'
import Prerequisites from '../../../../components/_prerequisites.jsx'
import TechnicalTag from '../../../../reference/learn/term_tags/_tag.mdx';
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

A Store retrieves and stores data metadata. Stores are available from your Data Context.

GX Core supports the following Stores 

- **Validation Result Store** - stores and retrieves information about objects generated when data is Validated against an Expectation Suite.
- **Expectation Store** - stores and retrieves information about collections of verifiable assertions about data.  These are Stores for Expectation Suites.
- **Checkpoint Store** - stores and retrieves information about means for validating data in a production deployment of GX Cores.
- **Validation Definition Store** - stores and retrieves information about .

## Configure Validation Result Stores

A Validation Results Store is a connector that is used to store and retrieve information about objects generated when data is Validated against an Expectation. By default, Validation Results are stored in JSON format in the ``uncommitted/validations/`` subdirectory of your ``gx/`` folder. Use the information provided here to configure a store for your Validation Results.

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

<Preface />

### Install boto3 in your local environment
<ConfigureBotoToConnectToTheAmazonSBucketWhereValidationResultsWillBeStored />

### Verify your AWS credentials are properly configured
<VerifyYourAwsCredentials />

### Identify your Data Context Validation Results Store

Look for this section in your Data Context `great_expectations.yml` file:

```yaml
stores:
  validations_store:
    class_name: ValidationsStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: uncommitted/validations/

validations_store_name: validations_store
```
These parameters tell GX Core to look for Validation Results in a Store named `validations_store`. It also creates a `ValidationsStore` named `validations_store` that is backed by a Filesystem and stores Validation Results under the `base_directory uncommitted/validations` (the default).

### Update your configuration file to include a new Store for Validation Results

1. To manually add a Validation Results Store, add the following configuration to the `stores` section of your `great_expectations.yml` file:

    ```yaml
    stores:
    validations_S3_store:
        class_name: ValidationsStore
        store_backend:
        class_name: TupleS3StoreBackend
        bucket: '<your>'
        prefix: '<your>'  # Bucket and prefix in combination must be unique across all stores
    ```

2. Change the default store_backend settings to make the Store work with S3. The `class_name` is set to `TupleS3StoreBackend`, `bucket` is the address of your S3 bucket, and `prefix` is the folder in your S3 bucket where Validation Results are located.

    The following example shows the additional options that are available to customize TupleS3StoreBackend:

    ```yaml
    class_name: ValidationsStore
    store_backend:
    class_name: TupleS3StoreBackend
    bucket: '<your_s3_bucket_name>'
    prefix: '<your_s3_bucket_folder_name>'  # Bucket and prefix in combination must be unique across all stores
    boto3_options:
        endpoint_url: ${S3_ENDPOINT} # Uses the S3_ENDPOINT environment variable to determine which endpoint to use.
        region_name: '<your_aws_region_name>'
    ```
3. Optional. To use a personalized Store name, update the validations_store_name key value to match the Store name. For example:

    ```yaml
    validations_store_name: validations_S3_store
    ```
    When you update the validations_store_name key value, Great Expectations uses the new Store for Validation Results.

4. Add the following code to `great_expectations.yml` to configure the IAM user:
    
    ```yaml
    class_name: ValidationsStore
    store_backend:
    class_name: TupleS3StoreBackend
    bucket: '<your_s3_bucket_name>'
    prefix: '<your_s3_bucket_folder_name>' # Bucket and prefix in combination must be unique across all stores
    boto3_options:
        aws_access_key_id: ${AWS_ACCESS_KEY_ID} # Uses the AWS_ACCESS_KEY_ID environment variable to get aws_access_key_id.
        aws_secret_access_key: ${AWS_ACCESS_KEY_ID}
        aws_session_token: ${AWS_ACCESS_KEY_ID}
    ```
5. Add the following code to `great_expectations.yml` to configure the IAM Assume Role:

    ```yaml
    class_name: ValidationsStore
    store_backend:
    class_name: TupleS3StoreBackend
    bucket: '<your_s3_bucket_name>'
    prefix: '<your_s3_bucket_folder_name>' # Bucket and prefix in combination must be unique across all stores
    boto3_options:
        assume_role_arn: '<your_role_to_assume>'
        region_name: '<your_aws_region_name>'
        assume_role_duration: session_duration_in_seconds
    ```
    If you are also storing Expectations or Data Docs in S3, make sure the `prefix` values are disjoint and one is not a substring of the other.

### Copy existing Validation results to the S3 bucket (Optional)

If you are converting an existing local GX Core deployment to one that works in AWS, you might have Validation Results saved that you want to transfer to your S3 bucket.

To copy Validation Results into Amazon S3, use the aws s3 sync command. For example:

### Confirm the configuration
<ConfirmThatTheValidationsResultsStoreHasBeenCorrectlyConfigured />

</TabItem>
<TabItem value="azure">

Use the information provided here to configure a new storage location for Validation Results in Azure Blob Storage.

## Prerequisites

<Prerequisites>

- [A Data Context](./manage_data_contexts.md).
- [An Expectations Suite](/core/create_expectations/expectation_suites/manage_expectation_suites.md).
- [A Checkpoint](/core/validate_data/checkpoints/manage_checkpoints.md).
- [An Azure Storage account](https://docs.microsoft.com/en-us/azure/storage) and get the [connection string](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal).
- An Azure Blob container.
- A prefix (folder) to store Validation Results. You don't need to create the folder, the prefix is just part of the Blob name.

</Prerequisites>

## Configure the ``config_variables.yml`` file with your Azure Storage credentials

GX recommends that you store Azure Storage credentials in the ``config_variables.yml`` file, which is located in the ``uncommitted/`` folder by default, and is not part of source control. The following code adds Azure Storage credentials under the key ``AZURE_STORAGE_CONNECTION_STRING``: 

```yaml
AZURE_STORAGE_CONNECTION_STRING: "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>"
```

To learn more about the additional options for configuring the ``config_variables.yml`` file, or additional environment variables, see [Manage credentials](/core/installation_and_setup/manage_credentials.md)

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

## Update your configuration file to include a new Store for Validation Results on Azure Storage account

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
If the container for hosting and sharing Data Docs on Azure Blob Storage is named ``$web``, use ``container: \$web`` to allow access to the ``$web``container.
:::

## Copy existing Validation Results JSON files to the Azure blob (Optional)

You can use the ``az storage blob upload`` command to copy Validation Results into Azure Blob Storage. The following command copies one Validation Result from a local folder to the Azure blob: 

```bash
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>"
az storage blob upload -f <local/path/to/validation.json> -c <GREAT-EXPECTATION-DEDICATED-AZURE-BLOB-CONTAINER-NAME> -n <PREFIX>/<validation.json>
example with a validation related to the exp1 expectation:
az storage blob upload -f gx/uncommitted/validations/exp1/20210306T104406.877327Z/20210306T104406.877327Z/8313fb37ca59375eb843adf388d4f882.json -c <blob-container> -n validations/exp1/20210306T104406.877327Z/20210306T104406.877327Z/8313fb37ca59375eb843adf388d4f882.json
Finished[#############################################################]  100.0000%
{
"etag": "\"0x8D8E09F894650C7\"",
"lastModified": "2021-03-06T12:58:28+00:00"
}
```
To learn more about other methods that are available to copy Validation Result JSON files into Azure Blob Storage, see [Quickstart: Upload, download, and list blobs with the Azure portal](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal).

## Reference the new configuration

To make Great Expectations look for Validation Results on the Azure store, set the ``validations_store_name`` variable to the name of your Azure Validations Store. In the previous example this was `validations_AZ_store`.

### Confirm that the Validation Results Store has been correctly configured

[Run a Checkpoint](/core/validate_data/checkpoints/manage_checkpoints.md) to store results in the new Validation Results Store on Azure Blob and then visualize the results by re-building Data Docs.

</TabItem>
<TabItem value="gcs">

Use the information provided here to configure a new storage location for Validation Results in GCS.

To view all the code used in this topic, see [how_to_configure_a_validation_result_store_in_gcs.py](https://github.com/great-expectations/great_expectations/tree/develop/docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py).

## Prerequisites

<Prerequisites>

- [A Data Context](./manage_data_contexts.md).
- [An Expectations Suite](/core/create_expectations/expectation_suites/manage_expectation_suites.md).
- [A Checkpoint](/core/validate_data/checkpoints/manage_checkpoints.md).
- A GCP [service account](https://cloud.google.com/iam/docs/service-accounts) with credentials that allow access to GCP resources such as Storage Objects.
- A GCP project, GCS bucket, and prefix to store Validation Results.

</Prerequisites>

## Configure your GCP credentials

Confirm that your environment is configured with the appropriate authentication credentials needed to connect to the GCS bucket where Validation Results will be stored. This includes the following:

- A GCP service account.
- Setting the ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable.
- Verifying authentication by running a [Google Cloud Storage client](https://cloud.google.com/storage/docs/reference/libraries) library script.

For more information about validating your GCP authentication credentials, see [Authenticate to Cloud services using client libraries](https://cloud.google.com/docs/authentication/getting-started).

## Identify your Data Context Validation Results Store

The configuration for your <TechnicalTag tag="validation_result_store" text="Validation Results Store" /> is available in your <TechnicalTag tag="data_context" text="Data Context" />. Open ``great_expectations.yml``and find the following entry: 

```yaml title="great_expectations.yml" name="docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py expected_existing_validations_store_yaml"
```
This configuration tells Great Expectations to look for Validation Results in the ``validations_store`` Store. The default ``base_directory`` for ``validations_store`` is ``uncommitted/validations/``.

## Update your configuration file to include a new Store for Validation Results

In the following example, `validations_store_name` is set to ``validations_GCS_store``, but it can be personalized.  You also need to change the ``store_backend`` settings. The ``class_name`` is ``TupleGCSStoreBackend``, ``project`` is your GCP project, ``bucket`` is the address of your GCS bucket, and ``prefix`` is the folder on GCS where Validation Result files are stored.

```yaml title="YAML" name="docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py configured_validations_store_yaml"
```

:::warning
If you are also storing [Expectations in GCS](../configuring_metadata_stores/configure_expectation_stores.md) make sure that the ``prefix`` values are disjoint and one is not a substring of the other.
:::

## Copy existing Validation Results to the GCS bucket (Optional)

Use the ``gsutil cp`` command to copy Validation Results into GCS. For example, the following command copies the Validation results ``validation_1`` and ``validation_2``into a GCS bucket: 

```bash title="Terminal input" name="docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py copy_validation_command"
```
The following confirmation message is returned:

```bash title="Terminal output" name="docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py copy_validation_output"
```
Additional methods for copying Validation Results into GCS are available. See [Upload objects from a filesystem](https://cloud.google.com/storage/docs/uploading-objects).

## Reference the new configuration

To make Great Expectations look for Validation Results on the GCS store, set the ``validations_store_name`` variable to the name of your GCS Validations Store. In the previous example this was `validations_GCS_store`.

## Confirm that the Validation Results Store has been correctly configured

[Run a Checkpoint](/core/validate_data/checkpoints/manage_checkpoints.md) to store results in the new Validation Results Store on GCS, and then visualize the results by re-building Data Docs.

</TabItem>
<TabItem value="filesystem">

Use the information provided here to configure a new storage location for Validation Results in your filesystem. You'll learn how to use an <TechnicalTag tag="action" text="Action" /> to update <TechnicalTag tag="data_docs" text="Data Docs" /> sites with new Validation Results from <TechnicalTag tag="checkpoint" text="Checkpoint" /> runs.

## Prerequisites

<Prerequisites>

- [A Data Context](./manage_data_contexts.md).
- [An Expectations Suite](/core/create_expectations/expectation_suites/manage_expectation_suites.md).
- [A Checkpoint](/core/validate_data/checkpoints/manage_checkpoints.md).
- A new storage location to store Validation Results. This can be a local path, or a path to a secure network filesystem.

</Prerequisites>

## Create a new folder for Validation Results

Run the following command to create a new folder for your Validation Results and move your existing Validation Results to the new folder:

```bash
# in the gx/ folder
mkdir shared_validations
mv uncommitted/validations/npi_validations/ uncommitted/shared_validations/
```
In this example, the name of the Validation Result is ``npi_validations`` and the path to the new storage location is ``shared_validations/``.

## Identify your Data Context Validation Results Store

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

## Update your configuration file to include a new Store for Validation results

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

## Confirm that the Validation Results Store has been correctly configured

Run a [Checkpoint](/core/validate_data/checkpoints/manage_checkpoints.md) to store results in the new Validation Results Store in your new location, and then visualize the results by re-building Data Docs.

</TabItem>
<TabItem value="postgresql">

Use the information provided here to configure Great Expectations to store Validation Results in a PostgreSQL database.

## Prerequisites

<Prerequisites>

- [A Data Context](./manage_data_contexts.md).
- [An Expectations Suite](/core/create_expectations/expectation_suites/manage_expectation_suites.md).
- [A Checkpoint](/core/validate_data/checkpoints/manage_checkpoints.md).
- [A PostgreSQL database](https://www.postgresql.org/) with appropriate credentials.

</Prerequisites>

## Configure the ``config_variables.yml`` file with your database credentials

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
    To configure the ``config_variables.yml`` file, or additional environment variables, see [Manage credentials](/core).

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

## Identify your Data Context Validation Results Store

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

## Update your configuration file to include a new Validation Results Store

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

## Confirm the addition of the new Validation Results Store

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

## Confirm the Validation Results Store is configured correctly

[Run a Checkpoint](/core/validate_data/checkpoints/manage_checkpoints.md) to store results in the new Validation Results store in PostgreSQL, and then visualize the results by re-building Data Docs.

Great Expectations creates a new table in your database named ``ge_validations_store``, and populates the fields with information from the Validation Results.

</TabItem>
</Tabs>

## Configure Expectation Stores

An Expectation Store is a connector to store and retrieve information about collections of verifiable assertions about data.

By default, new <TechnicalTag tag="expectation" text="Expectations" /> are stored as <TechnicalTag tag="expectation_suite" text="Expectation Suites" /> in JSON format in the `expectations/` subdirectory of your `gx/` folder. Use the information provided here to configure a store for your Expectations.

<Tabs
  groupId="configure-expectation-stores"
  defaultValue='amazon'
  values={[
  {label: 'Amazon S3', value:'amazon'},
  {label: 'Microsoft Azure Blob Storage', value:'azure'},
  {label: 'Google Cloud Service', value:'gcs'},
  {label: 'Filesystem', value:'filesystem'},
  {label: 'PostgreSQL', value:'postgresql'},
  ]}>
<TabItem value="amazon">

<Preface />

## Install boto3 with pip
<InstallBoto3 />

## Verify your AWS credentials
<VerifyAwsCredentials />

## Identify your Data Context Expectations Store
<IdentifyYourDataContextExpectationsStore />

## Update your configuration file to include a new Store for Expectations
<UpdateYourConfigurationFileToIncludeANewStoreForExpectationsOnS />

## Copy existing Expectation JSON files to the S3 bucket (Optional)
<CopyExistingExpectationJsonFilesToTheSBucketThisStepIsOptional />

## Confirm Expectation Suite availability
<ConfirmList />

</TabItem>
<TabItem value="azure">

Use the information provided here to configure a new storage location for Expectations in Microsoft Azure Blob Storage.

## Prerequisites

<Prerequisites>

- [A Data Context](./manage_data_contexts.md).
- [An Expectations Suite](/core/create_expectations/expectation_suites/manage_expectation_suites.md).
- [An Azure Storage account](https://docs.microsoft.com/en-us/azure/storage/).
- An Azure Blob container.
- A prefix (folder) where to store Expectations. You don't need to create the folder, the prefix is just part of the Azure Blob name.

</Prerequisites>

## Configure the ``config_variables.yml`` file with your Azure Storage credentials

GX recommends that you store Azure Storage credentials in the ``config_variables.yml`` file, which is located in the ``uncommitted/`` folder by default, and is not part of source control. The following code adds Azure Storage credentials below the ``AZURE_STORAGE_CONNECTION_STRING`` key:

```yaml
AZURE_STORAGE_CONNECTION_STRING: "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>"
```
To learn more about the additional options for configuring the ``config_variables.yml`` file, or additional environment variables, see [Manage credentials](/core/installation_and_setup/manage_credentials.md).

## Identify your Data Context Expectations Store

Your Expectations Store configuration is provided in your <TechnicalTag tag="data_context" text="Data Context" />. Open ``great_expectations.yml`` and find the following entry:

```yaml
expectations_store_name: expectations_store

stores:
  expectations_store:
      class_name: ExpectationsStore
      store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: expectations/
```

This configuration tells Great Expectations to look for Expectations in a Store named ``expectations_store``. The default ``base_directory`` for ``expectations_store`` is ``expectations/``.

## Update your configuration file to include a new Store for Expectations

In the following example, ``expectations_store_name`` is set to ``expectations_AZ_store``, but it can be personalized.  You also need to change the ``store_backend`` settings.  The ``class_name`` is ``TupleAzureBlobStoreBackend``, ``container`` is the name of your blob container where Expectations are stored, ``prefix`` is the folder in the container where Expectations are located, and ``connection_string`` is ``${AZURE_STORAGE_CONNECTION_STRING}`` to reference the corresponding key in the ``config_variables.yml`` file.

```yaml
expectations_store_name: expectations_AZ_store

stores:
  expectations_AZ_store:
      class_name: ExpectationsStore
      store_backend:
        class_name: TupleAzureBlobStoreBackend
        container: <blob-container>
        prefix: expectations
        connection_string: ${AZURE_STORAGE_CONNECTION_STRING}
```

:::note
If the container for hosting and sharing Data Docs on Azure Blob Storage is named ``$web``, use ``container: \$web`` to allow access to the ``$web``container.
:::

## Copy existing Expectation JSON files to the Azure blob (Optional)

You can use the ``az storage blob upload`` command to copy Expectations into Azure Blob Storage. The following command copies the Expectation ``exp1`` from a local folder to Azure Blob Storage: 

```bash
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>"
az storage blob upload -f <local/path/to/expectation.json> -c <GREAT-EXPECTATION-DEDICATED-AZURE-BLOB-CONTAINER-NAME> -n <PREFIX>/<expectation.json>
example :
az storage blob upload -f gx/expectations/exp1.json -c <blob-container> -n expectations/exp1.json

Finished[#############################################################]  100.0000%
{
"etag": "\"0x8D8E08E5DA47F84\"",
"lastModified": "2021-03-06T10:55:33+00:00"
}
```
To learn more about other methods that are available to copy Expectation JSON files into Azure Blob Storage, see [Introduction to Azure Blob Storage](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction).

## Confirm that the new Expectation Suites have been added

If you copied your existing Expectation Suites to Azure Blob Storage, run the following Python command to confirm that Great Expectations can find them:

<!--A snippet is required for this code block.-->

```python
import great_expectations as gx

context = gx.get_context()
context.list_expectation_suite_names()
```
A list of Expectations you copied to Azure Blob Storage is returned. Expectations that weren't copied to the new folder are not listed.

## Confirm that Expectations can be accessed from Azure Blob Storage

Run the following command to confirm your Expectations have been copied to Azure Blob Storage: 

```bash
great_expectations suite list
```
If your Expectations have not been copied to Azure Blob Storage, the message "No Expectations were found" is returned.

</TabItem>
<TabItem value="gcs">

Use the information provided here to configure a new storage location for Expectations in GCS.

To view all the code used in this topic, see [how_to_configure_an_expectation_store_in_gcs.py](https://github.com/great-expectations/great_expectations/tree/develop/docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py).

## Prerequisites

<Prerequisites>

- [A Data Context](./manage_data_contexts.md).
- [An Expectations Suite](/core/create_expectations/expectation_suites/manage_expectation_suites.md).
- A GCP [service account](https://cloud.google.com/iam/docs/service-accounts) with credentials that allow access to GCP resources such as Storage Objects.
- A GCP project, GCS bucket, and prefix to store Expectations.

</Prerequisites>

## Configure your GCP credentials

Confirm that your environment is configured with the appropriate authentication credentials needed to connect to the GCS bucket where Expectations will be stored. This includes the following:

- A GCP service account.
- Setting the ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable.
- Verifying authentication by running a [Google Cloud Storage client](https://cloud.google.com/storage/docs/reference/libraries) library script.

For more information about validating your GCP authentication credentials, see [Authenticate to Cloud services using client libraries](https://cloud.google.com/docs/authentication/getting-started).

## Identify your Data Context Expectations Store

The configuration for your Expectations <TechnicalTag tag="store" text="Store" /> is available in your <TechnicalTag tag="data_context" text="Data Context" />. Open ``great_expectations.yml`` and find the following entry: 

```yaml title="great_expectations.yml" name="docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py expected_existing_expectations_store_yaml"
```

This configuration tells Great Expectations to look for Expectations in the ``expectations_store`` Store. The default ``base_directory`` for ``expectations_store`` is ``expectations/``.

## Update your configuration file to include a new store for Expectations

In the following example, `expectations_store_name` is set to ``expectations_GCS_store``, but it can be personalized.  You also need to change the ``store_backend`` settings. The ``class_name`` is ``TupleGCSStoreBackend``, ``project`` is your GCP project, ``bucket`` is the address of your GCS bucket, and ``prefix`` is the folder on GCS where Expectations are stored.

```yaml title="YAML" name="docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py configured_expectations_store_yaml"
```

:::warning
If you are storing [Validations in GCS](./configure_result_stores.md) make sure that the ``prefix`` values are disjoint and one is not a substring of the other.
:::

## Copy existing Expectation JSON files to the GCS bucket (Optional)

Use the ``gsutil cp`` command to copy Expectations into GCS. For example, the following command copies the Expectation ```my_expectation_suite`` from a local folder into a GCS bucket:

```bash title="Terminal input" name="docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py copy_expectation_command"
```

The following confirmation message is returned:

```bash title="Terminal output" name="docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py copy_expectation_output"
```

Additional methods for copying Expectations into GCS are available. See [Upload objects from a filesystem](https://cloud.google.com/storage/docs/uploading-objects).

## Confirm that the new Expectation Suites have been added

If you copied your existing Expectation Suites to GCS, run the following Python command to confirm that Great Expectations can find them:

<!--A snippet is required for this code block.-->

```python
import great_expectations as gx

context = gx.get_context()
context.list_expectation_suite_names()
```

A list of Expectation Suites you copied to GCS is returned. Expectation Suites that weren't copied to the new Store aren't listed.

## Confirm that Expectations can be accessed from GCS

Run the following command to confirm your Expectations were copied to GCS:

```bash title="Terminal input" name="docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py list_expectation_suites_command"
```

If your Expectations were not copied to Azure Blob Storage, a message indicating no Expectations were found is returned.

</TabItem>
<TabItem value="filesystem">

Use the information provided here to configure a new storage location for Expectations on your Filesystem.

## Prerequisites

<Prerequisites>

- [A Data Context](./manage_data_contexts.md).
- [An Expectations Suite](/core/create_expectations/expectation_suites/manage_expectation_suites.md).
- A storage location for Expectations. This can be a local path, or a path to a network filesystem.
    
</Prerequisites>

## Create a new folder for Expectations

Run the following command to create a new folder for your Expectations and move your existing Expectations to the new folder:

```bash
# in the gx/ folder
mkdir shared_expectations
mv expectations/npi_expectations.json shared_expectations/
```
In this example, the name of the Expectation is ``npi_expectations`` and the path to the new storage location is ``/shared_expectations``.

## Identify your Data Context Expectations Store

The configuration for your Expectations <TechnicalTag tag="store" text="Store" /> is available in your <TechnicalTag tag="data_context" text="Data Context" />.  Open ``great_expectations.yml``and find the following entry:

```yaml
expectations_store_name: expectations_store

stores:
  expectations_store:
      class_name: ExpectationsStore
      store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: expectations/
```
This configuration tells Great Expectations to look for Expectations in the ``expectations_store`` Store. The default ``base_directory`` for ``expectations_store`` is ``expectations/``.

## Update your configuration file to include a new Store for Expectations results

In the following example, `expectations_store_name` is set to ``shared_expectations_filesystem_store``, but it can be personalized.  Also, ``base_directory`` is set to ``shared_expectations/``, but you can set it to another path that is accessible by Great Expectations.

```yaml
expectations_store_name: shared_expectations_filesystem_store

stores:
  shared_expectations_filesystem_store:
      class_name: ExpectationsStore
      store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: shared_expectations/
```

## Confirm that the new Expectation Suites have been added

If you copied your existing Expectation Suites to your filesystem, run the following Python command to confirm that Great Expectations can find them:

<!--A snippet is required for this code block.-->

```python
import great_expectations as gx

context = gx.get_context()
context.list_expectation_suite_names()
```

A list of Expectation Suites you copied your filesystem is returned. Expectation Suites that weren't copied to the new Store aren't listed.

## Version control systems

GX recommends that you store Expectations in a version control system such as Git. The JSON format of Expectations allows for informative diff-statements and modification tracking. In the following example, the ```expect_table_column_count_to_equal`` value changes from ``333`` to ``331``, and then to ``330``:

```bash
git log -p npi_expectations.json

commit cbc127fb27095364c3c1fcbf6e7f078369b07455
  changed expect_table_column_count_to_equal to 331

diff --git a/gx/expectations/npi_expectations.json b/great_expectations/expectations/npi_expectations.json

--- a/gx/expectations/npi_expectations.json
+++ b/gx/expectations/npi_expectations.json
@@ -17,7 +17,7 @@
   {
     "expectation_type": "expect_table_column_count_to_equal",
     "kwargs": {
-        "value": 333
+        "value": 331
     }
commit 05b3c8c1ed35d183bac1717d4877fe13bc574963
changed expect_table_column_count_to_equal to 333

diff --git a/gx/expectations/npi_expectations.json b/great_expectations/expectations/npi_expectations.json
--- a/gx/expectations/npi_expectations.json
+++ b/gx/expectations/npi_expectations.json
   {
     "expectation_type": "expect_table_column_count_to_equal",
     "kwargs": {
-        "value": 330
+        "value": 333
     }
```

</TabItem>
<TabItem value="postgresql">

Use the information provided here to configure an Expectations store in a PostgreSQL database.

## Prerequisites

<Prerequisites>

- [A Data Context](./manage_data_contexts.md).
- [An Expectations Suite](/core/create_expectations/expectation_suites/manage_expectation_suites.md).
- A [PostgreSQL](https://www.postgresql.org/) database with appropriate credentials.

</Prerequisites>

## Configure the `config_variables.yml` file with your database credentials

GX recommends storing database credentials in the ``config_variables.yml`` file, which is located in the ``uncommitted/`` folder by default, and not part of source control. 

To add database credentials, open ``config_variables.yml`` and add the following entry below the ``db_creds`` key: 

```yaml
    db_creds:
      drivername: postgresql
      host: '<your_host_name>'
      port: '<your_port>'
      username: '<your_username>'
      password: '<your_password>'
      database: '<your_database_name>'
```
To configure the ``config_variables.yml`` file, or additional environment variables, see [Manage credentials](/core/installation_and_setup/manage_credentials.md).

## Identify your Data Context Expectations Store

Open ``great_expectations.yml``and find the following entry:

```yaml
expectations_store_name: expectations_store

stores:
  expectations_store:
      class_name: ExpectationsStore
      store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: expectations/
```

This configuration tells Great Expectations to look for Expectations in the ``expectations_store`` Store. The default ``base_directory`` for ``expectations_store`` is ``expectations/``.

## Update your configuration file to include a new Store for Expectations

In the following example, `expectations_store_name` is set to ``expectations_postgres_store``, but it can be personalized. You also need to make some changes to the ``store_backend`` settings.  The ``class_name`` is ``DatabaseStoreBackend``, and ``credentials`` is ``${db_creds}`` to reference the corresponding key in the ``config_variables.yml`` file.

```yaml
expectations_store_name: expectations_postgres_store

stores:
  expectations_postgres_store:
      class_name: ExpectationsStore
      store_backend:
          class_name: DatabaseStoreBackend
          credentials: ${db_creds}
```

</TabItem>
</Tabs>