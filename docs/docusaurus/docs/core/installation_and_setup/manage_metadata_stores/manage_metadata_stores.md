---
title: Manage Metadata Stores
description: Create Stores to store data metadata.
toc_min_heading_level: 2
toc_max_heading_level: 2
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

A Store retrieves and stores metadata about your project. Stores are available from your Data Context.

When a Data Context initializes it includes a default location for metadata Stores.

- **File Data Contexts** - reads and writes Stores as JSON files in a subdirectory of the Data Context's root directory.  These Stores can be accessed by any Data Context that has access to the Store location and a Store configuration telling it to read from there.
- **Ephemeral Data Contexts** - reads and writes Stores in memory.  These Stores do not persist beyond the current Python Session and cannot be accessed by other Data Contexts.
- **Cloud Data Contexts** - reads and writes Stores through the associated GX Cloud account.  These Stores are automatically available to anyone with a shared organization ID for their Cloud Data Context.

If multiple File Data Contexts need access to shared metadata it may be necessary to configure alternative Store locations that they can all access.  In other cases, the default Store configuration is typically sufficient.

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
  ]}>

<TabItem value="amazon">

Use the information provided here to configure a new storage location for Validation Results in Amazon S3.

### Prerequisites

- [A Data Context](/core/installation_and_setup/manage_data_contexts.md).
- [An Expectations Suite](/core/create_expectations/expectation_suites/manage_expectation_suites.md).
- [A Checkpoint](/core/validate_data/checkpoints/manage_checkpoints.md).
- Permissions to install boto3 in your local environment.
- An S3 bucket and prefix for the Validation Results.

### Install boto3 with pip

Python interacts with AWS through the boto3 library. GX Core makes use of this library in the background when working with AWS. Although you won't use boto3 directly, you'll need to install it in your virtual environment.

To set up boto3 with AWS, and use boto3 within Python, see the [Boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html).

Run one of the following pip commands to install boto3 in your virtual environment:

```bash title="Terminal input"
python -m pip install boto3
```

or

```bash title="Terminal input"
python3 -m pip install boto3
```

### Verify your AWS credentials

Run the following command in the AWS CLI to verify that your AWS credentials are properly configured:

```bash title="Terminal input"
aws sts get-caller-identity
```

When your credentials are properly configured, your `UserId`, `Account`, and `Arn` are returned. If your credentials are not configured correctly, an error message appears. If you received an error message, or you couldn't verify your credentials, see [Configure the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).

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
    When you update the validations_store_name key value, GX Core uses the new Store for Validation Results.

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

```bash title="Terminal input"
aws s3 sync '<base_directory>' s3://'<your_s3_bucket_name>'/'<your_s3_bucket_folder_name>'
```

The base directory is set to `uncommitted/validations/` by default.

In the following example, the Validation Results `Validation1` and `Validation2` are copied to Amazon S3 and a confirmation message is returned:

```bash title="Terminal input"
upload: uncommitted/validations/val1/val1.json to s3://'<your_s3_bucket_name>'/'<your_s3_bucket_folder_name>'/val1.json
upload: uncommitted/validations/val2/val2.json to s3://'<your_s3_bucket_name>'/'<your_s3_bucket_folder_name>'/val2.json
```

### Confirm the configuration

Run a Checkpoint to store results in the new Validation Results Store on S3 then rebuild the Data Docs.

</TabItem>
<TabItem value="azure">

Use the information provided here to configure a new storage location for Validation Results in Azure Blob Storage.

### Prerequisites

- [A Data Context](/core/installation_and_setup/manage_data_contexts.md).
- [An Expectations Suite](/core/create_expectations/expectation_suites/manage_expectation_suites.md).
- [A Checkpoint](/core/validate_data/checkpoints/manage_checkpoints.md).
- [An Azure Storage account](https://docs.microsoft.com/en-us/azure/storage) and get the [connection string](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal).
- An Azure Blob container.
- A prefix (folder) to store Validation Results. You don't need to create the folder, the prefix is just part of the Blob name.

### Configure the ``config_variables.yml`` file with your Azure Storage credentials

GX recommends that you store Azure Storage credentials in the ``config_variables.yml`` file, which is located in the ``uncommitted/`` folder by default, and is not part of source control. The following code adds Azure Storage credentials under the key ``AZURE_STORAGE_CONNECTION_STRING``: 

```yaml
AZURE_STORAGE_CONNECTION_STRING: "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>"
```

To learn more about the additional options for configuring the ``config_variables.yml`` file, or additional environment variables, see [Manage credentials](/core/installation_and_setup/manage_credentials.md)

### Identify your Validation Results Store

Your Validation Results Store configuration is provided in your Data Context. Open ``great_expectations.yml`` and find the following entry: 

```yaml
validations_store_name: validations_store

stores:
  validations_store:
      class_name: ValidationsStore
      store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: uncommitted/validations/
```
This configuration tells GX Core to look for Validation Results in a Store named ``validations_store``. The default ``base_directory`` for ``validations_store`` is ``uncommitted/validations/``.

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
If the container for hosting and sharing Data Docs on Azure Blob Storage is named ``$web``, use ``container: \$web`` to allow access to the ``$web``container.
:::

### Copy existing Validation Results JSON files to the Azure blob (Optional)

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

### Reference the new configuration

To make GX Core look for Validation Results on the Azure store, set the ``validations_store_name`` variable to the name of your Azure Validations Store. In the previous example this was `validations_AZ_store`.

### Confirm that the Validation Results Store has been correctly configured

[Run a Checkpoint](/core/validate_data/checkpoints/manage_checkpoints.md) to store results in the new Validation Results Store on Azure Blob and then visualize the results by re-building Data Docs.

</TabItem>
<TabItem value="gcs">

Use the information provided here to configure a new storage location for Validation Results in GCS.

To view all the code used in this topic, see [how_to_configure_a_validation_result_store_in_gcs.py](https://github.com/great-expectations/great_expectations/tree/develop/docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py).

### Prerequisites

- [A Data Context](/core/installation_and_setup/manage_data_contexts.md).
- [An Expectations Suite](/core/create_expectations/expectation_suites/manage_expectation_suites.md).
- [A Checkpoint](/core/validate_data/checkpoints/manage_checkpoints.md).
- A GCP [service account](https://cloud.google.com/iam/docs/service-accounts) with credentials that allow access to GCP resources such as Storage Objects.
- A GCP project, GCS bucket, and prefix to store Validation Results.

### Configure your GCP credentials

Confirm that your environment is configured with the appropriate authentication credentials needed to connect to the GCS bucket where Validation Results will be stored. This includes the following:

- A GCP service account.
- Setting the ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable.
- Verifying authentication by running a [Google Cloud Storage client](https://cloud.google.com/storage/docs/reference/libraries) library script.

For more information about validating your GCP authentication credentials, see [Authenticate to Cloud services using client libraries](https://cloud.google.com/docs/authentication/getting-started).

### Identify your Data Context Validation Results Store

The configuration for your Validation Results Store is available in your Data Context. Open ``great_expectations.yml``and find the following entry: 

```yaml title="great_expectations.yml" name="docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py expected_existing_validations_store_yaml"
```
This configuration tells GX Core to look for Validation Results in the ``validations_store`` Store. The default ``base_directory`` for ``validations_store`` is ``uncommitted/validations/``.

### Update your configuration file to include a new Store for Validation Results

In the following example, `validations_store_name` is set to ``validations_GCS_store``, but it can be personalized.  You also need to change the ``store_backend`` settings. The ``class_name`` is ``TupleGCSStoreBackend``, ``project`` is your GCP project, ``bucket`` is the address of your GCS bucket, and ``prefix`` is the folder on GCS where Validation Result files are stored.

```yaml title="YAML" name="docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py configured_validations_store_yaml"
```

:::warning
If you are also storing Expectations in GCS make sure that the ``prefix`` values are disjoint and one is not a substring of the other.
:::

### Copy existing Validation Results to the GCS bucket (Optional)

Use the ``gsutil cp`` command to copy Validation Results into GCS. For example, the following command copies the Validation results ``validation_1`` and ``validation_2``into a GCS bucket: 

```bash title="Terminal input" name="docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py copy_validation_command"
```
The following confirmation message is returned:

```bash title="Terminal output" name="docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py copy_validation_output"
```
Additional methods for copying Validation Results into GCS are available. See [Upload objects from a filesystem](https://cloud.google.com/storage/docs/uploading-objects).

### Reference the new configuration

To make GX Core look for Validation Results on the GCS store, set the ``validations_store_name`` variable to the name of your GCS Validations Store. In the previous example this was `validations_GCS_store`.

### Confirm that the Validation Results Store has been correctly configured

[Run a Checkpoint](/core/validate_data/checkpoints/manage_checkpoints.md) to store results in the new Validation Results Store on GCS, and then visualize the results by re-building Data Docs.

</TabItem>
<TabItem value="filesystem">

Use the information provided here to configure a new storage location for Validation Results in your filesystem. You'll learn how to use an Action to update Data Docs sites with new Validation Results from Checkpoint runs.

### Prerequisites

- [A Data Context](/core/installation_and_setup/manage_data_contexts.md).
- [An Expectations Suite](/core/create_expectations/expectation_suites/manage_expectation_suites.md).
- [A Checkpoint](/core/validate_data/checkpoints/manage_checkpoints.md).
- A new storage location to store Validation Results. This can be a local path, or a path to a secure network filesystem.

### Create a new folder for Validation Results

Run the following command to create a new folder for your Validation Results and move your existing Validation Results to the new folder:

```bash
# in the gx/ folder
mkdir shared_validations
mv uncommitted/validations/npi_validations/ uncommitted/shared_validations/
```
In this example, the name of the Validation Result is ``npi_validations`` and the path to the new storage location is ``shared_validations/``.

### Identify your Data Context Validation Results Store

The configuration for your Validation Results Store is available in your Data Context.  Open ``great_expectations.yml``and find the following entry: 

```yaml
validations_store_name: validations_store

stores:
   validations_store:
       class_name: ValidationsStore
       store_backend:
           class_name: TupleFilesystemStoreBackend
           base_directory: uncommitted/validations/
```

This configuration tells GX Core to look for Validation Results in the ``validations_store`` Store. The default ``base_directory`` for ``validations_store`` is ``uncommitted/validations/``.

### Update your configuration file to include a new Store for Validation results

In the following example, `validations_store_name` is set to ``shared_validations_filesystem_store``, but it can be personalized.  Also, ``base_directory`` is set to ``uncommitted/shared_validations/``, but you can set it to another path that is accessible by GX Core.

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

Run a [Checkpoint](/core/validate_data/checkpoints/manage_checkpoints.md) to store results in the new Validation Results Store in your new location, and then visualize the results by re-building Data Docs.

</TabItem>

</Tabs>