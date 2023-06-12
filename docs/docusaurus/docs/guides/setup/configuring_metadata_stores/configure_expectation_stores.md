---
sidebar_label: "Configure Expectation Stores"
title: "Configure Expectation Stores"
id: configure_expectation_stores
description: Configure storage locations for Expectations.
toc_min_heading_level: 2
toc_max_heading_level: 2
---

import Preface from './components_how_to_configure_an_expectation_store_in_amazon_s3/_preface.mdx'
import InstallBoto3 from './components/_install_boto3_with_pip.mdx'
import VerifyAwsCredentials from './components/_verify_aws_credentials_are_configured_properly.mdx'
import IdentifyYourDataContextExpectationsStore from './components_how_to_configure_an_expectation_store_in_amazon_s3/_identify_your_data_context_expectations_store.mdx'
import UpdateYourConfigurationFileToIncludeANewStoreForExpectationsOnS from './components_how_to_configure_an_expectation_store_in_amazon_s3/_update_your_configuration_file_to_include_a_new_store_for_expectations_on_s.mdx'
import CopyExistingExpectationJsonFilesToTheSBucketThisStepIsOptional from './components_how_to_configure_an_expectation_store_in_amazon_s3/_copy_existing_expectation_json_files_to_the_s_bucket_this_step_is_optional.mdx'
import ConfirmList from './components_how_to_configure_an_expectation_store_in_amazon_s3/_confirm_list.mdx'
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

An Expectation Store is a connector to store and retrieve information about collections of verifiable assertions about data.

By default, new <TechnicalTag tag="profiling" text="Profiled" /> <TechnicalTag tag="expectation" text="Expectations" /> are stored as <TechnicalTag tag="expectation_suite" text="Expectation Suites" /> in JSON format in the `expectations/` subdirectory of your `great_expectations/` folder. Use the information provided here to configure a store for your Expectations.

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

## Amazon S3

<Preface />

### Install boto3 with pip
<InstallBoto3 />

### Verify your AWS credentials
<VerifyAwsCredentials />

### Identify your Data Context Expectations Store
<IdentifyYourDataContextExpectationsStore />

### Update your configuration file to include a new Store for Expectations
<UpdateYourConfigurationFileToIncludeANewStoreForExpectationsOnS />

### Copy existing Expectation JSON files to the S3 bucket (Optional)
<CopyExistingExpectationJsonFilesToTheSBucketThisStepIsOptional />

### Confirm Expectation Suite availability
<ConfirmList />

</TabItem>
<TabItem value="azure">

## Microsoft Azure Blob Storage

Use the information provided here to configure a new storage location for Expectations in Microsoft Azure Blob Storage.

### Prerequisites

<Prerequisites>

- [A Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context).
- [An Expectations Suite](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data).
- [An Azure Storage account](https://docs.microsoft.com/en-us/azure/storage/).
- An Azure Blob container. If you need to [host and share Data Docs on Azure Blob Storage](../configuring_data_docs/how_to_host_and_share_data_docs_on_azure_blob_storage.md), then you can set this up first and then use the ``$web`` existing container to store your Expectations.
- A prefix (folder) where to store Expectations. You don't need to create the folder, the prefix is just part of the Azure Blob name.

</Prerequisites>
    
### Configure the ``config_variables.yml`` file with your Azure Storage credentials

GX recommends that you store Azure Storage credentials in the ``config_variables.yml`` file, which is located in the ``uncommitted/`` folder by default, and is not part of source control. The following code adds Azure Storage credentials below the ``AZURE_STORAGE_CONNECTION_STRING`` key:

```yaml
AZURE_STORAGE_CONNECTION_STRING: "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>"
```
To learn more about the additional options for configuring the ``config_variables.yml`` file, or additional environment variables, see [How to configure credentials](../../setup/configuring_data_contexts/how_to_configure_credentials.md)

### Identify your Data Context Expectations Store

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

### Update your configuration file to include a new Store for Expectations

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
If the container for [hosting and sharing Data Docs on Azure Blob Storage](../../setup/configuring_data_docs/how_to_host_and_share_data_docs_on_azure_blob_storage.md) is named ``$web``, use ``container: \$web`` to allow access to the ``$web``container.
:::

Additional authentication and configuration options are available. See [Hosting and sharing Data Docs on Azure Blob Storage](../../setup/configuring_data_docs/how_to_host_and_share_data_docs_on_azure_blob_storage.md).

### Copy existing Expectation JSON files to the Azure blob (Optional)

You can use the ``az storage blob upload`` command to copy Expectations into Azure Blob Storage. The following command copies the Expectation ``exp1`` from a local folder to Azure Blob Storage: 

```bash
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>"
az storage blob upload -f <local/path/to/expectation.json> -c <GREAT-EXPECTATION-DEDICATED-AZURE-BLOB-CONTAINER-NAME> -n <PREFIX>/<expectation.json>
example :
az storage blob upload -f great_expectations/expectations/exp1.json -c <blob-container> -n expectations/exp1.json

Finished[#############################################################]  100.0000%
{
"etag": "\"0x8D8E08E5DA47F84\"",
"lastModified": "2021-03-06T10:55:33+00:00"
}
```
To learn more about other methods that are available to copy Expectation JSON files into Azure Blob Storage, see [Introduction to Azure Blob Storage](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction).

### Confirm that the new Expectation Suites have been added

If you copied your existing Expectation Suites to Azure Blob Storage, run the following Python command to confirm that Great Expectations can find them:

<!--A snippet is required for this code block.-->

```python
import great_expectations as gx

context = gx.get_context()
context.list_expectation_suite_names()
```
A list of Expectations you copied to Azure Blob Storage is returned. Expectations that weren't copied to the new folder are not listed.

### Confirm that Expectations can be accessed from Azure Blob Storage

Run the following command to confirm your Expectations have been copied to Azure Blob Storage: 

```bash
great_expectations suite list
```
If your Expectations have not been copied to Azure Blob Storage, the message "No Expectations were found" is returned.

</TabItem>
<TabItem value="gcs">

## GCS

Use the information provided here to configure a new storage location for Expectations in GCS.

To view all the code used in this topic, see [how_to_configure_an_expectation_store_in_gcs.py](https://github.com/great-expectations/great_expectations/tree/develop/tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py).

### Prerequisites

<Prerequisites>

- [A Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context).
- [An Expectations Suite](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data).
- A GCP [service account](https://cloud.google.com/iam/docs/service-accounts) with credentials that allow access to GCP resources such as Storage Objects.
- A GCP project, GCS bucket, and prefix to store Expectations.

</Prerequisites>

### Configure your GCP credentials

Confirm that your environment is configured with the appropriate authentication credentials needed to connect to the GCS bucket where Expectations will be stored. This includes the following:

- A GCP service account.
- Setting the ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable.
- Verifying authentication by running a [Google Cloud Storage client](https://cloud.google.com/storage/docs/reference/libraries) library script.

For more information about validating your GCP authentication credentials, see [Authenticate to Cloud services using client libraries](https://cloud.google.com/docs/authentication/getting-started).

### Identify your Data Context Expectations Store

The configuration for your Expectations <TechnicalTag tag="store" text="Store" /> is available in your <TechnicalTag tag="data_context" text="Data Context" />. Open ``great_expectations.yml`` and find the following entry: 

```yaml name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py expected_existing_expectations_store_yaml"
```

This configuration tells Great Expectations to look for Expectations in the ``expectations_store`` Store. The default ``base_directory`` for ``expectations_store`` is ``expectations/``.

### Update your configuration file to include a new store for Expectations

In the following example, `expectations_store_name` is set to ``expectations_GCS_store``, but it can be personalized.  You also need to change the ``store_backend`` settings. The ``class_name`` is ``TupleGCSStoreBackend``, ``project`` is your GCP project, ``bucket`` is the address of your GCS bucket, and ``prefix`` is the folder on GCS where Expectations are stored.

```yaml name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py configured_expectations_store_yaml"
```

:::warning
If you are also storing [Validations in GCS](./how_to_configure_a_validation_result_store_in_gcs.md) or [DataDocs in GCS](../configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.md), make sure that the ``prefix`` values are disjoint and one is not a substring of the other.
:::

### Copy existing Expectation JSON files to the GCS bucket (Optional)

Use the ``gsutil cp`` command to copy Expectations into GCS. For example, the following command copies the Expectation ```my_expectation_suite`` from a local folder into a GCS bucket:

```bash name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py copy_expectation_command"
```

The following confirmation message is returned:

```bash name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py copy_expectation_output"
```

Additional methods for copying Expectations into GCS are available. See [Upload objects from a filesystem](https://cloud.google.com/storage/docs/uploading-objects).

### Confirm that the new Expectation Suites have been added

If you copied your existing Expectation Suites to GCS, run the following Python command to confirm that Great Expectations can find them:

<!--A snippet is required for this code block.-->

```python
import great_expectations as gx

context = gx.get_context()
context.list_expectation_suite_names()
```

A list of Expectation Suites you copied to GCS is returned. Expectation Suites that weren't copied to the new Store aren't listed.

### Confirm that Expectations can be accessed from GCS

Run the following command to confirm your Expectations were copied to GCS:

```bash name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py list_expectation_suites_command"
```

If your Expectations were not copied to Azure Blob Storage, a message indicating no Expectations were found is returned.

</TabItem>
<TabItem value="filesystem">

## Filesystem

Use the information provided here to configure a new storage location for Expectations on your Filesystem.

### Prerequisites

<Prerequisites>

- [A Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context).
- [An Expectation Suite](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data).
- A storage location for Expectations. This can be a local path, or a path to a network filesystem.
    
</Prerequisites>

### Create a new folder for Expectations

Run the following command to create a new folder for your Expectations and move your existing Expectations to the new folder:

```bash
# in the great_expectations/ folder
mkdir shared_expectations
mv expectations/npi_expectations.json shared_expectations/
```
In this example, the name of the Expectation is ``npi_expectations`` and the path to the new storage location is ``/shared_expectations``.

### Identify your Data Context Expectations Store

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

### Update your configuration file to include a new Store for Expectations results

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

### Confirm that the new Expectation Suites have been added

If you copied your existing Expectation Suites to your filesystem, run the following Python command to confirm that Great Expectations can find them:

<!--A snippet is required for this code block.-->

```python
import great_expectations as gx

context = gx.get_context()
context.list_expectation_suite_names()
```

A list of Expectation Suites you copied your filesystem is returned. Expectation Suites that weren't copied to the new Store aren't listed.

### Version control systems

GX recommends that you store Expectations in a version control system such as Git. The JSON format of Expectations allows for informative diff-statements and modification tracking. In the following example, the ```expect_table_column_count_to_equal`` value changes from ``333`` to ``331``, and then to ``330``:

```bash
git log -p npi_expectations.json

commit cbc127fb27095364c3c1fcbf6e7f078369b07455
  changed expect_table_column_count_to_equal to 331

diff --git a/great_expectations/expectations/npi_expectations.json b/great_expectations/expectations/npi_expectations.json

--- a/great_expectations/expectations/npi_expectations.json
+++ b/great_expectations/expectations/npi_expectations.json
@@ -17,7 +17,7 @@
   {
     "expectation_type": "expect_table_column_count_to_equal",
     "kwargs": {
-        "value": 333
+        "value": 331
     }
commit 05b3c8c1ed35d183bac1717d4877fe13bc574963
changed expect_table_column_count_to_equal to 333

diff --git a/great_expectations/expectations/npi_expectations.json b/great_expectations/expectations/npi_expectations.json
--- a/great_expectations/expectations/npi_expectations.json
+++ b/great_expectations/expectations/npi_expectations.json
   {
     "expectation_type": "expect_table_column_count_to_equal",
     "kwargs": {
-        "value": 330
+        "value": 333
     }
```

</TabItem>
<TabItem value="postgresql">

## PostgreSQL

Use the information provided here to configure an Expectations store in a PostgreSQL database.

### Prerequisites

<Prerequisites>

- [A Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context).
- [An Expectations Suite](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data).
- A [PostgreSQL](https://www.postgresql.org/) database with appropriate credentials.

</Prerequisites>

### Configure the `config_variables.yml` file with your database credentials

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
To configure the ``config_variables.yml`` file, or additional environment variables, see [How to configure credentials](../configuring_data_contexts/how_to_configure_credentials.md).

### Identify your Data Context Expectations Store

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

### Update your configuration file to include a new Store for Expectations

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