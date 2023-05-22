---
title: How to configure an Expectation Store to use Azure Blob Storage
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

By default, newly <TechnicalTag tag="profiling" text="Profiled" /> <TechnicalTag tag="expectation" text="Expectations" /> are stored as <TechnicalTag tag="expectation_suite" text="Expectation Suites" /> in JSON format in the ``expectations/`` subdirectory of your ``great_expectations/`` folder. This guide will help you configure Great Expectations to store them in Azure Blob Storage.

## Prerequisites

<Prerequisites>

- [A Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context).
- [An Expectations Suite](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data).
- [An Azure Storage account](https://docs.microsoft.com/en-us/azure/storage/).
- An Azure Blob container. If you need to [host and share Data Docs on Azure Blob Storage](../configuring_data_docs/how_to_host_and_share_data_docs_on_azure_blob_storage.md), then you can set this up first and then use the ``$web`` existing container to store your Expectations.
- A prefix (folder) where to store Expectations. You don't need to create the folder, the prefix is just part of the Blob name.

</Prerequisites>
    

## Steps

### 1. Configure the ``config_variables.yml`` file with your Azure Storage credentials

We recommend that Azure Storage credentials be stored in the  ``config_variables.yml`` file, which is located in the ``uncommitted/`` folder by default, and is not part of source control.  The following lines add Azure Storage credentials under the key ``AZURE_STORAGE_CONNECTION_STRING``. Additional options for configuring the ``config_variables.yml`` file or additional environment variables can be found [here](https://docs.greatexpectations.io/docs/guides/setup/configuring_data_contexts/how_to_configure_credentials_using_a_yaml_file_or_environment_variables).

```yaml
AZURE_STORAGE_CONNECTION_STRING: "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>"
```


### 2. Identify your Data Context Expectations Store

In your ``great_expectations.yml`` , look for the following lines.  The configuration tells Great Expectations to look for Expectations in a <TechnicalTag tag="store" text="Store" /> called ``expectations_store``. The ``base_directory`` for ``expectations_store`` is set to ``expectations/`` by default.

```yaml
expectations_store_name: expectations_store

stores:
  expectations_store:
      class_name: ExpectationsStore
      store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: expectations/
```


### 3. Update your configuration file to include a new Store for Expectations on Azure Storage account

In our case, the name is set to ``expectations_AZ_store``, but it can be any name you like.  We also need to make some changes to the ``store_backend`` settings.  The ``class_name`` will be set to ``TupleAzureBlobStoreBackend``,  ``container`` will be set to the name of your blob container (the equivalent of S3 bucket for Azure) you wish to store your expectations, ``prefix`` will be set to the folder in the container where Expectation files will be located, and ``connection_string`` will be set to ``${AZURE_STORAGE_CONNECTION_STRING}``, which references the corresponding key in the ``config_variables.yml`` file.

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
If the container is called ``$web`` (for [hosting and sharing Data Docs on Azure Blob Storage](../configuring_data_docs/how_to_host_and_share_data_docs_on_azure_blob_storage.md)) then set ``container: \$web`` so the escape char will allow us to reach the ``$web``container.
:::

:::note
Various authentication and configuration options are available as documented in [hosting and sharing Data Docs on Azure Blob Storage](../../setup/configuring_data_docs/how_to_host_and_share_data_docs_on_azure_blob_storage.md).
:::


### 4. Copy existing Expectation JSON files to the Azure blob (This step is optional)

One way to copy Expectations into Azure Blob Storage is by using the ``az storage blob upload`` command, which is part of the Azure SDK. The following example will copy one Expectation, ``exp1`` from a local folder to the Azure blob.   Information on other ways to copy Expectation JSON files, like the Azure Storage browser in the Azure Portal, can be found in the [Documentation for Azure](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction).

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


### 5. Confirm that the new Expectation Suites have been added

If you followed the optional step to copy your existing Expectation Suites to Azure blob storage, you can confirm that Great Expectations can find them by running the following Python code:

```python
import great_expectations as gx

context = gx.get_context()
context.list_expectation_suite_names()
```

Your output should include the Expectations you copied to the Azure blob. Expectations that weren't copied to the new are not listed.

### 6. Confirm that Expectations can be accessed from Azure Blob Storage by running ``great_expectations suite list``

If you followed Step 4, the output should include the Expectation we copied to Azure Blob: ``exp1``.  If you did not copy Expectations to the new Store, you will see a message saying no Expectations were found.

```bash
great_expectations suite list
```
