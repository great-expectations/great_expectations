---
title: How to configure an Expectation Store to use Azure Blob Storage
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

By default, new <TechnicalTag tag="profiling" text="Profiled" /> <TechnicalTag tag="expectation" text="Expectations" /> are stored as <TechnicalTag tag="expectation_suite" text="Expectation Suites" /> in JSON format in the ``expectations/`` subdirectory of your ``great_expectations/`` folder.  Use the information provided here to configure a new storage location for Expectations in Azure Blob Storage.

## Prerequisites

<Prerequisites>

- [A Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context).
- [An Expectations Suite](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data).
- [An Azure Storage account](https://docs.microsoft.com/en-us/azure/storage/).
- An Azure Blob container. If you need to [host and share Data Docs on Azure Blob Storage](../configuring_data_docs/host_and_share_data_docs.md), then you can set this up first and then use the ``$web`` existing container to store your Expectations.
- A prefix (folder) where to store Expectations. You don't need to create the folder, the prefix is just part of the Azure Blob name.

</Prerequisites>
    
## 1. Configure the ``config_variables.yml`` file with your Azure Storage credentials

GX recommends that you store Azure Storage credentials in the ``config_variables.yml`` file, which is located in the ``uncommitted/`` folder by default, and is not part of source control. The following code adds Azure Storage credentials below the ``AZURE_STORAGE_CONNECTION_STRING`` key:

```yaml
AZURE_STORAGE_CONNECTION_STRING: "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>"
```
To learn more about the additional options for configuring the ``config_variables.yml`` file, or additional environment variables, see [How to configure credentials](../../setup/configuring_data_contexts/how_to_configure_credentials.md)

## 2. Identify your Data Context Expectations Store

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

## 3. Update your configuration file to include a new Store for Expectations

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
If the container for [hosting and sharing Data Docs on Azure Blob Storage](../../setup/configuring_data_docs/host_and_share_data_docs.md) is named ``$web``, use ``container: \$web`` to allow access to the ``$web``container.
:::

Additional authentication and configuration options are available. See [Hosting and sharing Data Docs on Azure Blob Storage](../../setup/configuring_data_docs/host_and_share_data_docs.md).

## 4. Copy existing Expectation JSON files to the Azure blob (Optional)

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

## 5. Confirm that the new Expectation Suites have been added

If you copied your existing Expectation Suites to Azure Blob Storage, run the following Python command to confirm that Great Expectations can find them:

<!--A snippet is required for this code block.-->

```python
import great_expectations as gx

context = gx.get_context()
context.list_expectation_suite_names()
```
A list of Expectations you copied to Azure Blob Storage is returned. Expectations that weren't copied to the new folder are not listed.

## 6. Confirm that Expectations can be accessed from Azure Blob Storage

Run the following command to confirm your Expectations have been copied to Azure Blob Storage: 

```bash
great_expectations suite list
```
If your Expectations have not been copied to Azure Blob Storage, the message "No Expectations were found" is returned.
