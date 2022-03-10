---
title: How to configure a Validation Result Store in Azure Blob Storage
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx'

By default, <TechnicalTag tag="validation_result" text="Validation Results" /> are stored in JSON format in the ``uncommitted/validations/`` subdirectory of your ``great_expectations/`` folder.  Since Validation Results may include examples of data (which could be sensitive or regulated) they should not be committed to a source control system. This guide will help you configure a new storage location for Validation Results in Azure Blob Storage.

<Prerequisites>

- [Configured a Data Context](../../../tutorials/getting_started/initialize_a_data_context.md).
- [Configured an Expectations Suite](../../../tutorials/getting_started/create_your_first_expectations.md).
- [Configured a Checkpoint](../../../tutorials/getting_started/validate_your_data.md).
- [Configured an Azure Storage account](https://docs.microsoft.com/en-us/azure/storage) and get the [connection string](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal).
- Create the Azure Blob container. If you also wish to [host and share Data Docs on Azure Blob Storage](../../../guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_azure_blob_storage.md) then you may set up this first and then use the ``$web`` existing container to store your <TechnicalTag tag="expectation" text="Expectations" />.
- Identify the prefix (folder) where Validation Results will be stored (you don't need to create the folder, the prefix is just part of the Blob name).

</Prerequisites>

## Steps

### 1. Configure the ``config_variables.yml`` file with your Azure Storage credentials

We recommend that Azure Storage credentials be stored in the ``config_variables.yml`` file, which is located in the ``uncommitted/`` folder by default, and is not part of source control. The following lines add Azure Storage credentials under the key ``AZURE_STORAGE_CONNECTION_STRING``. Additional options for configuring the ``config_variables.yml`` file or additional environment variables can be found [here](../../setup/configuring_data_contexts/how_to_configure_credentials.md).

```yaml
AZURE_STORAGE_CONNECTION_STRING: "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>"
```


### 2. Identify your Validation Results Store

As with all <TechnicalTag tag="store" text="Stores" />, you can find the configuration for your <TechnicalTag tag="validation_result_store" text="Validation Results Store" /> through your <TechnicalTag tag="data_context" text="Data Context" />.  In your ``great_expectations.yml``, look for the following lines.  The configuration tells Great Expectations to look for Validation Results in a store called ``validations_store``. The ``base_directory`` for ``validations_store`` is set to ``uncommitted/validations/`` by default.

```yaml
validations_store_name: validations_store

stores:
  validations_store:
      class_name: ValidationsStore
      store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: uncommitted/validations/
```


### 3. Update your configuration file to include a new Store for Validation Results on Azure Storage account

In our case, the name is set to ``validations_AZ_store``, but it can be any name you like.  We also need to make some changes to the ``store_backend`` settings.  The ``class_name`` will be set to ``TupleAzureBlobStoreBackend``,  ``container`` will be set to the name of your blob container (the equivalent of S3 bucket for Azure) you wish to store your Validation Results, ``prefix`` will be set to the folder in the container where Validation files will be located, and ``connection_string`` will be set to ``${AZURE_STORAGE_CONNECTION_STRING}``, which references the corresponding key in the ``config_variables.yml`` file.

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
If the container is called ``$web`` (for [hosting and sharing Data Docs on Azure Blob Storage](../../setup/configuring_data_docs/how_to_host_and_share_data_docs_on_azure_blob_storage.md)) then set ``container: \$web`` so the escape char will allow us to reach the ``$web``container.
:::

### 4. Copy existing Validation Results JSON files to the Azure blob (This step is optional)

One way to copy Validation Results into Azure Blob Storage is by using the ``az storage blob upload`` command, which is part of the Azure SDK. The following example will copy one Validation Result from a local folder to the Azure blob.   Information on other ways to copy Validation Result JSON files, like the Azure Storage browser in the Azure Portal, can be found in the [Documentation for Azure](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal).

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

### 5. Confirm that the new Validation Results Store has been added by running ``great_expectations store list``

Notice the output contains two Validation stores: the original ``validations_store`` on the local filesystem and the ``validations_AZ_store`` we just configured.  This is ok, since Great Expectations will look for Validation Results in Azure Blob as long as we set the ``validations_store_name`` variable to ``validations_AZ_store``, and the config for ``validations_store`` can be removed if you would like.

```bash
great_expectations store list

- name: validations_store
 class_name: ValidationsStore
 store_backend:
   class_name: TupleFilesystemStoreBackend
   base_directory: uncommitted/validations/

- name: validations_AZ_store
 class_name: ValidationsStore
 store_backend:
   class_name: TupleAzureBlobStoreBackend
   connection_string: "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>"
   container: <blob-container>
   prefix: validations
```


### 6. Confirm that the Validation Results Store has been correctly configured

[Run a Checkpoint](../../../tutorials/getting_started/validate_your_data.md) to store results in the new Validation Results Store on Azure Blob then visualize the results by [re-building Data Docs](../../../tutorials/getting_started/check_out_data_docs.md).

