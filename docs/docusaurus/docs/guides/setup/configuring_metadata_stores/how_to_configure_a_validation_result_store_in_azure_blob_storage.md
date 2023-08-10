---
title: How to configure a Validation Result Store in Azure Blob Storage
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx'

By default, <TechnicalTag tag="validation_result" text="Validation Results" /> are stored in JSON format in the ``uncommitted/validations/`` subdirectory of your ``great_expectations/`` folder. Validation Results might include sensitive or regulated data that should not be committed to a source control system. Use the information provided here to configure a new storage location for Validation Results in Azure Blob Storage.

## Prerequisites

<Prerequisites>

- [A Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context).
- [An Expectations Suite](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data).
- [A Checkpoint](/docs/guides/validation/checkpoints/how_to_create_a_new_checkpoint).
- [An Azure Storage account](https://docs.microsoft.com/en-us/azure/storage) and get the [connection string](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal).
- An Azure Blob container. If you want to [host and share Data Docs on Azure Blob Storage](../../../guides/setup/configuring_data_docs/host_and_share_data_docs.md), you can set this up first and then use the ``$web`` existing container to store your <TechnicalTag tag="expectation" text="Expectations" />.
- A prefix (folder) to store Validation Results. You don't need to create the folder, the prefix is just part of the Blob name.

</Prerequisites>

## 1. Configure the ``config_variables.yml`` file with your Azure Storage credentials

GX recommends that you store Azure Storage credentials in the ``config_variables.yml`` file, which is located in the ``uncommitted/`` folder by default, and is not part of source control. The following code adds Azure Storage credentials under the key ``AZURE_STORAGE_CONNECTION_STRING``: 

```yaml
AZURE_STORAGE_CONNECTION_STRING: "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>"
```

To learn more about the additional options for configuring the ``config_variables.yml`` file, or additional environment variables, see [How to configure credentials](../../setup/configuring_data_contexts/how_to_configure_credentials.md)

## 2. Identify your Validation Results Store

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

## 3. Update your configuration file to include a new Store for Validation Results on Azure Storage account

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
If the container for [hosting and sharing Data Docs on Azure Blob Storage](../../setup/configuring_data_docs/host_and_share_data_docs.md) is named ``$web``, use ``container: \$web`` to allow access to the ``$web``container.
:::

Additional authentication and configuration options are available. See [Hosting and sharing Data Docs on Azure Blob Storage](../../setup/configuring_data_docs/host_and_share_data_docs.md).

## 4. Copy existing Validation Results JSON files to the Azure blob (Optional)

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

## 5. Reference the new configuration

To make Great Expectations look for Validation Results on the Azure store, set the ``validations_store_name`` variable to the name of your Azure Validations Store. In the previous example this was `validations_AZ_store`.

## 6. Confirm that the Validation Results Store has been correctly configured

[Run a Checkpoint](/docs/guides/validation/checkpoints/how_to_create_a_new_checkpoint) to store results in the new Validation Results Store on Azure Blob and then visualize the results by [re-building Data Docs](../../../terms/data_docs.md).

