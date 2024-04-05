Use the information provided here to configure a new storage location for Expectations in Microsoft Azure Blob Storage.

### Prerequisites

- [A Data Context](/core/installation_and_setup/manage_data_contexts.md).
- [An Expectations Suite](/core/create_expectations/expectation_suites/manage_expectation_suites.md).
- [An Azure Storage account](https://docs.microsoft.com/en-us/azure/storage/).
- An Azure Blob container.
- A prefix (folder) where to store Expectations. You don't need to create the folder, the prefix is just part of the Azure Blob name.

### Configure the ``config_variables.yml`` file with your Azure Storage credentials

GX recommends that you store Azure Storage credentials in the ``config_variables.yml`` file, which is located in the ``uncommitted/`` folder by default, and is not part of source control. The following code adds Azure Storage credentials below the ``AZURE_STORAGE_CONNECTION_STRING`` key:

```yaml
AZURE_STORAGE_CONNECTION_STRING: "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>"
```
To learn more about the additional options for configuring the ``config_variables.yml`` file, or additional environment variables, see [Manage credentials](/core/installation_and_setup/manage_credentials.md).

### Identify your Data Context Expectations Store

Your Expectations Store configuration is provided in your Data Context. Open ``great_expectations.yml`` and find the following entry:

```yaml
expectations_store_name: expectations_store

stores:
  expectations_store:
      class_name: ExpectationsStore
      store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: expectations/
```

This configuration tells GX Core to look for Expectations in a Store named ``expectations_store``. The default ``base_directory`` for ``expectations_store`` is ``expectations/``.

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
If the container for hosting and sharing Data Docs on Azure Blob Storage is named ``$web``, use ``container: \$web`` to allow access to the ``$web``container.
:::

### Copy existing Expectation JSON files to the Azure blob (Optional)

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

### Confirm that the new Expectation Suites have been added

If you copied your existing Expectation Suites to Azure Blob Storage, run the following Python command to confirm that GX Core can find them:

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