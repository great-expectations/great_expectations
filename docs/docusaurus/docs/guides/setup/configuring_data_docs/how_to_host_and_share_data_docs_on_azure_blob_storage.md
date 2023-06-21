---
title: How to host and share Data Docs on Azure Blob Storage
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will explain how to host and share <TechnicalTag relative="../../../" tag="data_docs" text="Data Docs" /> on Azure Blob Storage. 
Data Docs will be served using an Azure Blob Storage static website with restricted access.

## Prerequisites

<Prerequisites>

- [A working deployment of Great Expectations](/docs/guides/setup/setup_overview)
- Permissions to create and configure an [Azure Storage account](https://docs.microsoft.com/en-us/azure/storage)

</Prerequisites>

## Steps

## 1. Install Azure Storage Blobs client library for Python

Run the following pip command in a terminal to install Azure Storage Blobs client library and its dependencies:

```markup title="Terminal command:"
pip install azure-storage-blob
```
### 2. Create an Azure Blob Storage static website

- Create a [storage account](https://docs.microsoft.com/en-us/azure/storage).
- In settings select [Static website](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-static-website-host) to display the configuration page for static websites.
- Select **Enabled** to enable static website hosting for the storage account.
- Write "index.html" in the Index document.

Note the Primary endpoint url. Your team will be able to consult your data doc on this url when you have finished this tutorial. You could also map a custom domain to this endpoint.
A container called ``$web`` should have been created in your storage account.


### 3. Configure the ``config_variables.yml`` file with your Azure Storage credentials

Get the [Connection string](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal) of the storage account you have just created.

We recommend that Azure Storage credentials be stored in the ``config_variables.yml`` file, which is located in the ``uncommitted/`` folder by default, and is not part of source control. The following lines add Azure Storage credentials under the key ``AZURE_STORAGE_CONNECTION_STRING``. Additional options for configuring the ``config_variables.yml`` file or additional environment variables can be found [here](../../setup/configuring_data_contexts/how_to_configure_credentials.md).

```yaml
AZURE_STORAGE_CONNECTION_STRING: "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>"
```
   

### 4. Add a new Azure site to the data_docs_sites section of your great_expectations.yml
  
```yaml
data_docs_sites:
  local_site:
    class_name: SiteBuilder
    show_how_to_buttons: true
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: uncommitted/data_docs/local_site/
    site_index_builder:
      class_name: DefaultSiteIndexBuilder
  new_site_name:  # this is a user-selected name - you can select your own
    class_name: SiteBuilder
    store_backend:
       class_name: TupleAzureBlobStoreBackend
       container: \$web
       connection_string: ${AZURE_STORAGE_CONNECTION_STRING}
    site_index_builder:
      class_name: DefaultSiteIndexBuilder
```

You may also replace the default ``local_site`` if you would only like to maintain a single Azure Data Docs site.

:::note
 Since the container is called ``$web``, if we simply set ``container: $web`` in ``great_expectations.yml`` then Great Expectations would unsuccessfully try to find the variable called ``web`` in ``config_variables.yml``. 
 We use an escape char ``\`` before the ``$`` so the [substitute_config_variable](https://legacy.docs.greatexpectations.io/en/latest/autoapi/great_expectations/data_context/util/index.html?highlight=substitute_config_variable#great_expectations.data_context.util.substitute_config_variable) method will allow us to reach the ``$web`` container.
:::

You also may configure Great Expectations to store your <TechnicalTag relative="../../../" tag="expectation" text="Expectations" /> and <TechnicalTag relative="../../../" tag="validation_result" text="Validation Results" /> in this Azure Storage account.
You can follow the documentation from the guides for [Expectations](../../setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_azure_blob_storage.md) and [Validation Results](../../setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_azure_blob_storage.md) but be sure you set ``container: \$web`` in place of the other container name.

The following options are available for this backend:

  * ``container``: The name of the Azure Blob container to store your data in.
  * ``connection_string``: The Azure Storage connection string.
  This can also be supplied by setting the ``AZURE_STORAGE_CONNECTION_STRING`` environment variable.
  * ``prefix``: All paths on blob storage will be prefixed with this string.
  * ``account_url``: The URL to the blob storage account. Any other entities included in the URL path (e.g. container or blob) will be discarded.
  This URL can be optionally authenticated with a SAS token.
  This can only be used if you don't configure the ``connection_string``. 
  You can also configure this by setting the ``AZURE_STORAGE_ACCOUNT_URL`` environment variable.

The most common authentication methods are supported:

* SAS token authentication: append the SAS token to ``account_url`` or make sure it is set in the ``connection_string``.
* Account key authentication: include the account key in the ``connection_string``.
* When none of the above authentication methods are specified, the [DefaultAzureCredential](https://docs.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential?view=azure-python) will be used which supports most common authentication methods.
  You still need to provide the account url either through the config file or environment variable.


### 5. Build the Azure Blob Data Docs site

You can create or modify an <TechnicalTag tag="expectation_suite" text="Expectation Suite" /> and this will build the Data Docs website.

Run the following Python code to build and open your Data Docs:

``` python name="tests/integration/docusaurus/reference/glossary/data_docs.py data_docs_site"
```

### 6. Limit the access to your company

- On your Azure Storage Account Settings click on **Networking**
- Allow access from **Selected networks**
- You can add access to Virtual Network
- You can add IP ranges to the firewall 

More details are available [here](https://docs.microsoft.com/en-us/azure/storage/common/storage-network-security?tabs=azure-portal).
