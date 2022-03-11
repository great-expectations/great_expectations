---
title: How to host and share Data Docs on Azure Blob Storage
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '/docs/term_tags/_tag.mdx';

This guide will explain how to host and share <TechnicalTag relative="../../../" tag="data_docs" text="Data Docs" /> on Azure Blob Storage. 
Data Docs will be served using an Azure Blob Storage static website with restricted access.

<Prerequisites>

- [Set up a working deployment of Great Expectations](../../../tutorials/getting_started/intro.md)
- Have permission to create and configured an [Azure Storage account](https://docs.microsoft.com/en-us/azure/storage)

</Prerequisites>
    
   
## Steps

### 1. Create an Azure Blob Storage static website

- Create a [storage account](https://docs.microsoft.com/en-us/azure/storage).
- In settings select [Static website](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-static-website-host) to display the configuration page for static websites.
- Select **Enabled** to enable static website hosting for the storage account.
- Write "index.html" in Index document.

Note the Primary endpoint url. Your team will be able to consult your data doc on this url when you have finished this tuto. You could also map a custom domain to this endpoint.
A container called ``$web`` should have been created in your storage account.


### 2. Configure the ``config_variables.yml`` file with your Azure Storage credentials

Get the [Connection string](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal) of the storage account you have just created.

We recommend that Azure Storage credentials be stored in the ``config_variables.yml`` file, which is located in the ``uncommitted/`` folder by default, and is not part of source control. The following lines add Azure Storage credentials under the key ``AZURE_STORAGE_CONNECTION_STRING``. Additional options for configuring the ``config_variables.yml`` file or additional environment variables can be found [here](../../setup/configuring_data_contexts/how_to_configure_credentials.md).

```yaml
AZURE_STORAGE_CONNECTION_STRING: "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>"
```
   

### 3. Add a new Azure site to the data_docs_sites section of your great_expectations.yml
  
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
  az_site:  # this is a user-selected name - you may select your own
    class_name: SiteBuilder
    store_backend:
       class_name: TupleAzureBlobStoreBackend
       container: \$web
       connection_string: ${AZURE_STORAGE_WEB_CONNECTION_STRING}
    site_index_builder:
      class_name: DefaultSiteIndexBuilder
```

You may also replace the default ``local_site`` if you would only like to maintain a single Azure Data Docs site.

:::note
 Since the container is called ``$web``, if we simply set ``container: $web`` in ``great_expectations.yml`` then Great Expectations would unsuccefully try to find the variable called ``web`` in ``config_variables.yml``. 
 We use an escape char ``\`` before the ``$`` so the [substitute_config_variable](https://legacy.docs.greatexpectations.io/en/latest/autoapi/great_expectations/data_context/util/index.html?highlight=substitute_config_variable#great_expectations.data_context.util.substitute_config_variable) method will allow us to reach the ``$web`` container.
:::

You also may configure Great Expectations to store your <TechnicalTag relative="../../../" tag="expectation" text="Expectations" /> and <TechnicalTag relative="../../../" tag="validation_result" text="Validation Results" /> in this Azure Storage account.
You can follow the documentation from the guides for [Expectations](../../setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_azure_blob_storage.md) and [Validation Results](../../setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_azure_blob_storage.md) but unsure you set ``container: \$web`` inplace of other container name.


### 4. Build the Azure Blob Data Docs site

You can create or modify an <TechnicalTag tag="expectation_suite" text="Expectation Suite" /> and this will build the Data Docs website.
Or you can use the following <TechnicalTag relative="../../../" tag="cli" text="CLI" /> command: ``great_expectations docs build --site-name az_site``.

```bash
> great_expectations docs build --site-name az_site

 The following Data Docs sites will be built:

 - az_site: https://<your-storage-account>.blob.core.windows.net/$web/index.html

 Would you like to proceed? [Y/n]: y

 Building Data Docs...

 Done building Data Docs
```

If successful, the CLI will provide the object URL of the index page. 
You may secure the access of your website using an IP filtering mechanism.


### 5. Limit the access to your company

- On your Azure Storage Account Settings click on **Networking**
- Allow access from **Selected networks**
- You can add access to Virtual Network
- You can add IP ranges to the firewall 

More details are available [here](https://docs.microsoft.com/en-us/azure/storage/common/storage-network-security?tabs=azure-portal).
