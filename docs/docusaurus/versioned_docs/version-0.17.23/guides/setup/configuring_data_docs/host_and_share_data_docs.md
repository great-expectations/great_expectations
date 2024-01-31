---
sidebar_label: "Host and share Data Docs"
title: "Host and share Data Docs"
id: host_and_share_data_docs
description: Host and share Data Docs stored on a filesystem or a source data system.
toc_min_heading_level: 2
toc_max_heading_level: 2
---

import Preface from './components_how_to_host_and_share_data_docs_on_amazon_s3/_preface.mdx'
import CreateAnS3Bucket from './components_how_to_host_and_share_data_docs_on_amazon_s3/_create_an_s3_bucket.mdx'
import ConfigureYourBucketPolicyToEnableAppropriateAccess from './components_how_to_host_and_share_data_docs_on_amazon_s3/_configure_your_bucket_policy_to_enable_appropriate_access.mdx'
import ApplyThePolicy from './components_how_to_host_and_share_data_docs_on_amazon_s3/_apply_the_policy.mdx'
import AddANewS3SiteToTheDataDocsSitesSectionOfYourGreatExpectationsYml from './components_how_to_host_and_share_data_docs_on_amazon_s3/_add_a_new_s3_site_to_the_data_docs_sites_section_of_your_great_expectationsyml.mdx'
import TestThatYourConfigurationIsCorrectByBuildingTheSite from './components_how_to_host_and_share_data_docs_on_amazon_s3/_test_that_your_configuration_is_correct_by_building_the_site.mdx'
import AdditionalNotes from './components_how_to_host_and_share_data_docs_on_amazon_s3/_additional_notes.mdx'
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

Data Docs translate Expectations, Validation Results, and other metadata into human-readable documentation. Automatically compiling your data documentation from your data tests in the form of Data Docs keeps your documentation current. Use the information provided here to host and share Data Docs stored on a filesystem or a source data system.

<Tabs
  groupId="host-and-share-data-docs"
  defaultValue='amazon'
  values={[
  {label: 'Amazon S3', value:'amazon'},
  {label: 'Microsoft Azure Blob Storage', value:'azure'},
  {label: 'Google Cloud Service', value:'gcs'},
  {label: 'Filesystem', value:'filesystem'},
  ]}>
<TabItem value="amazon">

## Amazon S3

<Preface />

### Create an S3 bucket
<CreateAnS3Bucket />

### Configure your bucket policy
<ConfigureYourBucketPolicyToEnableAppropriateAccess />

### Apply the policy
<ApplyThePolicy />

### Add a new S3 site to `great_expectations.yml`
<AddANewS3SiteToTheDataDocsSitesSectionOfYourGreatExpectationsYml />

### Test your configuration
<TestThatYourConfigurationIsCorrectByBuildingTheSite />

### Additional notes
<AdditionalNotes />

</TabItem>
<TabItem value="azure">

## Microsoft Azure Blob Storage

Host and share <TechnicalTag relative="../../../" tag="data_docs" text="Data Docs" /> on Azure Blob Storage. Data Docs are served using an Azure Blob Storage static website with restricted access.

### Prerequisites

<Prerequisites>

- [A working deployment of Great Expectations](/docs/guides/setup/setup_overview)
- Permissions to create and configure an [Azure Storage account](https://docs.microsoft.com/en-us/azure/storage)

</Prerequisites>

### Install Azure Storage Blobs client library for Python

Run the following pip command in a terminal to install Azure Storage Blobs client library and its dependencies:

```markup title="Terminal command:"
pip install azure-storage-blob
```

### Create an Azure Blob Storage static website

1. Create a [storage account](https://docs.microsoft.com/en-us/azure/storage).

2. In **Settings**, select [Static website](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-static-website-host).

3. Select **Enabled** to enable static website hosting for the storage account.

4. Write "index.html" in the Index document.

5. Record the Primary endpoint URL. Your team will use this URL to view the Data Doc. A container named ``$web`` is added to your storage account to help you map a custom domain to this endpoint.

### Configure the ``config_variables.yml`` file

GX recommends storing Azure Storage credentials in the ``config_variables.yml`` file, which is located in the ``uncommitted/`` folder by default, and is not part of source control.

To review additional options for configuring the ``config_variables.yml`` file or additional environment variables, see [Configure credentials](../../setup/configuring_data_contexts/how_to_configure_credentials.md).

1. Get the [Connection string](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal) of the storage account you created.

2. Open the ``config_variables.yml`` file and then add the following entry below ``AZURE_STORAGE_CONNECTION_STRING``: 

    ```yaml
    AZURE_STORAGE_CONNECTION_STRING: "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>"
    ```
   
### Add a new Azure site to the data_docs_sites section of your great_expectations.yml

1. Open the ``great_expectations.yml`` file and add the following entry:
  
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

2. Optional. Replace the default ``local_site`` to maintain a single Azure Data Docs site.

:::note
 Since the container is named ``$web``, setting ``container: $web`` in ``great_expectations.yml`` would cause GX to unsuccessfully try to find the ``web`` variable in ``config_variables.yml``. Use an escape char ``\`` before the ``$`` so the [substitute_config_variable](https://legacy.docs.greatexpectations.io/en/latest/autoapi/great_expectations/data_context/util/index.html?highlight=substitute_config_variable#great_expectations.data_context.util.substitute_config_variable) can locate the ``$web`` container.
:::

You can also configure GX to store your <TechnicalTag relative="../../../" tag="expectation" text="Expectations" /> and <TechnicalTag relative="../../../" tag="validation_result" text="Validation Results" /> in the Azure Storage account.
See [Configure Expectation Stores](../../setup/configuring_metadata_stores/configure_expectation_stores.md) and [Configure Validation Result Stores](../../setup/configuring_metadata_stores/configure_result_stores.md). Make sure you set ``container: \$web`` correctly.

The following options are available:

  * ``container``: The name of the Azure Blob container to store your data in.
  * ``connection_string``: The Azure Storage connection string.
  This can also be supplied by setting the ``AZURE_STORAGE_CONNECTION_STRING`` environment variable.
  * ``prefix``: All paths on blob storage will be prefixed with this string.
  * ``account_url``: The URL to the blob storage account. Any other entities included in the URL path (e.g. container or blob) will be discarded.
  This URL can be optionally authenticated with a SAS token.
  This can only be used if you don't configure the ``connection_string``. 
  You can also configure this by setting the ``AZURE_STORAGE_ACCOUNT_URL`` environment variable.

The following authentication methods are supported:

* SAS token authentication: append the SAS token to ``account_url`` or make sure it is set in the ``connection_string``.
* Account key authentication: include the account key in the ``connection_string``.
* When none of the above authentication methods are specified, the [DefaultAzureCredential](https://docs.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential?view=azure-python) will be used which supports most common authentication methods.
  You still need to provide the account url either through the config file or environment variable.

### Build the Azure Blob Data Docs site

You can create or modify an <TechnicalTag tag="expectation_suite" text="Expectation Suite" /> and this will build the Data Docs website.

Run the following Python code to build and open your Data Docs:

``` python name="tests/integration/docusaurus/reference/glossary/data_docs.py data_docs_site"
```

### Limit Data Docs access (Optional)

1. On your Azure Storage Account Settings, click **Networking**.

2. Allow access from **Selected networks**.

3. Optional. Add access to a Virtual Network.

4. Optional. Add IP ranges to the firewall. See [Configure Azure Storage firewalls and virtual networks](https://docs.microsoft.com/en-us/azure/storage/common/storage-network-security?tabs=azure-portal).

</TabItem>
<TabItem value="gcs">

## GCS

Host and share <TechnicalTag relative="../../../" tag="data_docs" text="Data Docs" /> on Google Cloud Storage (GCS). GX recommends using IP-based access, which is achieved by deploying a Google App Engine application.

To view the code used in the examples, see [how_to_host_and_share_data_docs_on_gcs.py](https://github.com/great-expectations/great_expectations/tree/develop/tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py).

### Prerequisites

<Prerequisites>

- [A Google Cloud project](https://cloud.google.com/resource-manager/docs/creating-managing-projects)
- [The Google Cloud SDK](https://cloud.google.com/sdk/docs/quickstarts)
- [The gsutil command line tool](https://cloud.google.com/storage/docs/gsutil_install)
- Permissions to list and create buckets, deploy Google App Engine apps, add app firewall rules

</Prerequisites>

### Create a GCS bucket

Run the following command to create a GCS bucket: 

```bash name="tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py create bucket command"
```

Modify the project name, bucket name, and region.

This is the output after you run the command:

```bash name="tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py create bucket output"
```

### Create a directory for your Google App Engine app

GX recommends adding the directory to your project directory. For example, `great_expectations/team_gcs_app`.

1. Create and then open ``app.yaml`` and then add the following entry:

    ```yaml name="tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py app yaml"
    ```

2. Create and then open `requirements.txt` and then add the following entry:

    ```yaml name="tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py requirements.txt"
    ```

3. Create and then open `main.py` and then dd the following entry:

    ```python name="tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py imports"
    ```

### Authenticate the gcloud CLI

Run the following command to authenticate the gcloud CLI and set the project:

```bash name="tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py gcloud login and set project"
```

### Deploy your Google App Engine app

Run the following CLI command from within the app directory you created previously:

```bash name="tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py gcloud app deploy"
```

### Set up the Google App Engine firewall

See [Creating firewall rules](https://cloud.google.com/appengine/docs/standard/python3/creating-firewalls).

### Add a new GCS site to the data_docs_sites section of your great_expectations.yml

Open `great_expectations.yml` and add the following entry:

```yaml name="tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py data docs sites yaml"
```

Replace the default ``local_site`` to maintain a single GCS Data Docs site.

To host a Data Docs site with a private DNS, you can configure a ``base_public_path`` for the <TechnicalTag relative="../../../" tag="data_docs_store" text="Data Docs Store" />.  The following example configures a GCS site with the ``base_public_path`` set to www.mydns.com.  Data Docs are still written to the configured location on GCS. For example, https://storage.cloud.google.com/my_org_data_docs/index.html, but you will be able to access the pages from your DNS (http://www.mydns.com/index.html in the following example).

  ```yaml
  data_docs_sites:
    gs_site:  # this is a user-selected name - you may select your own
      class_name: SiteBuilder
      store_backend:
        class_name: TupleGCSStoreBackend
        project: <YOUR GCP PROJECT NAME>
        bucket: <YOUR GCS BUCKET NAME>
        base_public_path: http://www.mydns.com
      site_index_builder:
        class_name: DefaultSiteIndexBuilder
  ```

### Build the GCS Data Docs site

Run the following Python code to build and open your Data Docs:

``` python name="tests/integration/docusaurus/reference/glossary/data_docs.py data_docs_site"
```

### Test the configuration

In the gcloud CLI run ``gcloud app browse``. If the command runs successfully, the URL is provided to your app and launched in a new browser window. The page displayed is the index page for your Data Docs site.

### Related documentation

- [Google App Engine](https://cloud.google.com/appengine/docs/standard/python3)
- [Controlling App Access with Firewalls](https://cloud.google.com/appengine/docs/standard/python3/creating-firewalls)

</TabItem>
<TabItem value="filesystem">

## Filesystem

Host and share <TechnicalTag relative="../../../" tag="data_docs" text="Data Docs" /> on a filesystem.

### Prerequisites

<Prerequisites>

- [A Great Expectations instance](/docs/guides/setup/setup_overview)

</Prerequisites>

### Review the default settings

Filesystem-hosted Data Docs are configured by default for Great Expectations deployments created using great_expectations init.  To create additional Data Docs sites, you may re-use the default Data Docs configuration below. You may replace ``local_site`` with your own site name, or leave the default.

```yaml
data_docs_sites:
  local_site:  # this is a user-selected name - you may select your own
    class_name: SiteBuilder
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: uncommitted/data_docs/local_site/ # this is the default path but can be changed as required
    site_index_builder:
      class_name: DefaultSiteIndexBuilder
```

### Build the site

Run the following Python code to build and open your Data Docs:

``` python name="tests/integration/docusaurus/reference/glossary/data_docs.py data_docs"
```

To share the site, compress and distribute the directory in the ``base_directory`` key in your site configuration.

</TabItem>
</Tabs>