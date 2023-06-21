---
title: How to host and share Data Docs on GCS
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will explain how to host and share <TechnicalTag relative="../../../" tag="data_docs" text="Data Docs" /> on Google Cloud Storage. We recommend using IP-based access, which is achieved by deploying a simple Google App Engine app. Data Docs can also be served on Google Cloud Storage if the contents of the bucket are set to be publicly readable, but this is strongly discouraged.

## Prerequisites

<Prerequisites>

- [A Google Cloud project](https://cloud.google.com/resource-manager/docs/creating-managing-projects)
- [The Google Cloud SDK](https://cloud.google.com/sdk/docs/quickstarts)
- [The gsutil command line tool](https://cloud.google.com/storage/docs/gsutil_install)
- Permissions to list and create buckets, deploy Google App Engine apps, add app firewall rules

</Prerequisites>

## Steps

### 1. Create a Google Cloud Storage bucket using gsutil

Make sure you modify the project name, bucket name, and region for your situation.

```bash name="tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py create bucket command"
```

```bash name="tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py create bucket output"
```

### 2. Create a directory for your Google App Engine app and add the following files

We recommend placing it in your project directory, for example ``great_expectations/team_gcs_app``.

**app.yaml:**

```yaml name="tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py app yaml"
```

**requirements.txt:**

```yaml name="tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py requirements.txt"
```

**main.py:**

```python name="tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py imports"
```

### 3. If you haven't done so already, authenticate the gcloud CLI and set the project

```bash name="tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py gcloud login and set project"
```

### 4. Deploy your Google App Engine app

Run the following CLI command from within the app directory you created previously:

```bash name="tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py gcloud app deploy"
```

### 5. Set up Google App Engine firewall for your app to control access

Visit the following page for instructions on creating firewall rules: [Creating firewall rules](https://cloud.google.com/appengine/docs/standard/python3/creating-firewalls)

### 6. Add a new GCS site to the data_docs_sites section of your great_expectations.yml

You may also replace the default ``local_site`` if you would only like to maintain a single GCS Data Docs site.

```yaml name="tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py data docs sites yaml"
```

### 7. Build the GCS Data Docs site

Run the following Python code to build and open your Data Docs:

``` python name="tests/integration/docusaurus/reference/glossary/data_docs.py data_docs_site"
```

### 8. Test that everything was configured properly by launching your App Engine app

Issue the following CLI command: ``gcloud app browse``. If successful, the gcloud CLI will provide the URL to your app and launch it in a new browser window. The page displayed should be the index page of your Data Docs site.


## Additional notes

- If you wish to host a Data Docs site through a private DNS, you can configure a ``base_public_path`` for the <TechnicalTag relative="../../../" tag="data_docs_store" text="Data Docs Store" />.  The following example will configure a GCS site with the ``base_public_path`` set to www.mydns.com .  Data Docs will still be written to the configured location on GCS (for example https://storage.cloud.google.com/my_org_data_docs/index.html), but you will be able to access the pages from your DNS (http://www.mydns.com/index.html in our example).

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

## Additional resources

- [Google App Engine](https://cloud.google.com/appengine/docs/standard/python3)
- [Controlling App Access with Firewalls](https://cloud.google.com/appengine/docs/standard/python3/creating-firewalls)
- <TechnicalTag tag="data_docs" text="Data Docs"/>
- To view the full script used in this page, see it on GitHub: [how_to_host_and_share_data_docs_on_gcs.py](https://github.com/great-expectations/great_expectations/tree/develop/tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py)

