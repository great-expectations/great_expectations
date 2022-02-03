---
title: How to host and share Data Docs on GCS
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'


This guide will explain how to host and share Data Docs on Google Cloud Storage. We recommend using IP-based access, which is achieved by deploying a simple Google App Engine app. Data Docs can also be served on Google Cloud Storage if the contents of the bucket are set to be publicly readable, but this is strongly discouraged.

<Prerequisites>

- [Set up a Google Cloud project](https://cloud.google.com/resource-manager/docs/creating-managing-projects)
- [Installed and initialized the Google Cloud SDK (in order to use the gcloud CLI)](https://cloud.google.com/sdk/docs/quickstarts)
- [Set up the gsutil command line tool](https://cloud.google.com/storage/docs/gsutil_install)
- Have permissions to: list and create buckets, deploy Google App Engine apps, add app firewall rules

</Prerequisites>

**Steps**

1. **Create a Google Cloud Storage bucket using gsutil.**

  Make sure you modify the project name, bucket name, and region for your situation.

    ```bash file=../../../../tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py#L37
    ```

    ```bash file=../../../../tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py#L54
    ```

2. **Create a directory for your Google App Engine app and add the following files.**

  We recommend placing it in your project directory, for example ``great_expectations/team_gcs_app``.

  **app.yaml:**

    ```yaml file=../../../../tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py#L63-L65
    ```

  **requirements.txt:**

    ```yaml file=../../../../tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py#L79-L80
    ```

  **main.py:**

    ```python file=../../../../tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py#L88-L117
    ```

3. **If you haven't done so already, authenticate the gcloud CLI and set the project.**

    ```bash file=../../../../tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py#L125
    ```

4. **Deploy your Google App Engine app.**

  Issue the following CLI command from within the app directory created above:

    ```bash file=../../../../tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py#L129
    ```

5. **Set up Google App Engine firewall for your app to control access.**

  Visit the following page for instructions on creating firewall rules: [Creating firewall rules](https://cloud.google.com/appengine/docs/standard/python3/creating-firewalls)

6. **Add a new GCS site to the data_docs_sites section of your great_expectations.yml.**

  You may also replace the default ``local_site`` if you would only like to maintain a single GCS Data Docs site.

    ```yaml file=../../../../tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py#L138-L154
    ```

7. **Build the GCS Data Docs site.**

  Use the following CLI command: 
  
    ```bash file=../../../../tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py#L174
    ```

  If successful, the CLI will provide the object URL of the index page. Since the bucket is not public, this URL will be inaccessible. Rather, you will access the Data Docs site using the App Engine app configured above.

    ```bash file=../../../../tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py#L185-L192
    ```

8. **Test that everything was configured properly by launching your App Engine app.**

  Issue the following CLI command: ``gcloud app browse``. If successful, the gcloud CLI will provide the URL to your app and launch it in a new browser window. The page displayed should be the index page of your Data Docs site.


**Additional notes**

- If you wish to host a Data Docs site through a private DNS, you can configure a ``base_public_path`` for the Data Docs Store.  The following example will configure a GCS site with the ``base_public_path`` set to www.mydns.com .  Data Docs will still be written to the configured location on GCS (for example https://storage.cloud.google.com/my_org_data_docs/index.html), but you will be able to access the pages from your DNS (http://www.mydns.com/index.html in our example).

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

**Additional resources**

- [Google App Engine](https://cloud.google.com/appengine/docs/standard/python3)
- [Controlling App Access with Firewalls](https://cloud.google.com/appengine/docs/standard/python3/creating-firewalls)
- [Core concepts: Data Docs](../../../reference/data_docs.md)
- To view the full script used in this page, see it on GitHub: [how_to_host_and_share_data_docs_on_gcs.py](https://github.com/great-expectations/great_expectations/tree/develop/tests/integration/docusaurus/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.py)

