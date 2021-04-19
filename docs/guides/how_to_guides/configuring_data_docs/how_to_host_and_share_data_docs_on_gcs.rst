.. _how_to_guides__configuring_data_docs__how_to_host_and_share_data_docs_on_gcs:

How to host and share Data Docs on GCS
======================================

This guide will explain how to host and share Data Docs on Google Cloud Storage.  We recommend using IP-based access, which is achieved by deploying a simple Google App Engine app.  Data Docs can also be served on Google Cloud Storage if the contents of the bucket are set to be publicly readable, but this is strongly discouraged.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

    - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
    - `Set up a Google Cloud project <https://cloud.google.com/resource-manager/docs/creating-managing-projects>`_
    - `Installed and initialized the Google Cloud SDK (in order to use the gcloud CLI) <https://cloud.google.com/sdk/docs/quickstarts>`_
    - `Set up the gsutil command line tool <https://cloud.google.com/storage/docs/gsutil_install>`_
    - Have permissions to: list and create buckets, deploy Google App Engine apps, add app firewall rules

**Steps**

.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        1. **Create a Google Cloud Storage bucket using gsutil.**

          Make sure you modify the project name, bucket name, and region for your situation.

          .. code-block:: bash

            > gsutil mb -p my_org_project -l US-EAST1 -b on gs://my_org_data_docs/
            Creating gs://my_org_data_docs/...

        2. **Create a directory for your Google App Engine app and add the following files.**

          | We recommend placing it in your project directory, for example ``great_expectations/team_gcs_app``.

          .. code-block:: yaml
            :linenos:
            :caption: app.yaml (**make sure to use your own bucket name**)

            runtime: python37
            env_variables:
                CLOUD_STORAGE_BUCKET: my_org_data_docs

          .. code-block::
            :linenos:
            :caption: requirements.txt

            flask>=1.1.0
            google-cloud-storage

          .. code-block:: python
            :linenos:
            :caption: main.py

            import logging
            import os
            from flask import Flask, request
            from google.cloud import storage
            app = Flask(__name__)
            # Configure this environment variable via app.yaml
            CLOUD_STORAGE_BUCKET = os.environ['CLOUD_STORAGE_BUCKET']
            @app.route('/', defaults={'path': 'index.html'})
            @app.route('/<path:path>')
            def index(path):
                gcs = storage.Client()
                bucket = gcs.get_bucket(CLOUD_STORAGE_BUCKET)
                try:
                    blob = bucket.get_blob(path)
                    content = blob.download_as_string()
                    if blob.content_encoding:
                        resource = content.decode(blob.content_encoding)
                    else:
                        resource = content
                except Exception as e:
                    logging.exception("couldn't get blob")
                    resource = "<p></p>"
                return resource
            @app.errorhandler(500)
            def server_error(e):
                logging.exception('An error occurred during a request.')
                return """
                An internal error occurred: <pre>{}</pre>
                See logs for full stacktrace.
                """.format(e), 500

        3. **If you haven't done so already, authenticate the gcloud CLI and set the project.**

          .. code-block:: bash
            :caption: Insert the appropriate project name.

            > gcloud auth login && gcloud config set project <<project_name>>

        4. **Deploy your Google App Engine app.**

          Issue the following CLI command from within the app directory created above: ``gcloud app deploy``.

        5. **Set up Google App Engine firewall for your app to control access.**

          Visit the following page for instructions on creating firewall rules: `Creating firewall rules <https://cloud.google.com/appengine/docs/standard/python3/creating-firewalls>`_

        6. **Add a new GCS site to the data_docs_sites section of your great_expectations.yml.**

          You may also replace the default ``local_site`` if you would only like to maintain a single GCS Data Docs site.

          .. code-block:: yaml
            :linenos:

            data_docs_sites:
              local_site:
                class_name: SiteBuilder
                show_how_to_buttons: true
                store_backend:
                  class_name: TupleFilesystemStoreBackend
                  base_directory: uncommitted/data_docs/local_site/
                site_index_builder:
                  class_name: DefaultSiteIndexBuilder
              gs_site:  # this is a user-selected name - you may select your own
                class_name: SiteBuilder
                store_backend:
                  class_name: TupleGCSStoreBackend
                  project: my_org_project # UPDATE the project name with your own
                  bucket: my_org_data_docs  # UPDATE the bucket name here to match the bucket you configured above
                site_index_builder:
                  class_name: DefaultSiteIndexBuilder

        7. **Build the GCS Data Docs site.**

          Use the following CLI command: ``great_expectations docs build --site-name gs_site``. If successful, the CLI will provide the object URL of the index page. Since the bucket is not public, this URL will be inaccessible. Rather, you will access the Data Docs site using the App Engine app configured above.

          .. code-block:: bash

            > great_expectations docs build --site-name gs_site

            The following Data Docs sites will be built:

             - gs_site: https://storage.googleapis.com/my_org_data_docs/index.html

            Would you like to proceed? [Y/n]: Y

            Building Data Docs...

            Done building Data Docs

    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        1. **Create a Google Cloud Storage bucket using gsutil.**

          Make sure you modify the project name, bucket name, and region for your situation.

          .. code-block:: bash

            > gsutil mb -p my_org_project -l US-EAST1 -b on gs://my_org_data_docs/
            Creating gs://my_org_data_docs/...

        2. **Create a directory for your Google App Engine app and add the following files.**

          | We recommend placing it in your project directory, for example ``great_expectations/team_gcs_app``.

          .. code-block:: yaml
            :linenos:
            :caption: app.yaml (**make sure to use your own bucket name**)

            runtime: python37
            env_variables:
                CLOUD_STORAGE_BUCKET: my_org_data_docs

          .. code-block::
            :linenos:
            :caption: requirements.txt

            flask>=1.1.0
            google-cloud-storage

          .. code-block:: python
            :linenos:
            :caption: main.py

            import logging
            import os
            from flask import Flask, request
            from google.cloud import storage
            app = Flask(__name__)
            # Configure this environment variable via app.yaml
            CLOUD_STORAGE_BUCKET = os.environ['CLOUD_STORAGE_BUCKET']
            @app.route('/', defaults={'path': 'index.html'})
            @app.route('/<path:path>')
            def index(path):
                gcs = storage.Client()
                bucket = gcs.get_bucket(CLOUD_STORAGE_BUCKET)
                try:
                    blob = bucket.get_blob(path)
                    content = blob.download_as_string()
                    if blob.content_encoding:
                        resource = content.decode(blob.content_encoding)
                    else:
                        resource = content
                except Exception as e:
                    logging.exception("couldn't get blob")
                    resource = "<p></p>"
                return resource
            @app.errorhandler(500)
            def server_error(e):
                logging.exception('An error occurred during a request.')
                return """
                An internal error occurred: <pre>{}</pre>
                See logs for full stacktrace.
                """.format(e), 500

        3. **If you haven't done so already, authenticate the gcloud CLI and set the project.**

          .. code-block:: bash
            :caption: Insert the appropriate project name.

            > gcloud auth login && gcloud config set project <<project_name>>

        4. **Deploy your Google App Engine app.**

          Issue the following CLI command from within the app directory created above: ``gcloud app deploy``.

        5. **Set up Google App Engine firewall for your app to control access.**

          Visit the following page for instructions on creating firewall rules: `Creating firewall rules <https://cloud.google.com/appengine/docs/standard/python3/creating-firewalls>`_

        6. **Add a new GCS site to the data_docs_sites section of your great_expectations.yml.**

          You may also replace the default ``local_site`` if you would only like to maintain a single GCS Data Docs site.

          .. code-block:: yaml
            :linenos:

            data_docs_sites:
              local_site:
                class_name: SiteBuilder
                show_how_to_buttons: true
                store_backend:
                  class_name: TupleFilesystemStoreBackend
                  base_directory: uncommitted/data_docs/local_site/
                site_index_builder:
                  class_name: DefaultSiteIndexBuilder
              gs_site:  # this is a user-selected name - you may select your own
                class_name: SiteBuilder
                store_backend:
                  class_name: TupleGCSStoreBackend
                  project: my_org_project # UPDATE the project name with your own
                  bucket: my_org_data_docs  # UPDATE the bucket name here to match the bucket you configured above
                site_index_builder:
                  class_name: DefaultSiteIndexBuilder

        7. **Build the GCS Data Docs site.**

          Use the following CLI command: ``great_expectations --v3-api docs build --site-name gs_site``. If successful, the CLI will provide the object URL of the index page. Since the bucket is not public, this URL will be inaccessible. Rather, you will access the Data Docs site using the App Engine app configured above.

          .. code-block:: bash

            > great_expectations --v3-api docs build --site-name gs_site

            The following Data Docs sites will be built:

             - gs_site: https://storage.googleapis.com/my_org_data_docs/index.html

            Would you like to proceed? [Y/n]: Y

            Building Data Docs...

            Done building Data Docs



8. **Test that everything was configured properly by launching your App Engine app.**

  Issue the following CLI command: ``gcloud app browse``. If successful, the gcloud CLI will provide the URL to your app and launch it in a new browser window. The page displayed should be the index page of your Data Docs site.


**Additional notes**

- If you wish to host a Data Docs site through a private DNS, you can configure a ``base_public_path`` for the Data Docs Store.  The following example will configure a GCS site with the ``base_public_path`` set to ``www.mydns.com``.  Data Docs will still be written to the configured location on GCS (for example ``https://storage.cloud.google.com/my_org_data_docs/index.html``), but you will be able to access the pages from your DNS (``http://www.mydns.com/index.html`` in our example).

.. code-block:: yaml

    data_docs_sites:
      gs_site:  # this is a user-selected name - you may select your own
        class_name: SiteBuilder
        store_backend:
          class_name: TupleGCSStoreBackend
          project: my_org_project
          bucket: my_org_data_docs
          base_public_path: http://www.mydns.com
        site_index_builder:
          class_name: DefaultSiteIndexBuilder



**Additional resources**

- `Google App Engine <https://cloud.google.com/appengine/docs/standard/python3>`_
- `Controlling App Access with Firewalls <https://cloud.google.com/appengine/docs/standard/python3/creating-firewalls>`_
- :ref:`Core concepts: Data Docs <data_docs>`


Comments
--------

.. discourse::
   :topic_identifier: 232