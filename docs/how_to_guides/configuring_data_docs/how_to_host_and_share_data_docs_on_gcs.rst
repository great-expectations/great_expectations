.. _how_to_guides__configuring_data_docs__how_to_host_and_share_data_docs_on_gcs:

How to host and share Data Docs on GCS
======================================

By default, Data Docs are stored in HTML format in the ``data_docs/local_site/`` subdirectory of your ``great_expectations/uncommitted/`` folder.  This guide will help you configure Great Expectations to store them in a Google Cloud Storage (GCS) bucket so that they can be easily shared with your team. Since Data Docs may include examples of data (which could be sensitive or regulated) they should not be committed to a source control system, and care should be taken with GCS bucket access control. 

.. admonition:: Prerequisites: This how-to guide assumes that you have already:

    - Configured a :ref:`Data Context <tutorials__getting_started__initialize_a_data_context>`.
    - Configured a Google Cloud Platform (GCP) `service account <https://cloud.google.com/iam/docs/service-accounts>`_ with credentials that can access the appropriate GCP resources, which include Storage Objects.
    - Identified the GCP project, GCS bucket, and prefix where Data Docs will be stored.

1. **Configure your GCP credentials**

    Check that your environment is configured with the appropriate authentication credentials needed to connect to the GCS bucket where Data Docs will be stored.

    The Google Cloud Platform documentation describes how to verify your `authentication for the Google Cloud API <https://cloud.google.com/docs/authentication/getting-started>`_, which includes:

        1. Creating a Google Cloud Platform (GCP) service account,
        2. Setting the ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable,
        3. Verifying authentication by running a simple `Google Cloud Storage client <https://cloud.google.com/storage/docs/reference/libraries>`_ library script.

2. **Identify your Data Context Data Docs Sites**

    In your ``great_expectations.yml``, look for the following lines (comments removed in the snippet below).  This default configuration tells Great Expectations to build the ``local_site`` using the ``SiteBuilder`` and store in your filesystem in the ``store_backend.base_directory``.

    .. code-block:: yaml

        data_docs_sites:
            local_site:
                class_name: SiteBuilder
                show_how_to_buttons: true
                store_backend:
                    class_name: TupleFilesystemStoreBackend
                    base_directory: uncommitted/data_docs/local_site/
                site_index_builder:
                    class_name: DefaultSiteIndexBuilder

3. **Update your configuration file to include a new Data Doc site on GCS**

    In our case, the name is set to ``gcs_site``, but it can be any name you like. We also need to make some changes to the ``store_backend`` settings.  The ``class_name`` will be set to ``TupleGCSStoreBackend``, ``project`` will be set to your GCP project, ``bucket`` will be set to the address of your GCS bucket, and ``prefix`` will be set to the folder on GCS where Data Docs files will be located.


    .. warning::
        If you are also storing :ref:`Validations in GCS <how_to_guides__configuring_metadata_stores__how_to_configure_a_validation_result_store_in_gcs>` or :ref:`Expectations in GCS <how_to_guides__configuring_metadata_stores__how_to_configure_an_expectation_store_in_gcs>`, please ensure that the ``prefix`` values are disjoint and one is not a substring of the other.

    .. code-block:: yaml

        data_docs_sites:
            local_site:
                class_name: SiteBuilder
                show_how_to_buttons: true
                store_backend:
                    class_name: TupleFilesystemStoreBackend
                    base_directory: uncommitted/data_docs/local_site/
                site_index_builder:
                    class_name: DefaultSiteIndexBuilder
            gcs_site:
                class_name: SiteBuilder
                show_how_to_buttons: true
                store_backend:
                    class_name: TupleGCSStoreBackend
                    project: '<your_GCP_project_name>'
                    bucket: '<your_GCS_bucket_name>'
                    prefix: '<your_GCS_folder_name>'
                site_index_builder:
                    class_name: DefaultSiteIndexBuilder


4. **Confirm that the new Data Docs site has been added by running** ``great_expectations docs list``.

    Notice the output contains two Data Docs sites: the original ``local_site`` on the local filesystem and the ``gcs_site`` we just configured.  Data docs will be built in both places. To remove either site, simply remove the configuration from ``great_expectations.yml``. You can build the site with  ``great_expectations docs build --site-name gcs_site``.

    .. code-block:: bash

        great_expectations docs list

        2 Data Docs sites configured:
            - local_site: file:///<your_project_file_path>/great_expectations/uncommitted/data_docs/local_site/index.html
            - gcs_site: site configured but does not exist. Run the following command to build site: great_expectations docs build --site-name gcs_site


.. discourse::
   :topic_identifier: 232
