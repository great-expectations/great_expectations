.. _how_to_guides__configuring_metadata_stores__how_to_configure_an_expectation_store_in_gcs:

How to configure an Expectation store in GCP
============================================

By default, newly profiled Expectations are stored in JSON format in the ``expectations/`` subdirectory of your ``great_expectations/`` folder.  This guide will help you configure Great Expectations to store them in a Google Cloud Platform (GCP) bucket.

.. admonition:: Prerequisites: This how-to guide assumes that you have already:

    - Configured a :ref:`Data Context <tutorials__getting_started__initialize_a_data_context>`.
    - Configured an :ref:`Expectations Suite <tutorials__getting_started__create_your_first_expectations>`.
    - Configured a GCP `service account <https://cloud.google.com/iam/docs/service-accounts>`_ with credentials that can access the appropriate GCP resources, which include Storage Objects.
    - Identified the GCP project, bucket, and prefix where Expectations will be stored.

1. **Configure local environment with appropriate authentication credentials to connect to the** `Google Cloud API <https://cloud.google.com/docs/authentication/getting-started>`_ **where Expectations will be stored**.

2. **Identify your Data Context Expectations Store**

    In your ``great_expectations.yml`` , look for the following lines.  The configuration tells Great Expectations to look for Expectations in a store called ``expectations_store``. The ``base_directory`` for ``expectations_store`` is set to ``expectations/`` by default.


    .. code-block:: yaml

        expectations_store_name: expectations_store

        stores:
            expectations_store:
                class_name: ExpectationsStore
                store_backend:
                    class_name: TupleFilesystemStoreBackend
                    base_directory: expectations/


3. **Update your configuration file to include a new store for Expectations on GCP.**

    In our case, the name is set to ``expectations_GCP_store``, but it can be any name you like.  We also need to make some changes to the ``store_backend`` settings.  The ``class_name`` will be set to ``TupleGCSStoreBackend``, ``project`` will be set to the GCP project, ``bucket`` will be set to the address of your GCP bucket, and ``prefix`` will be set to the folder where Expectation files will be located.


.. warning::

    If you are also storing :ref:`Validations in GCP, <how_to_guides__configuring_metadata_stores__how_to_configure_a_validation_result_store_in_gcs>` please ensure that the ``prefix`` values are disjoint and one is not a substring of the other.

    .. code-block:: yaml

        expectations_store_name: expectations_GCP_store
        stores:
            expectations_GCP_store:
                class_name: ExpectationsStore
                store_backend:
                    class_name: TupleGCSStoreBackend
                    project: '<your_GCP_project_name>'
                    bucket: '<your_GCP_bucket_name>'
                    prefix: '<your_GCP_folder_name>'


4. **Copy existing Expectation JSON files to the GCP bucket**. (This step is optional).

    One way to copy Expectations into GCP is by using the `gsutil cp` command, which is part of the Google Cloud SDK. The command will copy one Expectation, ``exp1`` from a local folder to the GCP bucket.   Information on other options, like the Cloud Storage browser in the Google Cloud Console, can be found in the `Documentation for Google Cloud <https://cloud.google.com/storage/docs/uploading-objects>`_.

    .. code-block:: bash

        gsutil cp '<OBJECT_LOCATION>' gs://'<your_GCP_bucket_name>'/

        Operation completed over 1 objects/58.8 KiB.



5. **Confirm that the new Expectations store has been added by running** ``great_expectations store list``.

    Notice the output contains two Expectation stores: the original ``expectations_store`` on the local filesystem and the ``expectations_GCP_store`` we just configured.  This is ok, since Great Expectations will look for Expectations in GCP as long as we set the ``expectations_name`` variable to ``expectations_GCP_store``.

    .. code-block:: bash

        great_expectations store list

        - name: expectations_store
        class_name: ExpectationsStore
        store_backend:
            class_name: TupleFilesystemStoreBackend
            base_directory: expectations/

        - name: expectations_GCP_store
        class_name: ExpectationsStore
        store_backend:
            class_name: TupleGCSStoreBackend
            project: '<your_GCP_project_name>'
            bucket: '<your_GCP_bucket_name>'
            prefix: '<your_GCP_folder_name>'


6. **Confirm that Expectations can be accessed from GCP by running** ``great_expectations suite list``.

    If you followed Step 4, The output should include the Expectation we copied to GCP: ``exp1``.  If you did not copy Expectations to the new Store, you will see a message saying no expectations were found.

    .. code-block:: bash

        great_expectations suite list

        1 Expectation Suite found:
         - exp1


Additional resources
--------------------

If it would be useful to you, please comment with a +1 and feel free to add any suggestions or questions below.


.. discourse::
    :topic_identifier: 180
