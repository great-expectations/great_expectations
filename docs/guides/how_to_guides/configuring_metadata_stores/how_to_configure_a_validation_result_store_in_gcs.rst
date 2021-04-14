.. _how_to_guides__configuring_metadata_stores__how_to_configure_a_validation_result_store_in_gcs:

How to configure a Validation Result store in GCS
=================================================

By default, Validations are stored in JSON format in the ``uncommitted/validations/`` subdirectory of your ``great_expectations/`` folder.  Since Validations may include examples of data (which could be sensitive or regulated) they should not be committed to a source control system.  This guide will help you configure a new storage location for Validations in a Google Cloud Storage (GCS) bucket.

.. admonition:: Prerequisites: This how-to guide assumes that you have already:

    - Configured a :ref:`Data Context <tutorials__getting_started__initialize_a_data_context>`.
    - Configured an :ref:`Expectations Suite <tutorials__getting_started__create_your_first_expectations>`.
    - Configured a :ref:`Checkpoint <tutorials__getting_started__validate_your_data>`.
    - Configured a Google Cloud Platform (GCP) `service account <https://cloud.google.com/iam/docs/service-accounts>`_ with credentials that can access the appropriate GCP resources, which include Storage Objects.
    - Identified the GCP project, GCS bucket, and prefix where Validations will be stored.

Steps
-----
.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        1. **Configure your GCP credentials**

            Check that your environment is configured with the appropriate authentication credentials needed to connect to the GCS bucket where Validations will be stored.

            The Google Cloud Platform documentation describes how to verify your `authentication for the Google Cloud API <https://cloud.google.com/docs/authentication/getting-started>`_, which includes:

                1. Creating a Google Cloud Platform (GCP) service account,
                2. Setting the ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable,
                3. Verifying authentication by running a simple `Google Cloud Storage client <https://cloud.google.com/storage/docs/reference/libraries>`_ library script.

        2. **Identify your Data Context Validations Store**

            In your ``great_expectations.yml``, look for the following lines.  The configuration tells Great Expectations to look for Validations in a store called ``validations_store``. The ``base_directory`` for ``validations_store`` is set to ``uncommitted/validations/`` by default.

            .. code-block:: yaml

                validations_store_name: validations_store

                stores:
                    validations_store:
                        class_name: ValidationsStore
                        store_backend:
                            class_name: TupleFilesystemStoreBackend
                            base_directory: uncommitted/validations/


        3. **Update your configuration file to include a new store for Validations on GCS**

            In our case, the name is set to ``validations_GCS_store``, but it can be any name you like.  We also need to make some changes to the ``store_backend`` settings.  The ``class_name`` will be set to ``TupleGCSStoreBackend``, ``project`` will be set to your GCP project, ``bucket`` will be set to the address of your GCS bucket, and ``prefix`` will be set to the folder on GCS where Validation files will be located.


            .. warning::
                If you are also storing :ref:`Expectations in GCS <how_to_guides__configuring_metadata_stores__how_to_configure_an_expectation_store_in_gcs>` or :ref:`DataDocs in GCS <how_to_guides__configuring_data_docs__how_to_host_and_share_data_docs_on_gcs>`, please ensure that the ``prefix`` values are disjoint and one is not a substring of the other.

            .. code-block:: yaml

                validations_store_name: validations_GCS_store
                stores:
                    validations_GCS_store:
                        class_name: ValidationsStore
                        store_backend:
                            class_name: TupleGCSStoreBackend
                            project: '<your_GCP_project_name>'
                            bucket: '<your_GCS_bucket_name>'
                            prefix: '<your_GCS_folder_name>'


        4. **Copy existing Validation results to the GCS bucket**. (This step is optional).

            One way to copy Validations into GCS is by using the ``gsutil cp`` command, which is part of the Google Cloud SDK. In the example below, two Validation results, ``Validation1`` and ``Validation2`` are copied to the GCS bucket.   Information on other ways to copy Validation results, like the Cloud Storage browser in the Google Cloud Console, can be found in the `Documentation for Google Cloud <https://cloud.google.com/storage/docs/uploading-objects>`_.

            .. code-block:: bash

                gsutil cp uncommitted/validations/Validation1.json gs://'<your_GCS_bucket_name>'/'<your_GCS_folder_name>'
                gsutil cp uncommitted/validations/Validation2.json gs://'<your_GCS_bucket_name>'/'<your_GCS_folder_name>'

                Operation completed over 2 objects/58.8 KiB.



        5. **Confirm that the new Validations store has been added by running** ``great_expectations store list``.

            Notice the output contains two Validation stores: the original ``validations_store`` on the local filesystem and the ``validations_GCS_store`` we just configured.  This is ok, since Great Expectations will look for Validations in GCS as long as we set the ``validations_store_name`` variable to ``validations_GCS_store``, and the config for ``validations_store`` can be removed if you would like.

            .. code-block:: bash

                great_expectations store list

                - name: validations_store
                class_name: ValidationsStore
                store_backend:
                    class_name: TupleFilesystemStoreBackend
                    base_directory: uncommitted/validations/

                - name: validations_GCS_store
                class_name: ValidationsStore
                store_backend:
                    class_name: TupleGCSStoreBackend
                    project: '<your_GCP_project_name>'
                    bucket: '<your_GCS_bucket_name>'
                    prefix: '<your_GCS_folder_name>'

    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        1. **Configure your GCP credentials**

            Check that your environment is configured with the appropriate authentication credentials needed to connect to the GCS bucket where Validations will be stored.

            The Google Cloud Platform documentation describes how to verify your `authentication for the Google Cloud API <https://cloud.google.com/docs/authentication/getting-started>`_, which includes:

                1. Creating a Google Cloud Platform (GCP) service account,
                2. Setting the ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable,
                3. Verifying authentication by running a simple `Google Cloud Storage client <https://cloud.google.com/storage/docs/reference/libraries>`_ library script.

        2. **Identify your Data Context Validations Store**

            In your ``great_expectations.yml``, look for the following lines.  The configuration tells Great Expectations to look for Validations in a store called ``validations_store``. The ``base_directory`` for ``validations_store`` is set to ``uncommitted/validations/`` by default.

            .. code-block:: yaml

                validations_store_name: validations_store

                stores:
                    validations_store:
                        class_name: ValidationsStore
                        store_backend:
                            class_name: TupleFilesystemStoreBackend
                            base_directory: uncommitted/validations/


        3. **Update your configuration file to include a new store for Validations on GCS**

            In our case, the name is set to ``validations_GCS_store``, but it can be any name you like.  We also need to make some changes to the ``store_backend`` settings.  The ``class_name`` will be set to ``TupleGCSStoreBackend``, ``project`` will be set to your GCP project, ``bucket`` will be set to the address of your GCS bucket, and ``prefix`` will be set to the folder on GCS where Validation files will be located.


            .. warning::
                If you are also storing :ref:`Expectations in GCS <how_to_guides__configuring_metadata_stores__how_to_configure_an_expectation_store_in_gcs>` or :ref:`DataDocs in GCS <how_to_guides__configuring_data_docs__how_to_host_and_share_data_docs_on_gcs>`, please ensure that the ``prefix`` values are disjoint and one is not a substring of the other.

            .. code-block:: yaml

                validations_store_name: validations_GCS_store
                stores:
                    validations_GCS_store:
                        class_name: ValidationsStore
                        store_backend:
                            class_name: TupleGCSStoreBackend
                            project: '<your_GCP_project_name>'
                            bucket: '<your_GCS_bucket_name>'
                            prefix: '<your_GCS_folder_name>'


        4. **Copy existing Validation results to the GCS bucket**. (This step is optional).

            One way to copy Validations into GCS is by using the ``gsutil cp`` command, which is part of the Google Cloud SDK. In the example below, two Validation results, ``Validation1`` and ``Validation2`` are copied to the GCS bucket.   Information on other ways to copy Validation results, like the Cloud Storage browser in the Google Cloud Console, can be found in the `Documentation for Google Cloud <https://cloud.google.com/storage/docs/uploading-objects>`_.

            .. code-block:: bash

                gsutil cp uncommitted/validations/Validation1.json gs://'<your_GCS_bucket_name>'/'<your_GCS_folder_name>'
                gsutil cp uncommitted/validations/Validation2.json gs://'<your_GCS_bucket_name>'/'<your_GCS_folder_name>'

                Operation completed over 2 objects/58.8 KiB.



        5. **Confirm that the new Validations store has been added by running** ``great_expectations --v3-api store list``.

            Notice the output contains two Validation stores: the original ``validations_store`` on the local filesystem and the ``validations_GCS_store`` we just configured.  This is ok, since Great Expectations will look for Validations in GCS as long as we set the ``validations_store_name`` variable to ``validations_GCS_store``, and the config for ``validations_store`` can be removed if you would like.

            .. code-block:: bash

                great_expectations --v3-api store list

                - name: validations_store
                class_name: ValidationsStore
                store_backend:
                    class_name: TupleFilesystemStoreBackend
                    base_directory: uncommitted/validations/

                - name: validations_GCS_store
                class_name: ValidationsStore
                store_backend:
                    class_name: TupleGCSStoreBackend
                    project: '<your_GCP_project_name>'
                    bucket: '<your_GCS_bucket_name>'
                    prefix: '<your_GCS_folder_name>'


6. **Confirm that the Validations store has been correctly configured.**

    Run a :ref:`Checkpoint <tutorials__getting_started__validate_your_data>` to store results in the new Validations store on GCS then visualize the results by re-building :ref:`Data Docs <tutorials__getting_started__set_up_data_docs>`.


If it would be useful to you, please comment with a +1 and feel free to add any suggestions or questions below.  Also, please reach out to us on `Slack <https://greatexpectations.io/slack>`_ if you would like to learn more, or have any questions.

.. discourse::
    :topic_identifier: 174
