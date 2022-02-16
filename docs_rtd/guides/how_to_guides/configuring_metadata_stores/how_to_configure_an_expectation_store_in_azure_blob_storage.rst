.. _how_to_guides__configuring_metadata_stores__how_to_configure_an_expectation_store_in_azure_blob_storage:

How to configure an Expectation store in Azure blob storage
===========================================================

By default, newly profiled Expectations are stored in JSON format in the ``expectations/`` subdirectory of your ``great_expectations/`` folder. This guide will help you configure Great Expectations to store them in Azure Blob Storage.


.. admonition:: Prerequisites: This how-to guide assumes that you have already:

    - Configured a :ref:`Data Context <tutorials__getting_started__initialize_a_data_context>`.
    - Configured an :ref:`Expectations Suite <tutorials__getting_started__create_your_first_expectations>`.
    - Configured an Azure `storage account <https://docs.microsoft.com/en-us/azure/storage>`_ and get the `connection string <https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal>`_
    - Create the Azure Blob container. If you also wish to :ref:`host and share data docs on azure blob storage <how_to_guides__configuring_data_docs__how_to_host_and_share_data_docs_on_azure_blob_storage>` then you may setup this first and then use the ``$web`` existing container to store your expectations.
    - Identify the prefix (folder) where Expectations will be stored (you don't need to create the folder, the prefix is just part of the Blob name).
    
Steps
-----

.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        1. **Configure the** ``config_variables.yml`` **file with your azure storage credentials**

            We recommend that azure storage credentials be stored in the  ``config_variables.yml`` file, which is located in the ``uncommitted/`` folder by default, and is not part of source control.  The following lines add azure storage credentials under the key ``AZURE_STORAGE_CONNECTION_STRING``. Additional options for configuring the ``config_variables.yml`` file or additional environment variables can be found `here. <https://docs.greatexpectations.io/en/latest/guides/how_to_guides/configuring_data_contexts/how_to_use_a_yaml_file_or_environment_variables_to_populate_credentials.html>`_

            .. code-block:: yaml

                AZURE_STORAGE_CONNECTION_STRING: "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>"


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


        3. **Update your configuration file to include a new store for Expectations on Azure storage account**

            In our case, the name is set to ``expectations_AZ_store``, but it can be any name you like.  We also need to make some changes to the ``store_backend`` settings.  The ``class_name`` will be set to ``TupleAzureBlobStoreBackend``,  ``container`` will be set to the name of your blob container (the equivalent of S3 bucket for Azure) you wish to store your expectations, ``prefix`` will be set to the folder in the container where Expectation files will be located, and ``connection_string`` will be set to ``${AZURE_STORAGE_CONNECTION_STRING}``, which references the corresponding key in the ``config_variables.yml`` file.

            .. code-block:: yaml

                expectations_store_name: expectations_AZ_store

                stores:
                    expectations_AZ_store:
                        class_name: ExpectationsStore
                        store_backend:
                          class_name: TupleAzureBlobStoreBackend
                          container: <blob-container>
                          prefix: expectations
                          connection_string: ${AZURE_STORAGE_CONNECTION_STRING}

            .. note::
                If the container is called ``$web`` (for :ref:`hosting and sharing data docs on azure blob storage <how_to_guides__configuring_data_docs__how_to_host_and_share_data_docs_on_azure_blob_storage>`) then set ``container: \$web`` so the escape char will allow us to reach the ``$web``container.


        4. **Copy existing Expectation JSON files to the Azure blob**. (This step is optional).

            One way to copy Expectations into Azure Blob Storage is by using the ``az storage blob upload`` command, which is part of the Azure SDK. The following example will copy one Expectation, ``exp1`` from a local folder to the Azure blob.   Information on other ways to copy Expectation JSON files, like the Azure Storage browser in the Azure Portal, can be found in the `Documentation for Azure <https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal>`_.

            .. code-block:: bash

                export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>"
                az storage blob upload -f <local/path/to/expectation.json> -c <GREAT-EXPECTATION-DEDICATED-AZURE-BLOB-CONTAINER-NAME> -n <PREFIX>/<expectation.json>
                example :
                az storage blob upload -f great_expectations/expectations/exp1.json -c <blob-container> -n expectations/exp1.json

                Finished[#############################################################]  100.0000%
                {
                "etag": "\"0x8D8E08E5DA47F84\"",
                "lastModified": "2021-03-06T10:55:33+00:00"
                }


        5. **Confirm that the new Expectations store has been added by running** ``great_expectations store list``

            Notice the output contains two Expectation stores: the original ``expectations_store`` on the local filesystem and the ``expectations_AZ_store`` we just configured.  This is ok, since Great Expectations will look for Expectations in Azure Blob as long as we set the ``expectations_store_name`` variable to ``expectations_AZ_store``, which we did in the previous step.  The config for ``expectations_store`` can be removed if you would like.

            .. code-block:: bash

                great_expectations store list

                 - name: expectations_store
                   class_name: ExpectationsStore
                   store_backend:
                     class_name: TupleFilesystemStoreBackend
                     base_directory: expectations/

                 - name: expectations_AZ_store
                   class_name: ExpectationsStore
                   store_backend:
                     class_name: TupleAzureBlobStoreBackend
                     connection_string: DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>
                     container: <blob-container>
                     prefix: expectations


        6. **Confirm that Expectations can be accessed from Azure Blob Storage by running** ``great_expectations suite list``.

            If you followed Step 4, the output should include the Expectation we copied to Azure Blob: ``exp1``.  If you did not copy Expectations to the new Store, you will see a message saying no Expectations were found.

            .. code-block:: bash

                great_expectations suite list

                Using v2 (Batch Kwargs) API
                1 Expectation Suite found:
                - exp1

    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        1. **Configure the** ``config_variables.yml`` **file with your azure storage credentials**

            We recommend that azure storage credentials be stored in the  ``config_variables.yml`` file, which is located in the ``uncommitted/`` folder by default, and is not part of source control.  The following lines add azure storage credentials under the key ``AZURE_STORAGE_CONNECTION_STRING``. Additional options for configuring the ``config_variables.yml`` file or additional environment variables can be found `here. <https://docs.greatexpectations.io/en/latest/guides/how_to_guides/configuring_data_contexts/how_to_use_a_yaml_file_or_environment_variables_to_populate_credentials.html>`_

            .. code-block:: yaml

                AZURE_STORAGE_CONNECTION_STRING: "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>"


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


        3. **Update your configuration file to include a new store for Expectations on Azure storage account**

            In our case, the name is set to ``expectations_AZ_store``, but it can be any name you like.  We also need to make some changes to the ``store_backend`` settings.  The ``class_name`` will be set to ``TupleAzureBlobStoreBackend``,  ``container`` will be set to the name of your blob container (the equivalent of S3 bucket for Azure) you wish to store your expectations, ``prefix`` will be set to the folder in the container where Expectation files will be located, and ``connection_string`` will be set to ``${AZURE_STORAGE_CONNECTION_STRING}``, which references the corresponding key in the ``config_variables.yml`` file.

            .. code-block:: yaml

                expectations_store_name: expectations_AZ_store

                stores:
                    expectations_AZ_store:
                        class_name: ExpectationsStore
                        store_backend:
                          class_name: TupleAzureBlobStoreBackend
                          container: <blob-container>
                          prefix: expectations
                          connection_string: ${AZURE_STORAGE_CONNECTION_STRING}

            .. note::
                If the container is called ``$web`` (for :ref:`hosting and sharing data docs on azure blob storage <how_to_guides__configuring_data_docs__how_to_host_and_share_data_docs_on_azure_blob_storage>`) then set ``container: \$web`` so the escape char will allow us to reach the ``$web``container.


        4. **Copy existing Expectation JSON files to the Azure blob**. (This step is optional).

            One way to copy Expectations into Azure Blob Storage is by using the ``az storage blob upload`` command, which is part of the Azure SDK. The following example will copy one Expectation, ``exp1`` from a local folder to the Azure blob.   Information on other ways to copy Expectation JSON files, like the Azure Storage browser in the Azure Portal, can be found in the `Documentation for Azure <https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal>`_.

            .. code-block:: bash

                export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>"
                az storage blob upload -f <local/path/to/expectation.json> -c <GREAT-EXPECTATION-DEDICATED-AZURE-BLOB-CONTAINER-NAME> -n <PREFIX>/<expectation.json>
                example :
                az storage blob upload -f great_expectations/expectations/exp1.json -c <blob-container> -n expectations/exp1.json

                Finished[#############################################################]  100.0000%
                {
                "etag": "\"0x8D8E08E5DA47F84\"",
                "lastModified": "2021-03-06T10:55:33+00:00"
                }


        5. **Confirm that the new Expectations store has been added by running** ``great_expectations --v3-api store list``

            Notice the output contains two Expectation stores: the original ``expectations_store`` on the local filesystem and the ``expectations_AZ_store`` we just configured.  This is ok, since Great Expectations will look for Expectations in Azure Blob as long as we set the ``expectations_store_name`` variable to ``expectations_AZ_store``, which we did in the previous step.  The config for ``expectations_store`` can be removed if you would like.

            .. code-block:: bash

                great_expectations --v3-api store list

                 - name: expectations_store
                   class_name: ExpectationsStore
                   store_backend:
                     class_name: TupleFilesystemStoreBackend
                     base_directory: expectations/

                 - name: expectations_AZ_store
                   class_name: ExpectationsStore
                   store_backend:
                     class_name: TupleAzureBlobStoreBackend
                     connection_string: DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>
                     container: <blob-container>
                     prefix: expectations


        6. **Confirm that Expectations can be accessed from Azure Blob Storage by running** ``great_expectations --v3-api suite list``.

            If you followed Step 4, the output should include the Expectation we copied to Azure Blob: ``exp1``.  If you did not copy Expectations to the new Store, you will see a message saying no Expectations were found.

            .. code-block:: bash

                great_expectations --v3-api suite list

                Using v2 (Batch Kwargs) API
                1 Expectation Suite found:
                - exp1


.. discourse::
    :topic_identifier: 179
