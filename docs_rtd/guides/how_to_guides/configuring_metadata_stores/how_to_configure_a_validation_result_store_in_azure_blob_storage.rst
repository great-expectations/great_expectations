.. _how_to_guides__configuring_metadata_stores__how_to_configure_a_validation_result_store_in_azure_blob_storage:

How to configure a Validation Result store in Azure blob storage
================================================================

By default, Validations are stored in JSON format in the ``uncommitted/validations/`` subdirectory of your ``great_expectations/`` folder.  Since Validations may include examples of data (which could be sensitive or regulated) they should not be committed to a source control system. This guide will help you configure a new storage location for Validations in a Azure Blob Storage.


.. admonition:: Prerequisites: This how-to guide assumes that you have already:

    - Configured a :ref:`Data Context <tutorials__getting_started__initialize_a_data_context>`.
    - Configured an :ref:`Expectations Suite <tutorials__getting_started__create_your_first_expectations>`.
    - Configured a :ref:`Checkpoint <tutorials__getting_started__validate_your_data>`.
    - Configured an Azure `storage account <https://docs.microsoft.com/en-us/azure/storage>`_ and get the `connection string <https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal>`_
    - Create the Azure Blob container. If you also wish to :ref:`host and share data docs on azure blob storage <how_to_guides__configuring_data_docs__how_to_host_and_share_data_docs_on_azure_blob_storage>` then you may setup this first and then use the ``$web`` existing container to store your expectations.
    - Identify the prefix (folder) where Validations will be stored (you don't need to create the folder, the prefix is just part of the Blob name).

Steps
-----
.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        1. **Configure the** ``config_variables.yml`` **file with your azure storage credentials**

            We recommend that azure storage credentials be stored in the  ``config_variables.yml`` file, which is located in the ``uncommitted/`` folder by default, and is not part of source control.  The following lines add azure storage credentials under the key ``AZURE_STORAGE_CONNECTION_STRING``. Additional options for configuring the ``config_variables.yml`` file or additional environment variables can be found `here. <https://docs.greatexpectations.io/en/latest/guides/how_to_guides/configuring_data_contexts/how_to_use_a_yaml_file_or_environment_variables_to_populate_credentials.html>`_

            .. code-block:: yaml

                AZURE_STORAGE_CONNECTION_STRING: "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>"


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


        3. **Update your configuration file to include a new store for Validations on Azure storage account**

            In our case, the name is set to ``validations_AZ_store``, but it can be any name you like.  We also need to make some changes to the ``store_backend`` settings.  The ``class_name`` will be set to ``TupleAzureBlobStoreBackend``,  ``container`` will be set to the name of your blob container (the equivalent of S3 bucket for Azure) you wish to store your validations, ``prefix`` will be set to the folder in the container where Validation files will be located, and ``connection_string`` will be set to ``${AZURE_STORAGE_CONNECTION_STRING}``, which references the corresponding key in the ``config_variables.yml`` file.

            .. code-block:: yaml

                validations_store_name: validations_AZ_store

                stores:
                    validations_AZ_store:
                        class_name: ValidationsStore
                        store_backend:
                            class_name: TupleAzureBlobStoreBackend
                            container: <blob-container>
                            prefix: validations
                            connection_string: ${AZURE_STORAGE_CONNECTION_STRING}

            .. note::
                If the container is called ``$web`` (for :ref:`hosting and sharing data docs on azure blob storage <how_to_guides__configuring_data_docs__how_to_host_and_share_data_docs_on_azure_blob_storage>`) then set ``container: \$web`` so the escape char will allow us to reach the ``$web``container.

        4. **Copy existing Validations JSON files to the Azure blob**. (This step is optional).

            One way to copy Validations into Azure Blob Storage is by using the ``az storage blob upload`` command, which is part of the Azure SDK. The following example will copy one Validation from a local folder to the Azure blob.   Information on other ways to copy Validation JSON files, like the Azure Storage browser in the Azure Portal, can be found in the `Documentation for Azure <https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal>`_.

            .. code-block:: bash

                export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>"
                az storage blob upload -f <local/path/to/validation.json> -c <GREAT-EXPECTATION-DEDICATED-AZURE-BLOB-CONTAINER-NAME> -n <PREFIX>/<validation.json>
                example with a validation related to the exp1 expectation:
                az storage blob upload -f great_expectations/uncommitted/validations/exp1/20210306T104406.877327Z/20210306T104406.877327Z/8313fb37ca59375eb843adf388d4f882.json -c <blob-container> -n validations/exp1/20210306T104406.877327Z/20210306T104406.877327Z/8313fb37ca59375eb843adf388d4f882.json
                Finished[#############################################################]  100.0000%
                {
                  "etag": "\"0x8D8E09F894650C7\"",
                  "lastModified": "2021-03-06T12:58:28+00:00"
                }


        5. **Confirm that the new Validations store has been added by running** ``great_expectations store list``.

            Notice the output contains two Validation stores: the original ``validations_store`` on the local filesystem and the ``validations_AZ_store`` we just configured.  This is ok, since Great Expectations will look for Validations in Azure Blob as long as we set the ``validations_store_name`` variable to ``validations_AZ_store``, and the config for ``validations_store`` can be removed if you would like.


            .. code-block:: bash

                great_expectations store list

                 - name: validations_store
                   class_name: ValidationsStore
                   store_backend:
                     class_name: TupleFilesystemStoreBackend
                     base_directory: uncommitted/validations/

                 - name: validations_AZ_store
                   class_name: ValidationsStore
                   store_backend:
                     class_name: TupleAzureBlobStoreBackend
                     connection_string: "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>"
                     container: <blob-container>
                     prefix: validations
             
    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        1. **Configure the** ``config_variables.yml`` **file with your azure storage credentials**

            We recommend that azure storage credentials be stored in the  ``config_variables.yml`` file, which is located in the ``uncommitted/`` folder by default, and is not part of source control.  The following lines add azure storage credentials under the key ``AZURE_STORAGE_CONNECTION_STRING``. Additional options for configuring the ``config_variables.yml`` file or additional environment variables can be found `here. <https://docs.greatexpectations.io/en/latest/guides/how_to_guides/configuring_data_contexts/how_to_use_a_yaml_file_or_environment_variables_to_populate_credentials.html>`_

            .. code-block:: yaml

                AZURE_STORAGE_CONNECTION_STRING: "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>"


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


        3. **Update your configuration file to include a new store for Validations on Azure storage account**

            In our case, the name is set to ``validations_AZ_store``, but it can be any name you like.  We also need to make some changes to the ``store_backend`` settings.  The ``class_name`` will be set to ``TupleAzureBlobStoreBackend``,  ``container`` will be set to the name of your blob container (the equivalent of S3 bucket for Azure) you wish to store your validations, ``prefix`` will be set to the folder in the container where Validation files will be located, and ``connection_string`` will be set to ``${AZURE_STORAGE_CONNECTION_STRING}``, which references the corresponding key in the ``config_variables.yml`` file.

            .. code-block:: yaml

                validations_store_name: validations_AZ_store

                stores:
                    validations_AZ_store:
                        class_name: ValidationsStore
                        store_backend:
                            class_name: TupleAzureBlobStoreBackend
                            container: <blob-container>
                            prefix: validations
                            connection_string: ${AZURE_STORAGE_CONNECTION_STRING}

            .. note::
                If the container is called ``$web`` (for :ref:`hosting and sharing data docs on azure blob storage <how_to_guides__configuring_data_docs__how_to_host_and_share_data_docs_on_azure_blob_storage>`) then set ``container: \$web`` so the escape char will allow us to reach the ``$web``container.

        4. **Copy existing Validations JSON files to the Azure blob**. (This step is optional).

            One way to copy Validations into Azure Blob Storage is by using the ``az storage blob upload`` command, which is part of the Azure SDK. The following example will copy one Validation from a local folder to the Azure blob.   Information on other ways to copy Validation JSON files, like the Azure Storage browser in the Azure Portal, can be found in the `Documentation for Azure <https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal>`_.

            .. code-block:: bash

                export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>"
                az storage blob upload -f <local/path/to/validation.json> -c <GREAT-EXPECTATION-DEDICATED-AZURE-BLOB-CONTAINER-NAME> -n <PREFIX>/<validation.json>
                example with a validation related to the exp1 expectation:
                az storage blob upload -f great_expectations/uncommitted/validations/exp1/20210306T104406.877327Z/20210306T104406.877327Z/8313fb37ca59375eb843adf388d4f882.json -c <blob-container> -n validations/exp1/20210306T104406.877327Z/20210306T104406.877327Z/8313fb37ca59375eb843adf388d4f882.json
                Finished[#############################################################]  100.0000%
                {
                  "etag": "\"0x8D8E09F894650C7\"",
                  "lastModified": "2021-03-06T12:58:28+00:00"
                }


        5. **Confirm that the new Validations store has been added by running** ``great_expectations --v3-api store list``.

            Notice the output contains two Validation stores: the original ``validations_store`` on the local filesystem and the ``validations_AZ_store`` we just configured.  This is ok, since Great Expectations will look for Validations in Azure Blob as long as we set the ``validations_store_name`` variable to ``validations_AZ_store``, and the config for ``validations_store`` can be removed if you would like.


            .. code-block:: bash

                great_expectations --v3-api store list

                 - name: validations_store
                   class_name: ValidationsStore
                   store_backend:
                     class_name: TupleFilesystemStoreBackend
                     base_directory: uncommitted/validations/

                 - name: validations_AZ_store
                   class_name: ValidationsStore
                   store_backend:
                     class_name: TupleAzureBlobStoreBackend
                     connection_string: "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=<YOUR-STORAGE-ACCOUNT-NAME>;AccountKey=<YOUR-STORAGE-ACCOUNT-KEY==>"
                     container: <blob-container>
                     prefix: validations


6. **Confirm that the Validations store has been correctly configured.**

    Run a :ref:`Checkpoint <tutorials__getting_started__validate_your_data>` to store results in the new Validations store on Azure Blob then visualize the results by re-building :ref:`Data Docs <tutorials__getting_started__set_up_data_docs>`.


.. discourse::
    :topic_identifier: 173
