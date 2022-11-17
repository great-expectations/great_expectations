.. _how_to_guides__configuring_metadata_stores__how_to_configure_a_validation_result_store_on_a_filesystem:

How to configure a Validation Result store on a filesystem
==========================================================

By default, Validation results are stored in the ``uncommitted/validations/`` directory.  Since Validations may include examples of data (which could be sensitive or regulated) they should not be committed to a source control system.  This guide will help you configure a new storage location for Validations on your filesystem.

.. admonition:: Prerequisites: This how-to guide assumes that you have already:

    - Configured a :ref:`Data Context <tutorials__getting_started__initialize_a_data_context>`.
    - Configured an :ref:`Expectation Suite <tutorials__getting_started__create_your_first_expectations>`.
    - Configured a :ref:`Checkpoint <tutorials__getting_started__validate_your_data>`.
    - Determined a new storage location where you would like to store Validations. This can either be a local path, or a path to a secure network filesystem.

Steps
-----

.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        1. **Configure a new folder on your filesystem where Validation results will be stored.**

            Create a new folder where you would like to store your Validation results, and move your existing Validation results over to the new location. In our case, the name of the Validation result is ``npi_validations`` and the path to our new storage location is ``shared_validations/``.

            .. code-block:: bash

                # in the great_expectations/ folder
                mkdir shared_validations
                mv uncommitted/validations/npi_validations/ uncommitted/shared_validations/


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



        3. **Update your configuration file to include a new store for Validation results on your filesystem**

            In the example below, Validations Store is being set to ``shared_validations_filesystem_store``, but it can be any name you like.  Also, the ``base_directory`` is being set to ``uncommitted/shared_validations/``, but it can be set to any path accessible by Great Expectations.

            .. code-block:: yaml

                validations_store_name: shared_validations_filesystem_store

                stores:
                    shared_validations_filesystem_store:
                        class_name: ValidationsStore
                        store_backend:
                            class_name: TupleFilesystemStoreBackend
                            base_directory: uncommitted/shared_validations/


        4. **Confirm that the location has been updated by running** ``great_expectations store list``.

            Notice the output contains two Validation stores: the original ``validations_store`` and the ``shared_validations_filesystem_store`` we just configured.  This is ok, since Great Expectations will look for Validations in the ``uncommitted/shared_validations/`` folder as long as we set the ``validations_store_name`` variable to ``shared_validations_filesystem_store``. The config for ``validations_store`` can be removed if you would like.

            .. code-block:: bash

                great_expectations store list

                - name: validations_store
                class_name: ValidationsStore
                store_backend:
                    class_name: TupleFilesystemStoreBackend
                    base_directory: uncommitted/validations/

                - name: shared_validations_filesystem_store
                class_name: ValidationsStore
                store_backend:
                    class_name: TupleFilesystemStoreBackend
                    base_directory: uncommitted/shared_validations/

    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        1. **Configure a new folder on your filesystem where Validation results will be stored.**

            Create a new folder where you would like to store your Validation results, and move your existing Validation results over to the new location. In our case, the name of the Validation result is ``npi_validations`` and the path to our new storage location is ``shared_validations/``.

            .. code-block:: bash

                # in the great_expectations/ folder
                mkdir shared_validations
                mv uncommitted/validations/npi_validations/ uncommitted/shared_validations/


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



        3. **Update your configuration file to include a new store for Validation results on your filesystem**

            In the example below, Validations Store is being set to ``shared_validations_filesystem_store``, but it can be any name you like.  Also, the ``base_directory`` is being set to ``uncommitted/shared_validations/``, but it can be set to any path accessible by Great Expectations.

            .. code-block:: yaml

                validations_store_name: shared_validations_filesystem_store

                stores:
                    shared_validations_filesystem_store:
                        class_name: ValidationsStore
                        store_backend:
                            class_name: TupleFilesystemStoreBackend
                            base_directory: uncommitted/shared_validations/


        4. **Confirm that the location has been updated by running** ``great_expectations --v3-api store list``.

            Notice the output contains two Validation stores: the original ``validations_store`` and the ``shared_validations_filesystem_store`` we just configured.  This is ok, since Great Expectations will look for Validations in the ``uncommitted/shared_validations/`` folder as long as we set the ``validations_store_name`` variable to ``shared_validations_filesystem_store``. The config for ``validations_store`` can be removed if you would like.

            .. code-block:: bash

                great_expectations --v3-api store list

                - name: validations_store
                class_name: ValidationsStore
                store_backend:
                    class_name: TupleFilesystemStoreBackend
                    base_directory: uncommitted/validations/

                - name: shared_validations_filesystem_store
                class_name: ValidationsStore
                store_backend:
                    class_name: TupleFilesystemStoreBackend
                    base_directory: uncommitted/shared_validations/


5. **Confirm that the Validations store has been correctly configured**

    Run a :ref:`Checkpoint <tutorials__getting_started__validate_your_data>` to store results in the new Validations store on in your new location then visualize the results by re-building :ref:`Data Docs <tutorials__getting_started__set_up_data_docs>`.


If it would be useful to you, please comment with a +1 and feel free to add any suggestions or questions below.  Also, please reach out to us on `Slack <https://greatexpectations.io/slack>`_ if you would like to learn more, or have any questions.

.. discourse::
    :topic_identifier: 176
