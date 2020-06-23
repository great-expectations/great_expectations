.. _how_to_guides__configuring_metadata_stores__how_to_configure_a_validation_result_store_on_a_filesystem:

How to configure a Validation Result store on a filesystem
=====================================================

By default, Validation results are stored in the ``uncommitted/validations/`` directory.  Since Validations may include examples of data (which could be sensitive or regulated) they should not be committed to a source control system.

This guide will help you configure a new storage location for Validations on your filesystem.

.. admonition:: Prerequisites: This how-to guide assumes that you have already:

    - Configured a :ref:`Data Context <tutorials__getting_started__initialize_a_data_context>`.
    - Configured an :ref:`Expectation Suite <tutorials__getting_started__create_your_first_expectations>`.
    - Configured a :ref:`Checkpoint <tutorials__getting_started__set_up_your_first_checkpoint>`.
    - Determined a new storage location where you would like to store Validations. This can either be a local path, or a path to a secure network filesystem.

Steps
-----


1. First create a new folder where you would like to store your Validation results, and move your existing Validation results over to the new location. In our case, the name of the Validation result is ``npi_validations`` and the path to our new storage location is ``shared_validations/``.

    .. code-block:: bash

        # in the great_expectations folder
        mkdir shared_expectations
        mv uncommitted/validations/npi_validations/ uncommitted/shared_expectations/


2. Open the ``great_expectations.yml`` file and look for the following lines.

    .. code-block:: yaml

        validations_store_name: validations_store

        stores:
            validations_store:
                class_name: ValidationsStore
                store_backend:
                    class_name: TupleFilesystemStoreBackend
                    base_directory: uncommitted/validations/


The configuration file tells Great Expectations to look for Validations in a store called ``validations_store``. The ``base_directory`` is set to ``uncommitted/validations`` by default.

2. Update your configuration file using the example below. In our case, Validations store is being set to ``shared_validations_filesystem_store``, but it can be any name you like.  Also, the ``base_directory`` is being set to ``uncommitted/shared_validations/``, but it can be set to any path accessible by Great Expectations.  Copy existing Validation results to the new folder if necessary.

    .. code-block:: yaml

        expectations_store_name: shared_validations_filesystem_store

        stores:
            shared_validations_filesystem_store:
                class_name: ValidationsStore
                store_backend:
                    class_name: TupleFilesystemStoreBackend
                    base_directory: uncommitted/shared_validations/

3. Confirm that the Validation store has been updated by re-running a Checkpoint: ``great_expectations checkpoint run check_1``. You can also visualize the results by re-building :ref:`Data Docs <tutorials__getting_started__set_up_data_docs>`.

    .. code-block:: bash

        great_expectations checkpoint run check_1  # check_1 is the name of our Checkpoint

        Validation Succeeded!


If it would be useful to you, please comment with a +1 and feel free to add any suggestions or questions below.

.. discourse::
    :topic_identifier: 176
