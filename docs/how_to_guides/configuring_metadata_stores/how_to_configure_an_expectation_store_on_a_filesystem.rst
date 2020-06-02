.. _how_to_guides__configuring_metadata_stores__how_to_configure_an_expectation_store_on_a_filesystem:

How to configure an Expectation store on a filesystem
=====================================================


By default, newly profiled Expectations are stored in JSON format in the ``expectations`` subdirectory of your ``great_expectations`` folder.  This guide will help you configure the location of the Expectation store in the filesystem.

.. admonition:: Prerequisites: This how-to guide assumes that you have already:

    - Configured a Data Context
    - Configured an Expectation Suite
    - Determined a new path where you would like to store Expectations. This can either be a local path, or a path to a network filesystem.

Steps
-----

1. Open ``great_expectations.yml`` file and look for the following lines.

.. code-block:: yaml

    expectations_store_name: expectations_store

    stores:
        expectations_store:
            class_name: ExpectationsStore
            store_backend:
                class_name: TupleFilesystemStoreBackend
                base_directory: expectations/

    The configuration file tells ``Great Expectations`` to look for Expectations in a ``store`` called ``expectation_store``. Further down, the ``base_directory`` for ``expectations_store`` is set to ``expectations/``, which is the default.

2. The following changes will change it to ``new_expectations_store`` with the ``base_directory`` set to ``new_expectations_location``


.. code-block:: yaml

    expectations_store_name: new_expectations_store

    stores:
        new_expectations_store:
            class_name: ExpectationsStore
            store_backend:
                class_name: TupleFilesystemStoreBackend
                base_directory: new_expectations_location/

3. Re-run the Expectation suite to confirm that expectations are actually being stored in ``new_expectations_location/``

.. discourse::
    :topic_identifier: 182
