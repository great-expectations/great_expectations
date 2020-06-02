.. _how_to_guides__configuring_metadata_stores__how_to_configure_an_expectation_store_on_a_filesystem:

How to configure an Expectation store on a filesystem
=====================================================


This guide will help you configure the location of the expectation store in the filesystem. By default, newly profiled Expectations in an Expectation Suite, in JSON format in a subdirectory of your ``great_expectations`` folder.

.. admonition:: Prerequisites: This how-to guide assumes that you have already:

    - Configured a Data Context
    - Have a new location of a filesystem where you would like. tHis could be either a new local location, or a shared location.

Steps
-----

1. Open ``great_expectations.yml`` file and look for the following line.

.. code-block:: yaml

    expectations_store_name: expectations_store

    stores:
        expectations_store:
            class_name: ExpectationsStore
            store_backend:
                class_name: TupleFilesystemStoreBackend
                base_directory: expectations/


2. mModi



.. code-block:: yaml

    expectations_store_name: new_expectations_store

    stores:
        new_expectations_store:
            class_name: ExpectationsStore
            store_backend:
                class_name: TupleFilesystemStoreBackend
                base_directory: new_expectations_location/




.. discourse::
    :topic_identifier: 182
