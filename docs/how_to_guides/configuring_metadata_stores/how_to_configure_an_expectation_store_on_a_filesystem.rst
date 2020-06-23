.. _how_to_guides__configuring_metadata_stores__how_to_configure_an_expectation_store_on_a_filesystem:

How to configure an Expectation store on a filesystem
=====================================================


By default, newly profiled Expectations are stored in JSON format in the ``expectations/`` subdirectory of your ``great_expectations`` folder.  This guide will help you configure a new storage location for Expectations on your filesystem.

.. admonition:: Prerequisites: This how-to guide assumes that you have already:

    - Configured a :ref:`Data Context <tutorials__getting_started__initialize_a_data_context>`.
    - Configured an :ref:`Expectation Suite <tutorials__getting_started__create_your_first_expectations>`.
    - Determined a new storage location where you would like to store Expectations. This can either be a local path, or a path to a network filesystem.

Steps
-----

1. First create a new folder where you would like to store your Expectations, and move your existing Expectation files over to the new location. In our case, the name of the Expectations file is ``npi_expectations`` and the path to our new storage location is ``/shared_expectations``.

    .. code-block:: bash

        # in the great_expectations folder
        mkdir shared_expectations
        mv expectations/npi_expectations.json shared_expectations/


2. Next open the ``great_expectations.yml`` file and look for the following lines.

    .. code-block:: yaml

        expectations_store_name: expectations_store

        stores:
            expectations_store:
                class_name: ExpectationsStore
                store_backend:
                    class_name: TupleFilesystemStoreBackend
                    base_directory: expectations/


    The configuration file tells Great Expectations to look for Expectations in a Store called ``expectations_store``. The ``base_directory`` for ``expectations_store`` is set to ``expectations/`` by default.

3. Update your configuration following the example below. This example would change the Store name to ``shared_expectations_filesystem_store`` with the ``base_directory`` set to ``shared_expectations/``.

    Paths are relative to the directory where ``great_expectations.yml`` is stored.


    .. code-block:: yaml

        expectations_store_name: shared_expectations_filesystem_store

        stores:
            shared_expectations_filesystem_store:
                class_name: ExpectationsStore
                store_backend:
                    class_name: TupleFilesystemStoreBackend
                    base_directory: shared_expectations/



4. Confirm that the location has been updated by running ``great_expectations store list``.

    .. code-block:: bash

        great_expectations store list

        3 Stores found:

        - name: shared_expectations_filesystem_store
        class_name: ExpectationsStore
        store_backend:
            class_name: TupleFilesystemStoreBackend
            base_directory: shared_expectations/


5. Confirm that Expectations can be read from the new storage location by running ``great_expectations suite list``.

    .. code-block:: bash

        great_expectations suite list

        1 Expectation Suite found:
            - npi_expectations


Additional Notes
----------------

- For best practices, we highly recommend that you store Expectations in a version-control system like Git. The JSON format of Expectations will allow for informative diff-statements and effective tracking of modifications. In the example below, 2 changes have been made to ``npi_expectations``.  The Expectation ```expect_table_column_count_to_equal`` was been changed from ``330`` to ``333`` to ``331``.


    .. code-block:: bash

      git log -p npi_expectations.json

      commit cbc127fb27095364c3c1fcbf6e7f078369b07455
          changed expect_table_column_count_to_equal to 331

      diff --git a/great_expectations/expectations/npi_expectations.json b/great_expectations/expectations/npi_expectations.json

      --- a/great_expectations/expectations/npi_expectations.json
      +++ b/great_expectations/expectations/npi_expectations.json
      @@ -17,7 +17,7 @@
           {
             "expectation_type": "expect_table_column_count_to_equal",
             "kwargs": {
      -        "value": 333
      +        "value": 331
             }
      commit 05b3c8c1ed35d183bac1717d4877fe13bc574963
        changed expect_table_column_count_to_equal to 333

      diff --git a/great_expectations/expectations/npi_expectations.json b/great_expectations/expectations/npi_expectations.json
      --- a/great_expectations/expectations/npi_expectations.json
      +++ b/great_expectations/expectations/npi_expectations.json
           {
             "expectation_type": "expect_table_column_count_to_equal",
             "kwargs": {
      -        "value": 330
      +        "value": 333
             }


If it would be useful to you, please comment with a +1 and feel free to add any suggestions or questions below.

.. discourse::
    :topic_identifier: 182
