.. _how_to_guides__validation__how_to_add_validations_data_or_suites_to_a_checkpoint:

How to add validations, data, or suites to a Checkpoint
=======================================================

This guide will help you add validations, data or suites to an existing Checkpoint.
This is useful if you want to aggregate individual validations (across suites or datasources) into a single Checkpoint.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

    - :ref:`Set up a working deployment of Great Expectations <getting_started>`
    - You have an  :ref:`existing Expectation Suite <how_to_guides__create_and_edit_expectations>`
    - You have an :ref:`existing Checkpoint <how_to_guides__validation__how_to_create_a_new_checkpoint>`

Steps
-----

1. First, open your existing Checkpoint in a text editor.
It will look similar to this:

.. code-block:: yaml

    validation_operator_name: action_list_operator
    batches:
      - batch_kwargs:
          path: /home/me/my_project/source_files/npi.csv
          datasource: files_datasource
          reader_method: read_csv
        expectation_suite_names:
          - npi.warning

2. To add a second suite (in this example we add ``npi.critical``) to your Checkpoint modify the file to look like this:

.. code-block:: yaml

    validation_operator_name: action_list_operator
    batches:
      - batch_kwargs:
          path: /home/me/my_project/source_files/npi.csv
          datasource: files_datasource
          reader_method: read_csv
        expectation_suite_names:
          - npi.warning
          - npi.critical

3. To add a second validation of a batch of data (in this case a table named ``npi`` from a datasource named ``data_lake``) to your Checkpoint modify the file to look like this:

.. code-block:: yaml

    validation_operator_name: action_list_operator
    batches:
      - batch_kwargs:
          path: /home/me/my_project/source_files/npi.csv
          datasource: files_datasource
          reader_method: read_csv
        expectation_suite_names:
          - npi.warning
          - another_suite
      - batch_kwargs:
          table: npi
          datasource: data_lake
        expectation_suite_names:
          - npi.warning

Additional notes
----------------

.. tip::

    This is a good way to aggregate validations in a complex pipeline.
    You could use this feature to **validate multiple source files before and after their ingestion into your data lake**.
