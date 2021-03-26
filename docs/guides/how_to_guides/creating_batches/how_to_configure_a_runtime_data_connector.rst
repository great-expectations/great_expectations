.. _how_to_guides__creating_batches__how_to_configure_a_runtime_data_connector:

How to configure a RuntimeDataConnector
==========================================

This guide demonstrates how to configure a :ref:`RuntimeDataConnector<runtime_data_connector_and_runtime_batch_request>`.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

    - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
    - :ref:`Understand the basics of Datasources in 0.13 or later <reference__core_concepts__datasources>`
    - Learned how to configure a :ref:`DataContext using test_yaml_config <how_to_guides_how_to_configure_datacontext_components_using_test_yaml_config>`

A RuntimeDataConnector is a special kind of :ref:`Data Connector<reference__core_concepts__datasources__data_connector>` that enables you to use a RuntimeBatchRequest to provide a :ref:`Batch's<specifying_batches>` data directly at runtime. The RuntimeBatchRequest can wrap either an in-memory dataframe, filepath, or SQL query, and must include batch identifiers that uniquely identify the data (e.g. a ``run_id`` from an AirFlow DAG run). The batch identifiers that must be passed in at runtime are specified in the RuntimeDataConnector's configuration.

Add a RuntimeDataConnector to a Datasource configuration
---------------------------------------------------------

The following example uses ``test_yaml_config`` and ``sanitize_yaml_and_save_datasource`` to add a new SQL Datasource to a project's ``great_expectations.yml``. If you already have configured datasources, you can add an additional RuntimeDataConnector configuration directly to your ``great_expectations.yml``.

.. note:: Currently, RuntimeDataConnector cannot be used with datasources of type SimpleSqlalchemyDatasource.

.. code-block:: python

  import great_expectations as ge
  from great_expectations.cli.datasource import sanitize_yaml_and_save_datasource

  context = ge.get_context()
  config = f"""
    name: my_sqlite_datasource
    class_name: Datasource
    execution_engine:
      class_name: SqlAlchemyExecutionEngine
      connection_string: sqlite:///my_db_file
    data_connectors:
      my_runtime_data_connector:
        class_name: RuntimeDataConnector
        batch_identifiers:
          - pipeline_stage_name
          - airflow_run_id
    """
  context.test_yaml_config(
      yaml_config=config
  )
  sanitize_yaml_and_save_datasource(context, config, overwrite_existing=False)

At runtime, you would get a Validator from the Data Context as follows:

.. code-block:: python

  context.get_validator(
      batch_request=RuntimeBatchRequest(
          datasource_name="my_sqlite_datasource",
          data_connector_name="my_runtime_data_connector",
          data_asset_name="my_data_asset_name",
          runtime_parameters={
              "query": "SELECT * FROM table_partitioned_by_date_column__A"
          },
          batch_identifiers={
              "pipeline_stage_name": "core_processing",
              "airflow_run_id": 1234567890,
          },
      ),
      expectation_suite=my_expectation_suite,
  )

  # Simplified call to get_validator - RuntimeBatchRequest is inferred under the hood
  context.get_validator(
      datasource_name="my_sqlite_datasource",
      data_connector_name="my_runtime_data_connector",
      data_asset_name="my_data_asset_name",
      runtime_parameters={
          "query": "SELECT * FROM table_partitioned_by_date_column__A"
      },
      batch_identifiers={
          "pipeline_stage_name": "core_processing",
          "airflow_run_id": 1234567890,
      },
      expectation_suite=my_expectation_suite,
  )
