.. _how_to_guides__creating_batches__how_to_load_a_database_table_view_or_a_query_result_as_a_batch:

How to load a database table, view, or query result as a batch
==============================================================

This guide shows how to get a :ref:`batch <reference__core_concepts__batches>` of data that Great Expectations can validate from a SQL database. When you validate data using SQL, all compute is managed by your database, and only Validation Results are returned.

.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        .. admonition:: Prerequisites -- this how-to guide assumes you have already:

          - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
          - :ref:`Configured a SQL Datasource <how_to_guides__configuring_datasources>`
          - Identified a ``table`` name, ``view`` name, or ``query`` that you would like to use as the data to validate.
          - Optionally, configured a Batch Kwargs Generator to support inspecting your database and dynamically building Batch Kwargs.


        **Steps**

        1. Construct Batch Kwargs that describe the data you plan to validate.

          For example, if your configured datasource is named "my_db" and you plan to validate data in a view called "my_view" located in the "my_schema" schema:

          - To manually build ``table`` or ``view``-based Batch Kwargs, create a dictionary:

            .. code-block:: python

                batch_kwargs = {
                    "datasource": "my_db",
                    "schema": "my_schema",  # schema is optional; default schema will be used if it is omitted
                    "table": "my_view"  # note that the "table" key is used even to validate a view
                }

          - To use a Batch Kwargs Generator, call the ``build_batch_kwargs`` method:

            For example, if your configured generator is called ``my_generator`` and you wish to access the ``my_view`` asset, you can call:

            .. code-block:: python

                batch_kwargs = context.build_batch_kwargs("my_db", "my_generator", "my_view")

          - To manually build ``query``-based Batch Kwargs, create a dictionary:

            .. code-block:: python

                batch_kwargs = {
                    "datasource": "my_db",
                    "query": "SELECT * FROM my_schema.my_table WHERE '1988-01-01' <= date AND date < '1989-01-01';
                }

          - To use a Query Batch Kwargs Generator, call the ``build_batch_kwargs`` method:

            .. code-block:: python

                batch_kwargs = context.build_batch_kwargs(
                    "my_db",
                    "queries",
                    "movies_by_date",
                    query_parameters={
                        "start": "1988-01-01",
                        "end": "1989-01-01"
                    }
                )

        2. Obtain an Expectation Suite to use to validate your batch.

          .. code-block:: python

              expectation_suite_name = "npi.warning"  # choose an appropriate name for your suite

          If you have not already created a suite, you can do so now.

          .. code-block:: python

              # Note, you can add the "overwrite_existing" flag to the below command if the suite
              # exists but you would like to replace it.
              context.create_expectation_suite(expectation_suite_name)

        3. Get the batch to validate.

          .. code-block:: python

              batch = context.get_batch(
                  batch_kwargs=batch_kwargs,
                  expectation_suite_name=expectation_suite_name
              )

          Now that you have a Batch, you can use it to create Expectations or validate the data.

        **Additional Notes**
          * If you are using Snowflake, and you have lowercase table or column names:
          * If you are loading your batch with a table, you can use pass `"use_quoted_name":True` into your `batch_kwargs` dictionary. This will use the SQL Alchemy quoted_name method to ensure case sensitivity for your table and column names.
          * If you are loading your batch with a query, if you have lowercase column names, you still need to pass `"use_quoted_name":True` into your `batch_kwargs` dictionary. You will also need to wrap your query in single quotes, and your table or column name in double quotes like so:

            .. code-block:: python

                batch_kwargs = {
                    ...
                    "use_quoted_name": True,
                    "query: 'select "lowercase_column_one", "lowercase_column_two" from "lowercase_table_name" limit 100'
                    ...
                }

          * For more information on configuring a Batch Kwargs generator, please see the relevant guides. The above code snippets use the following configuration:

            .. code-block:: yaml

                my_db:
                  class_name: SqlAlchemyDatasource
                  credentials: ${rds_movies_db}
                  data_asset_type:
                    class_name: SqlAlchemyDataset
                    module_name: great_expectations.dataset
                  batch_kwargs_generators:
                    tables:
                      class_name: TableBatchKwargsGenerator
                    queries:
                      class_name: QueryBatchKwargsGenerator
                      query_store_backend:
                        class_name: TupleFilesystemStoreBackend
                        filepath_suffix: .sql
                        base_directory: queries

            .. code-block:: bash

                great_expectations/
                    queries/
                        movies_by_date.sql

            .. code-block:: sql

                SELECT * FROM movies WHERE '$start'::date <= release_date AND release_date <= '$end'::date;

    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        What used to be called a “Batch” in the old API was replaced with :ref:`Validator <reference__core_concepts__validation>`. A Validator knows how to validate a particular Batch of data on a particular :ref:`Execution Engine <reference__core_concepts>` against a particular :ref:`Expectation Suite <reference__core_concepts__expectations__expectation_suites>`. In interactive mode, the Validator can store and update an Expectation Suite while conducting Data Discovery or Exploratory Data Analysis.

        You can read more about the core classes that make Great Expectations run in our :ref:`Core Concepts reference guide <reference__core_concepts>`.

        .. admonition:: Prerequisites -- this how-to guide assumes you have already:

            - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
            - :ref:`Understand the basics of Datasources in the V3 (Batch Request) API <reference__core_concepts__datasources>`
            - :ref:`Configured a Data Context using test_yaml_config <how_to_guides_how_to_configure_datacontext_components_using_test_yaml_config>`
            - :ref:`Configured a Runtime Data Connector <how_to_guides__creating_batches__how_to_configure_a_runtime_data_connector>`
            - Identified a ``query`` that you would like to use as the data to validate.


        **Steps**

        0. Load or create a Data Context

          The ``context`` referenced below can be loaded from disk or configured in code.

          Load an on-disk Data Context via:

          .. code-block:: python

              import great_expectations as ge
              from great_expectations import DataContext
              from great_expectations.core import ExpectationSuite
              from great_expectations.core.batch import RuntimeBatchRequest
              from great_expectations.validator.validator import Validator

              context: DataContext = ge.get_context()

          Create an in-code Data Context using these instructions: :ref:`How to instantiate a Data Context without a yml file <how_to_guides__configuring_data_contexts__how_to_instantiate_a_data_context_without_a_yml_file>`


        1. Configure a Datasource

          Configure a :ref:`Datasource <reference__core_concepts__datasources>` using the :ref:`Runtime Data Connector <reference__core_concepts__datasources>` to connect to your SQL database. Since we are using a SQL database, we use the ``SqlAlchemyExecutionEngine``. You can use ``batch_identifiers`` to define what data you are able to attach as additional metadata to your Batch using the ``batch_identifiers`` parameter (shown in step 3).

          By default, the SqlAlchemy Execution Engine will create a temporary table using a given query (provided in step 3). This has a performance advantage when creating and working with a Batch because the query will only be executed once (when the temporary table is created). If you would like to override this default behavior (for example, if you do not have permissions to create a temporary table), you may do so by setting ``create_temp_table`` to ``False`` in the Execution Engine configuration. You may also override the default behavior at runtime, on a case-by-case basis via the ``batch_spec_passthrough`` argument of a Runtime Batch Request (see step 3 for details).

          .. code-block:: yaml

              insert_your_sqlalchemy_datasource_name_here:
                class_name: Datasource
                module_name: great_expectations.datasource
                execution_engine:
                  class_name: SqlAlchemyExecutionEngine
                  module_name: great_expectations.execution_engine
                  connection_string: sqlite:///my_db_file # Insert your SqlAlchemy connection string here
                  # create_temp_table is optional and defaults to True - you may override this behavior here
                  create_temp_table: False
                data_connectors:
                  insert_your_runtime_data_connector_name_here:
                    module_name: great_expectations.datasource.data_connector
                    class_name: RuntimeDataConnector
                    batch_identifiers:
                      - some_key_maybe_pipeline_stage
                      - some_other_key_maybe_run_id

        2. Obtain an Expectation Suite

          .. code-block:: python

            suite: ExpectationSuite = context.get_expectation_suite("insert_your_expectation_suite_name_here")

          Alternatively, you can simply use the name of the Expectation Suite.

          .. code-block:: python

            suite_name: str = "insert_your_expectation_suite_name_here"

          If you have not already created an Expectation Suite, you can do so now.

          .. code-block:: python

            suite: ExpectationSuite = context.create_expectation_suite("insert_your_expectation_suite_name_here")

        3. Construct a Runtime Batch Request

          We will create a Runtime Batch Request and pass it our query via the ``runtime_parameters`` argument, under the ``query`` key. The ``batch_identifiers`` argument is required and must be a non-empty dictionary containing all of the Batch Identifiers specified in your Runtime Data Connector configuration.

          By default, the associated SqlAlchemy Execution Engine will create a temporary table with your given query unless configured otherwise (see step 1). If you would like to control this behavior at runtime, instead of in configuration, you may do so by setting ``create_temp_table`` to ``False`` via the Runtime Batch Request's ``batch_spec_passthrough`` argument.

          .. code-block:: python

            batch_request = RuntimeBatchRequest(
                datasource_name="insert_your_sqlalchemy_datasource_name_here",
                data_connector_name="insert_your_runtime_data_connector_name_here",
                data_asset_name="insert_your_data_asset_name_here", # this can be anything that identifies this data_asset for you
                runtime_parameters={
                    "query": "SELECT * FROM my_table"
                },
                batch_identifiers={
                    "some_key_maybe_pipeline_stage": "validation_stage",
                    "some_other_key_maybe_run_id": 1234567890
                }
                # Use batch_spec_passthrough to control whether the associated SqlAlchemy Execution Engine will create
                # a temporary table
                batch_spec_passthrough={
                  "create_temp_table": False  # if not provided, this defaults to True
                }
            )

          .. admonition:: Best Practice

            Though not strictly required, we recommend that you make every Data Asset Name **unique**. Choosing a unique Data Asset Name makes it easier to navigate quickly through Data Docs and ensures your logical Data Assets are not confused with any particular view of them provided by an Execution Engine.

        4. Construct a Validator

          .. code-block:: python

            my_validator = context.get_validator(
                batch_request=batch_request,
                expectation_suite=suite
            )

          Alternatively, you may skip step 3 and pass the same Runtime Batch Request instantiation arguments, along with the Expectation Suite (or name), directly to to the ``get_validator`` method.

          .. code-block:: python

            my_validator: Validator = context.get_validator(
                datasource_name="insert_your_sqlalchemy_datasource_name_here",
                data_connector_name="insert_your_runtime_data_connector_name_here",
                data_asset_name="insert_your_data_asset_name_here", # this can be anything that identifies this data_asset for you
                runtime_parameters={
                    "query": "SELECT * FROM my_table"
                },
                batch_identifiers={
                    "some_key_maybe_pipeline_stage": "validation_stage",
                    "some_other_key_maybe_run_id": 1234567890
                },
                # Use batch_spec_passthrough to control whether the associated SqlAlchemy Execution Engine will create
                # a temporary table
                batch_spec_passthrough={
                  "create_temp_table": False  # if not provided, this defaults to True
                }
                expectation_suite=suite,  # OR
                # expectation_suite_name=suite_name
            )

        5. Check your data

          You can check that the first few lines of your Batch are what you expect by running:

          .. code-block:: python

              my_validator.active_batch.head()

        Now that you have a Validator, you can use it to create Expectations or validate the data.


.. discourse::
    :topic_identifier: 186
