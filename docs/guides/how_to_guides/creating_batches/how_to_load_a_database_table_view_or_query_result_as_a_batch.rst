.. _how_to_guides__creating_batches__how_to_load_a_database_table_view_or_a_query_result_as_a_batch:

How to load a database table, view, or query result as a batch
==============================================================

This guide shows how to get a :ref:`batch <reference__core_concepts__batches>` of data that Great Expectations can validate from a SQL database. When you validate data using SQL, all compute is managed by your database, and only Validation Results are returned.


.. admonition:: Prerequisites -- this how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
  - :ref:`Configured a SQL datasource <how_to_guides__configuring_datasources>`
  - Identified a ``table`` name, ``view`` name, or ``query`` that you would like to use as the data to validate.
  - Optionally, configured a Batch Kwargs Generator to support inspecting your database and dynamically building Batch Kwargs.


Steps
-----

#. Construct Batch Kwargs that describe the data you plan to validate.

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

#. **Obtain an Expectation Suite to use to validate your batch**.

    .. code-block:: python

        expectation_suite_name = "npi.warning"  # choose an appropriate name for your suite

    If you have not already created a suite, you can do so now.

    .. code-block:: python

        # Note, you can add the "overwrite_existing" flag to the below command if the suite
        # exists but you would like to replace it.
        context.create_expectation_suite(expectation_suite_name)


#. **Get the batch to validate**.

    .. code-block:: python

        batch = context.get_batch(
            batch_kwargs=batch_kwargs,
            expectation_suite_name=expectation_suite_name
        )


Now that you have a Batch, you can use it to create Expectations or validate the data.


Additional Notes
----------------
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


.. discourse::
    :topic_identifier: 186
