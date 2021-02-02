.. _how_to_guides__creading_and_editing_expectations__how_to_dynamically_load_evaluation_parameters_from_a_database:

How to dynamically load evaluation parameters from a database
==============================================================

This guide will help you create an Expectation that loads part of its :ref:`Expectation Configuration <reference__core_concepts>` from a database at runtime. Using a dynamic :ref:`Evaluation Parameter <reference__core_concepts>` makes it possible to maintain part of an Expectation Suite in a shared database.

.. admonition:: Prerequisites - This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
  - Obtained credentials for a database to query for dynamic values
  - Identified a SQL query that will return values for your expectation configuration.

Steps
-----

#. **Add a new SqlAlchemy Query Store to your context**

    A SqlAlchemy Query Store acts as a bridge that can query a SqlAlchemy-connected database and return the result of the query to be available for an evaluation parameter.

    Find the ``stores`` section in your ``great_expectations.yml`` file, and add the following configuration for a new store called "my_query_store". You can add and reference multiple Query Stores with different names.

    .. code-block:: yaml

      my_query_store:
        class_name: SqlAlchemyQueryStore
        credentials: ${rds_movies_db}
        queries:
          current_genre_ids: "SELECT id FROM genres;"  # The query name and value can be replaced with your desired query

    By default, query results will be returned as a list. If instead you need a scalar for your expectation, you can specify the return_type

    .. code-block:: yaml

      my_query_store:
        class_name: SqlAlchemyQueryStore
        credentials: ${rds_movies_db}
        queries:
          current_ratings_max:
            query: "SELECT MAX(rating) FROM ratings;"
            return_type: "scalar"  # return_type can be either "scalar" or "list" or omitted
          current_genre_ids:
            query: "SELECT id FROM genres;"
            return_type: "list"  # return_type can be either "scalar" or "list" or omitted

    Ensure you have added valid credentials to the ``config-variables.yml`` file:

    .. code-block:: yaml

        rds_movies_db:
          drivername: postgresql
          host: <<hostname>>  # Update with your hostname
          port: 5432
          username: testuser  # Update with your username
          password: <<password>>  # Update with your password
          database: testdb  # Update with your database

#. **In a notebook, get a test batch of data to use for validation.**

    .. code-block:: python

        import great_expectations as ge
        context = ge.DataContext()


        batch_kwargs = {
            "datasource": "movies_db",
            "table": "genres_movies"
        }
        expectation_suite_name = "genres_movies.fkey"
        context.create_expectation_suite(expectation_suite_name, overwrite_existing=True)
        batch = context.get_batch(
            batch_kwargs=batch_kwargs,
            expectation_suite_name=expectation_suite_name
        )



#. **Define an expectation that relies on a dynamic query**

    Great Expectations recognizes several types of :ref:`Evaluation Parameters <reference__core_concepts__data_context__evaluation_parameter_stores>` that can use advanced features provided by the Data Context. To dynamically load data, we will be using a store-style URN, which starts with "urn:great_expectations:stores". The next component of the URN is the name of the store we configured above (``my_query_store``), and the final component is the name of the query we defined above (``current_genre_ids``):

    .. code-block:: python

        batch.expect_column_values_to_be_in_set(
            column="genre_id",
            value_set={"$PARAMETER": "urn:great_expectations:stores:my_query_store:current_genre_ids"}
        )

    The SqlAlchemyQueryStore that you configured above will execute the defined query and return the results as the value of the ``value_set`` parameter to evaluate your expectation:

    .. code-block:: json

        {
          "meta": {
            "substituted_parameters": {
              "value_set": [
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                16,
                17,
                18
              ]
            }
          },
          "result": {
            "element_count": 2891,
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_count": 0,
            "unexpected_percent": 0.0,
            "unexpected_percent_nonmissing": 0.0,
            "partial_unexpected_list": []
          },
          "success": true,
          "exception_info": null
        }

Comments
--------

.. discourse::
   :topic_identifier: 265
