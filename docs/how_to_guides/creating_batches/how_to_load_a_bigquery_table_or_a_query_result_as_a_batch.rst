.. _how_to_guides__creating_batches__how_to_load_a_bigquery_table_or_a_query_result_as_a_batch:

How to load a BigQuery table or a query result as a batch
=========================================================

This guide will help you load a BigQuery table or a query result as a batch. This is important when you want to validate
a table or the result of a query against an Expectation Suite.


Steps
-----

1. First, make sure that you have a Data Context object with a SQLAlchemy Datasource that is configured to connect to your BigQuery account.


2. Next, construct the Batch Kwargs that describe your table or query to GE:


**If you want to refer to a table:**

If the SQLAlchemy URL that you used to configure the Datasource has only the project name, but no dataset name, use the full name of the table in batch_kwargs:

.. code-block:: python

    batch_kwargs = {
        "table": "dataset-name.my-table",
    }

If the URL had both project name and dataset name, you may specify the short name of the table:

.. code-block:: python

    batch_kwargs = {
        "table": "my-table",
    }


**If you want to refer to a query result:**

When you specify a result set of a query as a batch to be validated, GE executes the query and stores
the result in a temporary table in order to optimize the performance of validation.

BigQuery does not support ephemeral temporary tables. As a
work-around, GE will create or replace a *permanent table*.

The user must provide the name of the table via an additional key when constructing
BatchKwargs for the query: ``bigquery_temp_table``.

Here is an example:

.. code-block:: python

    batch_kwargs = {
        "query": "SELECT * FROM `project-name.dataset-name.my-table`",
        "bigquery_temp_table": "project-name.other-dataset-name.temp-table"
    }



3. Use the BatchKwargs to load a Batch or pass them to a Validation Operator.


Additional notes
----------------

It is safest to specify the fully qualified name for this "temporary" table.

Otherwise, default behavior depends on how the pybigquery engine is configured:

* If a default BigQuery dataset is defined in the connection string (for example, ``bigquery://project-name/dataset-name``), and no ``bigquery_temp_table`` value is supplied, then GE will create a permanent table with a random UUID in that location (e.g. ``project-name.dataset-name.ge_tmp_1a1b6511``).

* If a default BigQuery dataset is not defined in the connection string (for example, ``bigquery://project-name``) and no ``bigquery_temp_table`` value is supplied, then custom queries will fail.

Additional resources
--------------------

