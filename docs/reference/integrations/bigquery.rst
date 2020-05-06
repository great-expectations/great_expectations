.. _BigQuery:

##############
BigQuery
##############

How to set up BigQuery as a DataSource
======================================

To add BigQuery datasource do this:

Follow the `Google Cloud library guide <https://googleapis.dev/python/google-api-core/latest/auth.html>`_
for authentication.

Install the pybigquery package for the BigQuery sqlalchemy dialect (``pip install pybigquery``)


Use the CLI:

1. Run ``great_expectations datasource new``
2. Choose "Big Query" from the list of database engines, when prompted.
3. Consult the `PyBigQuery <https://github.com/mxmzdlv/pybigquery`_ docs
   for help building a connection string for your BigQuery cluster. It will look
   something like this:

    .. code-block:: python

        "bigquery://project-name"

   If you want GE to connect to one of the Google's public datasets, the URL should be:

    .. code-block:: python

        "bigquery://project-name/bigquery-public-data"

5. Paste in this connection string when prompted for SQLAlchemy URL and finish out the interactive prompts.
6. Should you need to modify your connection string you can manually edit the
   ``great_expectations/uncommitted/config_variables.yml`` file.


Note: environment variables can be used to store the SQLAlchemy URL instead of the file, if preferred - search documentation for "Managing Environment and Secrets".

How to specify a query result as a batch
==========================================

When you specify a result set of a query as a batch to be validated, GE executes the query and stores
the result in a temporary table in order to optimize the performance of validation.

BigQuery does not support ephemeral temporary tables. As a
work-around, GE will create or replace a *permanent table*.

The user must provide the name of the table via an additional key when constructing
BatchKwargs for the query: ``bigquery_temp_table``.

Here is an example:

    .. code-block:: python

        batch_kwargs = {
            "query": "SELECT * FROM `my-project.my_dataset.my_table`",
            "bigquery_temp_table": "my-project.my_other_dataset.temp_table"
        }

It is safest to specify the fully qualified name for this "temporary" table.

Otherwise, default behavior depends on how the pybigquery engine is configured:

If a default BigQuery dataset is defined in the connection string
(for example, ``bigquery://project-name/dataset-name``), and no ``bigquery_temp_table``
Batch Kwarg is supplied, then GE will create a permanent table with a random
UUID in that location (e.g. ``project-name.dataset-name.ge_tmp_1a1b6511_03e6_4e18_a1b2_d85f9e9045c3``).

If a default BigQuery dataset is not defined in the connection string
(for example, ``bigquery://project-name``) and no ``bigquery_temp_table`` Batch Kwawrg
is supplied, then custom queries will fail.


Additional Notes
=================
