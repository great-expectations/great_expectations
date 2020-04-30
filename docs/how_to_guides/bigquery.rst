.. _BigQuery:

##############
BigQuery
##############

To add a BigQuery datasource do this:

1. Run ``great_expectations datasource new``
2. Choose the *SQL* option from the menu.
3. When asked which SQLAlchemy driver to use, enter ``other``.
4. Consult the `PyBigQuery <https://github.com/mxmzdlv/pybigquery`_ docs
   for help building a connection string for your BigQuery cluster. It will look
   something like this:

    .. code-block:: python

        "bigquery://project-name"


5. Paste in this connection string and finish out the interactive prompts.
6. Should you need to modify your connection string you can manually edit the
   ``great_expectations/uncommitted/config_variables.yml`` file.

Custom Queries with SQL datasource
==================================

While other backends use temporary tables to generate batches of data from
custom queries, BigQuery does not support ephemeral temporary tables. As a
work-around, GE will create or replace a *permanent table* when the user supplies
a custom query.

Users can specify a table via a Batch Kwarg called ``bigquery_temp_table``:

    .. code-block:: python

        batch_kwargs = {
            "query": "SELECT * FROM `my-project.my_dataset.my_table`",
            "bigquery_temp_table": "my_other_dataset.temp_table"
        }

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

Follow the `Google Cloud library guide <https://googleapis.dev/python/google-api-core/latest/auth.html>`_
for authentication.

Install the pybigquery package for the BigQuery sqlalchemy dialect (``pip install pybigquery``)
