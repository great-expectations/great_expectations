.. _BigQuery:

##############
BigQuery
##############

To add a BigQuery datasource do this:

1. Run ``great_expectations add-datasource``
2. Choose the *SQL* option from the menu.
3. When asked which sqlalchemy driver to use enter ``bigquery``.
4. Consult the `PyBigQuery <https://github.com/mxmzdlv/pybigquery`_ docs
   for help building a connection string for your BigQuery cluster. It will look
   something like this:

    .. code-block:: python

        "bigquery://project-name"


5. Paste in this connection string and finish out the cli prompts.
6. Should you need to modify your connection string you can manually edit the
   ``great_expectations/uncommitted/config_variables.yml`` file.

Additional Notes
=================

Follow the `Google Cloud library guide <https://googleapis.dev/python/google-api-core/latest/auth.html>`_
for authentication.

Install the pybigquery package for the BigQuery sqlalchemy dialect (``pip install pybigquery``)
