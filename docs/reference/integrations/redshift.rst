.. _redshift:

##############
Redshift
##############

To add a Redshift datasource do this:

1. Run ``great_expectations add-datasource``
2. Choose the *SQL* option from the menu.
3. When asked which sqlalchemy driver to use enter ``redshift``.
4. Consult the `SQLAlchemy docs <https://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls>`_
   for help building a connection string for your Redshift cluster. It will look
   something like this:

   Note we have had better luck so far using postgres dialect instead of redshift
   dialect.

    .. code-block:: python

        "postgresql+psycopg2://username:password@my_redshift_endpoint.us-east-2.redshift.amazonaws.com:5439/my_database?sslmode=require"


5. Paste in this connection string and finish out the cli prompts.
6. Should you need to modify your connection string you can manually edit the
   ``great_expectations/uncommitted/config_variables.yml`` file.

Additional Notes
=================

Depending on your Redshift cluster configuration, you may or may not need the
``sslmode`` parameter at the end of the connection url. You can delete everything
after the ``?`` in the connection string above.