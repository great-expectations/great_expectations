.. _how_to_guides__configuring_data_contexts__how_to_use_environment_variables_to_populate_credentials:

How to use environment variables to populate credentials
========================================================

This guide will explain how to use environment variables to populate credentials (or any value) in your ``great_expectations.yml`` 
project config. 

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <getting_started>`

Steps
-----

1. If you haven't already done so, save your desired credentials or config values to environment variables. For example, 
for credentials to a postgres database, you might enter the following commands into your terminal, or add them to your 
``.bashrc`` file:

.. code-block:: bash

  export POSTGRES_DRIVERNAME=postgres
  export POSTGRES_HOST=localhost
  export POSTGRES_PORT='5432'
  export POSTGRES_USERNAME=postgres
  export POSTGRES_PW=''
  export POSTGRES_DB=postgres

2. Great Expectations uses the same $-based syntax as Python template strings to make substitutions. To populate credentials or 
other config values in your ``great_expectations.yml`` from environment variables, simply wrap the environment variable
like so - ``${ENVIRONMENT_VARIABLE}`` - and place where desired. Continuing the example from above, a postgres datasource 
configuration would look like this:

.. code-block:: yaml

  datasources:
    my_postgres_db:
      class_name: SqlAlchemyDatasource
      data_asset_type:
        class_name: SqlAlchemyDataset
        module_name: great_expectations.dataset
      module_name: great_expectations.datasource
      credentials:
        drivername: ${POSTGRES_DRIVERNAME}
        host: ${POSTGRES_HOST}
        port: ${POSTGRES_PORT}
        username: ${POSTGRES_USERNAME}
        password: ${POSTGRES_PW}
        database: ${POSTGRES_DB}

Additional resources
--------------------

- :ref:`how_to_guides__configuring_data_contexts__how_to_populate_credentials_from_a_secrets_store`

.. discourse::
    :topic_identifier: 161
