.. _how_to_guides__configuring_data_contexts__how_to_use_environment_variables_to_populate_credentials:

How to Use Environment Variables to Populate Credentials
=========================================================

This guide will explain how to use environment variables to populate credentials (or any value) in your ``great_expectations.yml`` 
project config. 

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <getting_started>`

Steps
------

1. If you haven't already done so, save your desired credentials or config values to environment variables.

  .. code-block:: bash
  
    export POSTGRES_DRIVERNAME=postgres
    export POSTGRES_HOST=localhost
    export POSTGRES_PORT='5432'
    export POSTGRES_USERNAME=postgres
    export POSTGRES_PW=''
    export POSTGRES_DB=postgres

2. Replace credentials or other values in your ``great_expectations.yml`` with ${}-wrapped environment variable names (i.e. ``${ENVIRONMENT_VARIABLE}``).

  .. code-block:: yaml
  
    # great_expectations/great_expectations.yml
  
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

Additional Notes
--------------------

- You can set environment variables by entering ``export ENV_VAR_NAME=env_var_value`` in the terminal or adding the commands to your ``~/.bashrc`` file.

Additional Resources
---------------------

- :ref:`how_to_guides__configuring_data_contexts__how_to_populate_credentials_from_yaml_file`

.. discourse::
    :topic_identifier: 161
