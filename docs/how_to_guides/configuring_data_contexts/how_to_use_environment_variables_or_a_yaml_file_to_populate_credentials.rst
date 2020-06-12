.. _how_to_guides__configuring_data_contexts__how_to_use_environment_variables_or_a_yaml_file_to_populate_credentials:

How to use environment variables or a yaml file to populate credentials
=========================================================================================

This guide will explain how to use environment variables and/or a yaml file to populate credentials (or any value) in your ``great_expectations.yml`` project config. 

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <getting_started>`

Steps
------

1. Save desired credentials or config values to environment variables - these take precedence over variables defined in a config variables ``YAML``.

  .. code-block:: bash
  
    export POSTGRES_DRIVERNAME=postgres
    export POSTGRES_HOST=localhost
    export POSTGRES_PORT='5432'
    export POSTGRES_USERNAME=postgres
    export POSTGRES_PW=''
    export POSTGRES_DB=postgres

2. Save desired credentials or config values to ``great_expectations/uncommitted/config_variables.yml`` or another ``YAML`` file of your choosing.

  .. admonition:: Note:

    Environment variable substitution is only supported in the ``great_expectations.yml`` config file. It's not recursive. For example, you cannot use substitution in ``config_variables.yml``.

  .. code-block:: yaml
    
    # great_expectations/uncommitted/config_variables.yml

    my_postgres_db_yaml_creds:
      drivername: postgres
      host: localhost
      port: '5432'
      username: postgres
      password: ''
      database: postgres

3. Set the ``config_variables_file_path`` key in your ``great_expectations.yml`` or leave the default.

  .. code-block:: yaml
  
    # great_expectations/great_expectations.yml

    config_variables_file_path: uncommitted/config_variables.yml

4. Replace credentials or other values in your ``great_expectations.yml`` with ${}-wrapped variable names (i.e. ``${ENVIRONMENT_VARIABLE}`` or ``${YAML_KEY}``).

  .. code-block:: yaml
  
    # great_expectations/great_expectations.yml
  
    datasources:
      my_postgres_db_env_vars:
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
      my_postgres_db_config_yaml:
        class_name: SqlAlchemyDatasource
        data_asset_type:
          class_name: SqlAlchemyDataset
          module_name: great_expectations.dataset
        module_name: great_expectations.datasource
        credentials: ${my_postgres_db_yaml_creds}

Additional Notes
--------------------

- You can set environment variables by entering ``export ENV_VAR_NAME=env_var_value`` in the terminal or adding the commands to your ``~/.bashrc`` file.
- The default ``config_variables.yml`` file located at ``great_expectations/uncommitted/config_variables.yml`` applies to deployments created using ``great_expectations init``.

.. discourse::
    :topic_identifier: 161
