.. _how_to_guides__configuring_data_contexts__how_to_use_a_yaml_file_or_environment_variables_to_populate_credentials:

How to use a YAML file or environment variables to populate credentials
=========================================================================================

This guide will explain how to use a YAML file and/or environment variables to populate credentials (or any value) in your ``great_expectations.yml`` project config.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`

Steps
------

1. Decide where you would like to save the desired credentials or config values - in a YAML file, environment variables, or a combination - then save the values. In most cases, we suggest using a config variables YAML file. YAML files make variables more visible, easily editable, and allow for modularization (e.g. one file for dev, another for prod).

  .. admonition:: Note:

    - In the ``great_expectations.yml`` config file, environment variables take precedence over variables defined in a config variables YAML
    - Environment variable substitution is supported in both the ``great_expectations.yml`` and config variables ``config_variables.yml`` config file.

  If using a YAML file, save desired credentials or config values to ``great_expectations/uncommitted/config_variables.yml`` or another YAML file of your choosing:

  .. code-block:: yaml

    # great_expectations/uncommitted/config_variables.yml

    my_postgres_db_yaml_creds:
      drivername: postgres
      host: 127.0.0.778
      port: '7987'
      username: administrator
      password: ${MY_DB_PW}
      database: postgres

  .. admonition:: Note:

    - If you wish to store values that include the dollar sign character ``$``, please escape them using a backslash ``\`` so substitution is not attempted. For example in the above example for postgres credentials you could set ``password: pa\$sword`` if your password is ``pa$sword``. Say that 5 times fast, and also please choose a more secure password!
    - When you save values via the CLI, they are automatically escaped if they contain the ``$`` character.
    - You can also have multiple substitutions for the same item, e.g. ``database_string: ${USER}:${PASSWORD}@${HOST}:${PORT}/${DATABASE}``

  If using environment variables, set values by entering ``export ENV_VAR_NAME=env_var_value`` in the terminal or adding the commands to your ``~/.bashrc`` file:

  .. code-block:: bash

    export POSTGRES_DRIVERNAME=postgres
    export POSTGRES_HOST=localhost
    export POSTGRES_PORT='5432'
    export POSTGRES_USERNAME=postgres
    export POSTGRES_PW=''
    export POSTGRES_DB=postgres
    export MY_DB_PW=password

2. If using a YAML file, set the ``config_variables_file_path`` key in your ``great_expectations.yml`` or leave the default.

  .. code-block:: yaml

    # great_expectations/great_expectations.yml

    config_variables_file_path: uncommitted/config_variables.yml

3. Replace credentials or other values in your ``great_expectations.yml`` with ``${}``-wrapped variable names (i.e. ``${ENVIRONMENT_VARIABLE}`` or ``${YAML_KEY}``).

  .. code-block:: yaml

    # great_expectations/great_expectations.yml

    datasources:
      my_postgres_db:
        class_name: SqlAlchemyDatasource
        data_asset_type:
          class_name: SqlAlchemyDataset
          module_name: great_expectations.dataset
        module_name: great_expectations.datasource
        credentials: ${my_postgres_db_yaml_creds}
      my_other_postgres_db:
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

- The default ``config_variables.yml`` file located at ``great_expectations/uncommitted/config_variables.yml`` applies to deployments created using ``great_expectations init``.

.. discourse::
    :topic_identifier: 161
