.. _how_to_guides__configuring_data_contexts__how_to_populate_credentials_from_yaml_file:

How to Populate Credentials From a YAML File
=============================================

This guide will explain how to populate credentials (or any value) in your ``great_expectations.yml`` project config from 
another ``YAML`` file.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <getting_started>`

Steps
-----

1. Save your desired credentials or config values to ``great_expectations/uncommitted/config_variables.yml`` or another ``YAML`` file of your choosing.

  .. admonition:: Note:

Environment variable substitution is only supported in the great_expectations.yml config file. It's not recursive. For example, you cannot use substitution in `config_variables.yml``.

  .. code-block:: yaml
    
    # great_expectations/uncommitted/config_variables.yml

    my_postgres_db:
      drivername: postgres
      host: localhost
      port: '5432'
      username: postgres
      password: ''
      database: postgres

2. Set the ``config_variables_file_path`` key in your ``great_expectations.yml`` or leave the default.

  .. code-block:: yaml
  
    # great_expectations/great_expectations.yml

    config_variables_file_path: uncommitted/config_variables.yml

3. Replace credentials or other values in your ``great_expectations.yml`` with ${}-wrapped top-level keys from the ``YAML`` file (i.e. ``${YAML_KEY}``).

  .. admonition:: Note:

    The same syntax is used to make variable substitutions from environment variables, which take precedence over values defined in a config variables ``YAML`` file.

  .. code-block:: yaml
  
    # great_expectations/great_expectations.yml

    datasources:
      my_postgres_db:
        class_name: SqlAlchemyDatasource
        data_asset_type:
          class_name: SqlAlchemyDataset
          module_name: great_expectations.dataset
        module_name: great_expectations.datasource
        credentials: ${my_postgres_db}

Additional Notes
--------------------

- The default ``config_variables.yml`` file located at ``great_expectations/uncommitted/config_variables.yml`` applies to deployments created using ``great_expectations init``. 

Additional Resources
--------------------

- :ref:`how_to_guides__configuring_data_contexts__how_to_use_environment_variables_to_populate_credentials`
