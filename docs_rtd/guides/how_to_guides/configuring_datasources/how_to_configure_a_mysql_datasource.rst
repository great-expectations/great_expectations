.. _how_to_guides__configuring_datasources__how_to_configure_a_mysql_datasource:

#######################################
How to configure a MySQL Datasource
#######################################

This guide shows how to connect to a MySql Datasource. Great Expectations uses SqlAlchemy to connect to MySQL.


Steps
-----

.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        .. admonition:: Prerequisites: This how-to guide assumes you have already:

          - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
          - Obtained database credentials for MySql, including username, password, hostname, and database.

        #. **Install the required modules**

            If you have not already done so, install required modules for connecting to MySql.

            .. code-block:: bash

                pip install sqlalchemy
                pip install PyMySQL


        #. **Run datasource new**

            From the command line, run:

            .. code-block:: bash

                great_expectations datasource new


        #. **Choose "Relational database (SQL)"**

            .. code-block:: bash

                What data would you like Great Expectations to connect to?
                    1. Files on a filesystem (for processing with Pandas or Spark)
                    2. Relational database (SQL)
                : 2

        #. **Choose MySQL**

            .. code-block:: bash

                Which database backend are you using?
                    1. MySQL
                    2. Postgres
                    3. Redshift
                    4. Snowflake
                    5. BigQuery
                    6. other - Do you have a working SQLAlchemy connection string?
                : 1

        #. **Give your Datasource a name**

            When prompted, provide a custom name for your Snowflake data source, or hit Enter to accept the default.

            .. code-block:: bash

                Give your new Datasource a short name.
                 [my_mysql_db]:

        #. **Enter connection information**

            When prompted, provide a custom name for your new Datasource, or hit Enter to accept the default.

            .. code-block:: bash

                Next, we will configure database credentials and store them in the `my_mysql_db` section
                of this config file: great_expectations/uncommitted/config_variables.yml:

                What is the host for the MySQL connection? [localhost]:
                What is the port for the MySQL connection? [3306]:
                What is the username for the MySQL connection? []: root
                What is the password for the MySQL connection?:
                What is the database name for the MySQL connection? []: test_ci
                Attempting to connect to your database. This may take a moment...


        #. **Save your new configuration**

            .. code-block:: bash

                Great Expectations will now add a new Datasource 'my_mysql_db' to your deployment, by adding this entry to your great_expectations.yml:

                  my_mysql_db:
                    credentials: ${my_mysql_db}
                    data_asset_type:
                      class_name: SqlAlchemyDataset
                      module_name: great_expectations.dataset
                    class_name: SqlAlchemyDatasource
                    module_name: great_expectations.datasource

                The credentials will be saved in uncommitted/config_variables.yml under the key 'my_mysql_db'


    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        .. admonition:: Prerequisites: This how-to guide assumes you have already:

            - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
            - :ref:`Understand the basics of Datasources <reference__core_concepts__datasources>`
            - Learned how to configure a :ref:`Data Context using test_yaml_config <how_to_guides_how_to_configure_datacontext_components_using_test_yaml_config>`
            - Obtained database credentials for MySql, including username, password, hostname, and database.

        To add a MySql datasource, do the following:

        #. **Install the required modules.**

            If you have not already done so, install required modules for connecting to MySql.

            .. code-block:: bash

                pip install sqlalchemy
                pip install PyMySQL

        #. **Run datasource new**

            From the command line, run:

            .. code-block:: bash

                great_expectations --v3-api datasource new

        #. **Choose "Relational database (SQL)"**

            .. code-block:: bash

                What data would you like Great Expectations to connect to?
                    1. Files on a filesystem (for processing with Pandas or Spark)
                    2. Relational database (SQL)
                : 2

        #. **Choose MySQL**

            .. code-block:: bash

                Which database backend are you using?
                    1. MySQL
                    2. Postgres
                    3. Redshift
                    4. Snowflake
                    5. BigQuery
                    6. other - Do you have a working SQLAlchemy connection string?
                : 1

        #. You will be presented with a Jupyter Notebook which will guide you through the steps of creating a Datasource.


        **MySql SimpleSqlalchemyDatasource Example.**

        Within this notebook, you will have the opportunity to create your own yaml Datasource configuration. The following text walks through an example.


            Parameters can be set as strings, or passed in as environment variables. In the following example, a yaml config is configured for a ``SimpleSqlalchemyDatasource`` with associated credentials passed in as strings.

            .. code-block:: python

                datasource_name = "my_mysql_datasource"
                config = f"""
                    name: {datasource_name}
                    class_name: SimpleSqlalchemyDatasource
                    credentials:
                      drivername: mysql+pymysql
                      host: YOUR_MYSQL_HOST
                      port: YOUR_MYSQL_PORT
                      username: YOUR_MYSQL_USERNAME
                      password: YOUR_MYSQL_PASSWORD
                      database: YOUR_MYSQL_DB_NAME
                    introspection:
                      whole_table:
                        data_asset_name_suffix: __whole_table
                    """

            **Note**: Additional examples of yaml configurations for various filesystems and databases can be found in the following document: :ref:`How to configure Data Context components using test_yaml_config <how_to_guides_how_to_configure_datacontext_components_using_test_yaml_config>`


        #. **Test your config using ``context.test_yaml_config``.**

            .. code-block:: python

                context.test_yaml_config(
                    yaml_config=config
                )

            When executed, ``test_yaml_config`` will instantiate the component and run through a ``self_check`` procedure to verify that the component works as expected.

            The resulting output will look something like this:

            .. code-block:: bash

                Attempting to instantiate class from config...
                Instantiating as a DataSource, since class_name is SimpleSqlalchemyDatasource
                Successfully instantiated SimpleSqlalchemyDatasource

                Execution engine: SqlAlchemyExecutionEngine
                Data connectors:
                    whole_table : InferredAssetSqlDataConnector

                    Available data_asset_names (1 of 1):
                        imdb_100k_main__whole_table (1 of 1): [{}]

                    Unmatched data_references (0 of 0): []

            This means all has gone well and you can proceed with configuring your new Datasource. If something about your configuration wasn't set up correctly, ``test_yaml_config`` will raise an error.

        #. **Save the config.**
            Once you are satisfied with the config of your new Datasource, you can make it a permanent part of your Great Expectations configuration. The following method will save the new Datasource to your ``great_expectations.yml``:

            .. code-block:: python

                sanitize_yaml_and_save_datasource(context, config, overwrite_existing=False)

            **Note**: This will output a warning if a Datasource with the same name already exists. Use ``overwrite_existing=True`` to force overwriting.

            **Note**: The credentials will be stored in ``uncommitted/config_variables.yml`` to prevent checking them into version control.


* The default configuration of the most recent MySQL releases does not support some GROUP_BY operations used in Great Expectations. To use the full range of statistical Expectations, you need to disable the ``ONLY_FULL_GROUP_BY`` ``sql_mode`` setting. Please see the following article for more information (https://stackoverflow.com/questions/36829911/how-to-resolve-order-by-clause-is-not-in-select-list-caused-mysql-5-7-with-sel).


Comments
--------

.. discourse::
   :topic_identifier: 294
