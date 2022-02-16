.. _how_to_guides__configuring_datasources__how_to_configure_a_redshift_datasource:

######################################
How to configure a Redshift Datasource
######################################

This guide shows how to connect to a Redshift Datasource.

-----
Steps
-----

.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        .. admonition:: Prerequisites: This how-to guide assumes you have already:

            - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`


        To add a Redshift datasource, do the following:

        #. **Install the required modules**

            If you haven't already, install these modules for connecting to Redshift.

            .. code-block:: bash

                pip install sqlalchemy

                pip install psycopg2

                # or if on macOS:
                pip install psycopg2-binary

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

        #. **Choose Redshift**

            .. code-block:: bash

                Which database backend are you using?
                    1. MySQL
                    2. Postgres
                    3. Redshift
                    4. Snowflake
                    5. BigQuery
                    6. other - Do you have a working SQLAlchemy connection string?
                : 3

        #. **Give your Datasource a name**

            When prompted, provide a custom name for your Redshift data source, or hit Enter to accept the default.

            .. code-block:: bash

                Give your new Datasource a short name.
                 [my_redshift_db]:

        #. **Provide credentials**

            Next, you will be asked to supply the credentials for your Redshift instance:

            .. code-block:: bash

                Next, we will configure database credentials and store them in the `my_redshift_db` section
                of this config file: great_expectations/uncommitted/config_variables.yml:

                What is the host for the Redshift connection? []: my-datawarehouse-name.abcde1qrstuw.us-east-1.redshift.amazonaws.com
                What is the port for the Redshift connection? [5439]:
                What is the username for the Redshift connection? []: myusername
                What is the password for the Redshift connection?:
                What is the database name for the Redshift connection? []: my_database
                What is sslmode name for the Redshift connection? [prefer]: prefer

            Great Expectations will store these secrets privately on your machine. They will not be committed to git.

        #. **Wait to verify your connection**

            You will then see the following message on your terminal screen:

            .. code-block:: bash

                Attempting to connect to your database. This may take a moment...

            If all goes well, it will be followed by the message:

            .. code-block:: bash

                Great Expectations connected to your database!

            If you run into an error, you will see something like:

            .. code-block:: bash

                Cannot connect to the database.
                  - Please check your environment and the configuration you provided.
                  - Database Error: Cannot initialize datasource my_redshift_db, error: (psycopg2.OperationalError) could not connect to server: No such file or directory
                    Is the server running locally and accepting
                    connections on Unix domain socket "/tmp/.s.PGSQL.5439"?

                (Background on this error at: http://sqlalche.me/e/e3q8)
                Enter the credentials again? [Y/n]: n

            In this case, please check your credentials, ports, firewall, etc. and try again.

        #. **Save your new configuration**

            Finally, you'll be asked to confirm that you want to save your configuration:

            .. code-block:: bash

                Great Expectations will now add a new Datasource 'my_redshift_db' to your deployment, by adding this entry to your great_expectations.yml:

                  my_redshift_db:
                    credentials: ${my_redshift_db}
                    data_asset_type:
                      class_name: SqlAlchemyDataset
                      module_name: great_expectations.dataset
                    class_name: SqlAlchemyDatasource

                The credentials will be saved in uncommitted/config_variables.yml under the key 'my_redshift_db'

                Would you like to proceed? [Y/n]:

            After this confirmation, you can proceed with exploring the data sets in your new Redshift Datasource.


    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        .. admonition:: Prerequisites: This how-to guide assumes you have already:

            - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
            - :ref:`Understand the basics of Datasources <reference__core_concepts__datasources>`
            - Learned how to configure a :ref:`Data Context using test_yaml_config <how_to_guides_how_to_configure_datacontext_components_using_test_yaml_config>`

        To add a Redshift datasource, do the following:

        #. **Install the required modules.**

            If you haven't already, install these modules for connecting to Redshift.

            .. code-block:: bash

                pip install sqlalchemy

                pip install psycopg2

                # or if on macOS:
                pip install psycopg2-binary

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

        #. **Choose Redshift**

            .. code-block:: bash

                Which database backend are you using?
                    1. MySQL
                    2. Postgres
                    3. Redshift
                    4. Snowflake
                    5. BigQuery
                    6. other - Do you have a working SQLAlchemy connection string?
                : 3

        #. You will be presented with a Jupyter Notebook which will guide you through the steps of creating a Datasource.


        **Redshift SimpleSqlalchemyDatasource Example.**

            Within this notebook, you will have the opportunity to create your own yaml Datasource configuration. The following text walks through an example.

        #. **Create or copy a yaml config.**

            Parameters can be set as strings, or passed in as environment variables. In the following example, a yaml config is configured for a ``SimpleSqlalchemyDatasource`` with associated credentials.  Username, password, host, port, and database are set as strings.

            .. code-block:: python

                datasource_name = "my_redshift_datasource"
                config = f"""
                name: {datasource_name}
                class_name: SimpleSqlalchemyDatasource
                credentials:
                  drivername: postgresql+psycopg2
                  username: YOUR_REDSHIFT_USERNAME
                  password: YOUR_REDSHIFT_PASSWORD
                  host: YOUR_REDSHIFT_HOSTNAME
                  port: 5439
                  database: YOUR_REDSHIFT_DATABASE
                  query:
                      sslmode: prefer
                introspection:
                  whole_table:
                    data_asset_name_suffix: __whole_table
                """

            **Note**: Additional examples of yaml configurations for various filesystems and databases can be found in the following document: :ref:`How to configure Data Context components using test_yaml_config <how_to_guides_how_to_configure_datacontext_components_using_test_yaml_config>`

        #. **Run context.test_yaml_config.**

            .. code-block:: python

                context.test_yaml_config(
                    yaml_config=config
                )

            When executed, ``test_yaml_config`` will instantiate the component and run through a ``self_check`` procedure to verify that the component works as expected.

            The resulting output will look something like this:

            .. code-block:: bash

                Attempting to instantiate class from config...
                Instantiating as a Datasource, since class_name is SimpleSqlalchemyDatasource
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

----------------
Additional Notes
----------------

#.
    Depending on your Redshift cluster configuration, you may or may not need the ``sslmode`` parameter.

#.
    Should you need to modify your connection string, you can manually edit the ``great_expectations/uncommitted/config_variables.yml`` file.

#.
    You can edit the ``great_expectations/uncommitted/config_variables.yml`` file to accomplish the connection configuration without using the CLI.  The entry would have the following format:

    .. code-block:: yaml

        my_redshift_db:
            url: "postgresql+psycopg2://username:password@host:port/database_name?sslmode=require"

--------
Comments
--------

    .. discourse::
        :topic_identifier: 169
