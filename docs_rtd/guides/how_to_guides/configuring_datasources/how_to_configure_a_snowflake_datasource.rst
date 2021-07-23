.. _how_to_guides__configuring_datasources__how_to_configure_a_snowflake_datasource:

#######################################
How to configure a Snowflake Datasource
#######################################

This guide shows how to connect to a Snowflake Datasource.

Great Expectations supports 3 different authentication mechanisms for Snowflake:

    * User / password
    * Single sign-on (SSO)
    * Key pair

-----
Steps
-----

.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        .. admonition:: Prerequisites: This how-to guide assumes you have already:

            - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`

        To add a Snowflake datasource, for all authentication mechanisms:

        #. **Install the required modules**

            If you haven't already, install these modules for connecting to Snowflake.

            .. code-block:: bash

                pip install sqlalchemy

                pip install snowflake-connector-python

                pip install snowflake-sqlalchemy

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

        #. **Choose Snowflake**

            .. code-block:: bash

                Which database backend are you using?
                    1. MySQL
                    2. Postgres
                    3. Redshift
                    4. Snowflake
                    5. BigQuery
                    6. other - Do you have a working SQLAlchemy connection string?
                : 4

        #. **Give your Datasource a name**

            When prompted, provide a custom name for your Snowflake data source, or hit Enter to accept the default.

            .. code-block:: bash

                Give your new Datasource a short name.
                 [my_snowflake_db]:

        #. **Choose an authentication mechanism**

            .. code-block:: bash

                What authentication method would you like to use?

                1. User and Password
                2. Single sign-on (SSO)
                3. Key pair authentication

        #. **Enter connection information**

            Next, you will be asked for information common to all authentication mechanisms.

            .. code-block:: bash

                Next, we will configure database credentials and store them in the `my_snowflake_db` section
                of this config file: great_expectations/uncommitted/config_variables.yml:

                What is the user login name for the snowflake connection? []: myusername
                What is the account name for the snowflake connection (include region -- ex 'ABCD.us-east-1')? []: xyz12345.us-east-1
                What is database name for the snowflake connection? (optional -- leave blank for none) []: MY_DATABASE
                What is schema name for the snowflake connection? (optional -- leave blank for none) []: MY_SCHEMA
                What is warehouse name for the snowflake connection? (optional -- leave blank for none) []: MY_COMPUTE_WH
                What is role name for the snowflake connection? (optional -- leave blank for none) []: MY_ROLE

        #. **For "User and Password": provide password**

            Next, you will be asked to supply the password for your Snowflake instance:

            .. code-block:: bash

                What is the password for the snowflake connection?:

            Great Expectations will store these secrets privately on your machine. They will not be committed to git.

        #. **For "Single sign-on (SSO)": provide SSO information**

            Next, you will be asked to enter single sign-on information:

            .. code-block:: bash

                Valid okta URL or 'externalbrowser' used to connect through SSO: externalbrowser

        #. **For "Key pair authentication": provide key pair information**

            Next, you will be asked to enter key pair authentication information:

            .. code-block:: bash

                Path to the private key used for authentication: ~/.ssh/my_snowflake.p8
                Passphrase for the private key used for authentication (optional -- leave blank for none): mypass

            Great Expectations will store these secrets privately on your machine. They will not be committed to git.

        #. **Wait to verify your connection**

            You will then see the following message on your terminal screen:

            .. code-block:: bash

                Attempting to connect to your database. This may take a moment...

            For SSO, you will additionally see a "browser tab" open, follow the authentication process and close the tab once
            the following message is displayed:

            .. code-block:: bash

                Your identity was confirmed and propagated to Snowflake PythonConnector. You can close this window now and go back where you started from.

            If all goes well, it will be followed by the message:

            .. code-block:: bash

                Great Expectations connected to your database!

            If you run into an error, you will see something like:

            .. code-block:: bash

                Cannot connect to the database.
                  - Please check your environment and the configuration you provided.
                  - Database Error: Cannot initialize datasource my_snowflake_db, error: (snowflake.connector.errors.DatabaseError) 250001 (08001): Failed to connect to DB: oca29081.us-east-1.snowflakecomputing.com:443. Incorrect username or password was specified.

                (Background on this error at: http://sqlalche.me/e/4xp6)
                Enter the credentials again? [Y/n]:

            In this case, please check your credentials, ports, firewall, etc. and try again.

        #. **Save your new configuration**

            Finally, you'll be asked to confirm that you want to save your configuration:

            .. code-block:: bash

                Great Expectations will now add a new Datasource 'my_snowflake_db' to your deployment, by adding this entry to your great_expectations.yml:

                  my_snowflake_db:
                    credentials: ${my_snowflake_db}
                    data_asset_type:
                      class_name: SqlAlchemyDataset
                      module_name: great_expectations.dataset
                    class_name: SqlAlchemyDatasource

                The credentials will be saved in uncommitted/config_variables.yml under the key 'my_snowflake_db'

                Would you like to proceed? [Y/n]:

            After this confirmation, you can proceed with exploring the data sets in your new Snowflake Datasource.

    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        .. admonition:: Prerequisites: This how-to guide assumes you have already:

            - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
            - :ref:`Understand the basics of Datasources <reference__core_concepts__datasources>`
            - Learned how to configure a :ref:`Data Context using test_yaml_config <how_to_guides_how_to_configure_datacontext_components_using_test_yaml_config>`

        To add a Snowflake datasource, do the following:

        #. **Install the required modules.**

            If you haven't already, install these modules for connecting to Snowflake.

            .. code-block:: bash

                pip install sqlalchemy
                pip install snowflake-connector-python
                pip install snowflake-sqlalchemy

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

        #. **Choose Snowflake**

            .. code-block:: bash

                Which database backend are you using?
                    1. MySQL
                    2. Postgres
                    3. Redshift
                    4. Snowflake
                    5. BigQuery
                    6. other - Do you have a working SQLAlchemy connection string?
                : 4

        #. **Choose an authentication mechanism**

            .. code-block:: bash

                What authentication method would you like to use?

                1. User and Password
                2. Single sign-on (SSO)
                3. Key pair authentication

        #. You will be presented with a Jupyter Notebook which will guide you through the steps of creating a Datasource.


        **Snowflake SimpleSqlalchemyDatasource Example.**

            Within this notebook, you will have the opportunity to create your own yaml Datasource configuration. The following text walks through an example.


        #.  **Create or copy a yaml config.**

                Parameters can be set as strings, or passed in as environment variables. In the following example, a yaml config is configured for a ``SimpleSqlalchemyDatasource`` with associated credentials using username and password authentication.  Username, password, host, database and query are set as strings.


                .. code-block:: python

                    datasource_name = "my_snowflake_datasource"
                    config = f"""
                        name: {datasource_name}
                        class_name: SimpleSqlalchemyDatasource
                        credentials:
                          drivername: snowflake
                          username: YOUR_SNOWFLAKE_USERNAME
                          password: YOUR_SNOWFLAKE_PASSWORD
                          host: YOUR_SNOWFLAKE_HOST
                          database: TEST
                          query:
                            schema: KAGGLE_MOVIE_DATASET
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

            The output will look something like this:

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

----------------
Additional Notes
----------------

#.
    When using the Snowflake dialect, ``SqlAlchemyDataset`` may create a **transient** table instead of a **temporary**
    table when passing in ``query`` Batch Kwargs or providing ``custom_sql`` to its constructor. Consequently, users
    may provide a ``snowflake_transient_table`` in addition to the ``query`` parameter. Any existing table with that
    name will be overwritten. By default, if no ``snowflake_transient_table`` is passed into Batch Kwargs,
    ``SqlAlchemyDataset`` will create a **temporary** table instead.

#.
   ``snowflake_transient_table`` and ``table`` Batch Kwargs do not currently accept a fully qualified table name (i.e. ``database.schema.table``)
   - only the table name alone. Queries generated by Great Expectations are scoped to the the schema and database specified in your datasource
   configuration, including the creation of the transient table specified in ``snowflake_transient_table``. If you need to use custom SQL,
   but want to isolate transient tables creates to a schema separate from the rest of your warehouse, you can fully qualify your custom SQL,
   and let the transient table be created using the database and schema specified in your datasource configuration.

#.
    Should you need to modify your connection string, you can manually edit the ``great_expectations/uncommitted/config_variables.yml`` file.

#.
    You can edit the  ``great_expectations/uncommitted/config_variables.yml`` file to accomplish the connection configuration without using the CLI.  The entry would have the following format:

    **For "User and password authentication":**

        .. code-block:: yaml

            my_snowflake_db:
                url: "snowflake://<user_login_name>:<password>@<account_name>/<database_name>/<schema_name>?warehouse=<warehouse_name>&role=<role_name>"

    **For "Single sign-on authentication":**

        .. code-block:: yaml

            my_snowflake_db:
                url: "snowflake://<myuser%40mydomain.com>:<password>@<account_name>/<database_name>/<schema_name>?authenticator=<externalbrowser or valid URL encoded okta url>&warehouse=<warehouse_name>&role=<role_name>"

    **For "Key pair authentication":**

        .. code-block:: yaml

            my_snowflake_db:
                drivername: snowflake
                username: <user_login_name>
                host: <account_name>
                database: <database_name>
                query:
                    schema: <schema_name>
                    warehouse: <warehouse_name>
                    role: <role_name>
                private_key_path: </path/to/key.p8>
                private_key_passphrase: <pass_phrase or ''>

#.
    For Snowflake SSO authentication, by default, one browser tab will be opened per connection.
    You can enable token caching at the account level to re-use tokens and minimize the number of browser tabs opened.

    To do so, run the following SQL on Snowflake:

    .. code-block:: sql

        alter account set allow_id_token = true;

    And make sure the version of your ``snowflake-connector-python`` library is ``>=2.2.8``


#.
    **Single sign-on (SSO) Authentication for V3 (Batch Request) API**

    Add ``connect_args`` and ``authenticator`` to ``credentials`` in the yaml configuration.
    The value for ``authenticator`` can be ``externalbrowser``, or a valid okta URL.

    .. code-block:: python

        config = f"""
            class_name: SimpleSqlalchemyDatasource
            credentials:
                drivername: snowflake
                username: YOUR_SNOWFLAKE_USERNAME
                host: YOUR_SNOWFLAKE_HOST
                database: TEST
                connect_args:
                    authenticator: externalbrowser
                query:
                    schema: KAGGLE_MOVIE_DATASET
            introspection:
                whole_table:
                    data_asset_name_suffix: __whole_table
            """

    **Note** This feature is still experimental, so please leave us a comment below if you run into any problems.

#.
    **Key pair Authentication for V3 (Batch Request) API**

    Add ``private_key_path`` and optional ``private_key_passphrase`` to ``credentials`` in the yaml configuration.

        - ``private_key_path`` will need to be set to the path to the private key used for authentication ( ie ``~/.ssh/my_snowflake.p8`` ).
        - ``private_key_passphrase``: is the optional passphrase used for authentication with private key ( ie ``mypass`` ).

        .. code-block:: python

            config = f"""
                class_name: SimpleSqlalchemyDatasource
                credentials:
                    drivername: snowflake
                    username: YOUR_SNOWFLAKE_USERNAME
                    private_key_path: ~/.ssh/my_snowflake.p8
                    private_key_passphrase: mypass
                    host: YOUR_SNOWFLAKE_HOST
                    database: TEST
                    query:
                        schema: KAGGLE_MOVIE_DATASET
                introspection:
                    whole_table:
                        data_asset_name_suffix: __whole_table
                """
    **Note** This feature is still experimental, so please leave us a comment below if you run into any problems.

--------
Comments
--------

    .. discourse::
        :topic_identifier: 171
