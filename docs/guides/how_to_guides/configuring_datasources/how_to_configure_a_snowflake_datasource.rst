.. _how_to_guides__configuring_datasources__how_to_configure_a_snowflake_datasource:

#######################################
How to configure a Snowflake Datasource
#######################################

This guide shows how to connect to a Snowflake Datasource.

Great Expectations supports 3 different authentication mechanisms for Snowflake:

    * User / password
    * Single sign-on (SSO)
    * Key pair

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <getting_started>`

-----
Steps
-----

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

----------------
Additional Notes
----------------

#.
    When using the Snowflake dialect, ``SqlAlchemyDataset`` will create a **transient** table instead of a **temporary**
    table when passing in ``query`` Batch Kwargs or providing ``custom_sql`` to its constructor. Consequently, users
    **must** provide a ``snowflake_transient_table`` in addition to the ``query`` parameter. Any existing table with that
    name will be overwritten. Note that a transient table is **only** required when using custom SQL. If your Snowflake
    user is unable to create transient tables, or you don't want to manually maintain or drop them, you can always use
    the ``table`` Batch Kwarg and no additional tables will be created by Great Expectations.

#.
   ``snowflake_transient_table`` and ``table`` Batch Kwargs do not currently accept a fully qualified table name (i.e. ``database.schema.table``)
   - only the table name alone. Queries generated by Great Expectations are scoped to the the schema and database specified in your datasource
   configuration, including the creation of the transient table specified in ``snowflake_transient_table``. If you need to use custom SQL,
   but want to isolate transient tables creates to a schema seperate from the rest of your warehouse, you can fully qualify your custom SQL,
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

--------
Comments
--------

    .. discourse::
        :topic_identifier: 171

