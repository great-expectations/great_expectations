.. _how_to_guides__configuring_datasources__how_to_configure_a_snowflake_datasource:

######################################
How to configure a Snowflake Datasource
######################################

This guide shows how to connect to a Snowflake Datasource.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <getting_started>`

-----
Steps
-----

To add a Snowflake datasource, do this:

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

#. **Provide credentials**

    Next, you will be asked to supply the credentials for your Snowflake instance:

    .. code-block:: bash

        Next, we will configure database credentials and store them in the `my_snowflake_db` section
        of this config file: great_expectations/uncommitted/config_variables.yml:

        What is the user login name for the snowflake connection? []:
        What is the password for the snowflake connection?:
        What is the account name for the snowflake connection (include region -- ex 'ABCD.us-east-1')? []:
        What is database name for the snowflake connection? (optional -- leave blank for none) []:
        What is schema name for the snowflake connection? (optional -- leave blank for none) []:
        What is warehouse name for the snowflake connection? (optional -- leave blank for none) []:
        What is role name for the snowflake connection? (optional -- leave blank for none) []:

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

#. When using the Snowflake dialect, `SqlAlchemyDataset` will create a **transient** table instead of a **temporary**
    table when passing in `query` Batch Kwargs or providing `custom_sql` to its constructor. Consequently, users
    **must** provide a `snowflake_transient_table` in addition to the `query` parameter. Any existing table with that
    name will be overwritten.

#. Note that your Snowflake connection can be equivalently described under the '<your_new_snowflake_data_source>' key in your
    "uncommitted/config_variables.yml" file as follows:

    .. code-block:: python

        "snowflake://<user_login_name>:<password>@<account_name>/<database_name>/<schema_name>?warehouse=<warehouse_name>&role=<role_name>"

#. Should you need to modify your connection string, you can manually edit the ``great_expectations/uncommitted/config_variables.yml`` file.

--------
Comments
--------

    .. discourse::
        :topic_identifier: 171

