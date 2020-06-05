.. _how_to_guides__configuring_datasources__how_to_configure_a_redshift_datasource:

######################################
How to configure a Redshift Datasource
######################################

This guide shows how to connect to a Redshift Datasource.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <getting_started>`

-----
Steps
-----

To add a Redshift datasource, do this:

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

