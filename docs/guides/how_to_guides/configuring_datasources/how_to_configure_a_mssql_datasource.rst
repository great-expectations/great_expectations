.. _how_to_guides__configuring_datasources__how_to_configure_a_mssql_datasource:


#######################################
How to configure a MSSQL Datasource
#######################################

This guide shows how to connect to a MSSQL Datasource. Great Expectations uses SqlAlchemy to connect to MSSQL, and relies further on the PyODBC driver.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
  - Obtained database credentials for MSSQL, including username, password, hostname, and database.

Steps
-----

#. **Install the required ODBC drivers**

    Follow guides from Microsoft according to your operating system. We have included additional links to relevant resources for connecting to MSSQL databases in the Additional Information section below.

    * `Installing Microsoft ODBC driver for MacOS <https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos>`__
    * `Installing Microsoft ODBC driver for Linux <https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server>`__

#. **Install the required python modules**

    If you have not already done so, install required modules for connecting to mysql.

    .. code-block:: bash

        pip install sqlalchemy
        pip install pyodbc


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

#. **Choose 'other' and provide a connection string**

    .. code-block:: bash

        Which database backend are you using?
            1. MySQL
            2. Postgres
            3. Redshift
            4. Snowflake
            5. BigQuery
            6. other - Do you have a working SQLAlchemy connection string?
        : 6

#. **Give your Datasource a name**

    When prompted, provide a custom name for your Snowflake data source, or hit Enter to accept the default.

    .. code-block:: bash

        Give your new Datasource a short name.
         [my_database]: mssql_db

#. **Enter connection information**

    When prompted, enter a connection string to use to connect to your datasource. Note that we add a query parameter to our connection string to specify the driver: ``driver=ODBC Driver 17 for SQL Server``

    .. code-block:: bash

        Next, we will configure database credentials and store them in the `my_database` section
        of this config file: great_expectations/uncommitted/config_variables.yml:

        What is the url/connection string for the sqlalchemy connection?
        (reference: https://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls)
        : mssql+pyodbc://<<username>>:<<password>>@<<host>>:<<port>>/<<database>>?driver=ODBC Driver 17 for SQL Server&charset=utf&autocommit=true

#. **Save your new configuration**

    .. code-block:: bash

        Great Expectations will now add a new Datasource 'mssql_db' to your deployment, by adding this entry to your great_expectations.yml:

          mssql_db:
            credentials: ${my_database}
            data_asset_type:
              class_name: SqlAlchemyDataset
              module_name: great_expectations.dataset
            class_name: SqlAlchemyDatasource
            module_name: great_expectations.datasource

        The credentials will be saved in uncommitted/config_variables.yml under the key 'mssql_db'




Additional notes
----------------

The following blog post provides a useful overview of using SqlAlchemy to connect to MSSQL.

* https://medium.com/@anushkamehra16/connecting-to-sql-database-using-sqlalchemy-in-python-2be2cf883f85


Comments
--------

.. discourse::
   :topic_identifier: 295
