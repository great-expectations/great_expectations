.. _how_to_guides__configuring_datasources__how_to_configure_a_mysql_datasource:


#######################################
How to configure a MySQL Datasource
#######################################

This guide shows how to connect to a MySql Datasource. Great Expectations uses SqlAlchemy to connect to MySQL.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
  - Obtained database credentials for mysql, including username, password, hostname, and database.

Steps
-----

#. **Install the required modules**

    If you have not already done so, install required modules for connecting to mysql.

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


Additional notes
----------------

* The default configuration of the most recent MySQL releases does not support some GROUP_BY operations used in Great Expectations. To use the full range of statistical Expectations, you need to disable the ``ONLY_FULL_GROUP_BY`` ``sql_mode`` setting. Please see the following article for more information https://stackoverflow.com/questions/36829911/how-to-resolve-order-by-clause-is-not-in-select-list-caused-mysql-5-7-with-sel).


Comments
--------

.. discourse::
   :topic_identifier: 294
