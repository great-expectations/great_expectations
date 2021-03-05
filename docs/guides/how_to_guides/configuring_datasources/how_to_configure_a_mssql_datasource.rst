.. _how_to_guides__configuring_datasources__how_to_configure_a_mssql_datasource:


#######################################
How to configure a MSSQL Datasource
#######################################

This guide shows how to connect to a MSSQL Datasource. Great Expectations uses SqlAlchemy to connect to MSSQL, and relies further on the PyODBC driver.

Steps
-----

.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        .. admonition:: Prerequisites: This how-to guide assumes you have already:

            - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
            - Obtained database credentials for MSSQL, including username, password, hostname, and database.


        #. **Install the required ODBC drivers**

            Follow guides from Microsoft according to your operating system. We have included additional links to relevant resources for connecting to MSSQL databases in the Additional Information section below.

            * `Installing Microsoft ODBC driver for MacOS <https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos>`__
            * `Installing Microsoft ODBC driver for Linux <https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server>`__

        #. **Install the required python modules**

            If you have not already done so, install required modules for connecting to MSSQL.

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
                : mssql+pyodbc://YOUR_MSSQL_USERNAME:YOUR_MSSQL_PASSWORD@YOUR_MSSQL_HOST:YOUR_MSSQL_PORT/YOUR_MSSQL_DATABASE?driver=ODBC Driver 17 for SQL Server&charset=utf&autocommit=true

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

    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        .. admonition:: Prerequisites: This how-to guide assumes you have already:

            - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
            - :ref:`Understand the basics of Datasources <reference__core_concepts__datasources>`
            - Learned how to configure a :ref:`DataContext using test_yaml_config <how_to_guides_how_to_configure_datacontext_components_using_test_yaml_config>`
            - Obtained database credentials for MSSQL, including username, password, hostname, and database.

        To add a MSSQL datasource, do the following:

        #. **Install the required ODBC drivers.**

            Follow guides from Microsoft according to your operating system. We have included additional links to relevant resources for connecting to MSSQL databases in the Additional Information section below.

            * `Installing Microsoft ODBC driver for MacOS <https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos>`__
            * `Installing Microsoft ODBC driver for Linux <https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server>`__

        #. **Install the required python modules.**

            If you have not already done so, install required modules for connecting to MSSQL.

            .. code-block:: bash

                pip install sqlalchemy
                pip install pyodbc
        #. **Instantiate a DataContext.**

            Create a new Jupyter Notebook and instantiate a DataContext by running the following lines:

            .. code-block:: python

                import great_expectations as ge
                context = ge.get_context()

        #.  **Create or copy a yaml config.**

                Parameters can be set as strings, or passed in as environment variables. In the following example, a yaml config is configured for a ``SimpleSqlalchemyDatasource`` with associated credentials passed in as strings.  GE uses a ``connection_string`` to connect to MSSQL databases through sqlalchemy (reference: https://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls).

                ``SimpleSqlalchemyDatasource`` is a sub-class of ``Datasource`` that automatically configures a ``SqlDataConnector``, and is one you will probably want to use when connecting to data in an sql database. (More information on ``Datasources``
                in GE 0.13 can found in :ref:`Core Great Expectations Concepts document. <reference__core_concepts>`)

                This example also uses ``introspection`` to configure the datasource, where each table in the database is associated with its own ``data_asset``.  A deeper explanation on the different modes of building ``data_asset`` from data (``introspective`` / ``inferred`` vs ``configured``) can be found in the :ref:`Core Great Expectations Concepts document. <reference__core_concepts>`

                Also, additional examples of yaml configurations for various filesystems and databases can be found in the following document: :ref:`How to configure DataContext components using test_yaml_config <how_to_guides_how_to_configure_datacontext_components_using_test_yaml_config>`

                .. code-block:: python

                    config = f"""
                    class_name: SimpleSqlalchemyDatasource
                    connection_string: mssql+pyodbc://YOUR_MSSQL_USERNAME:YOUR_MSSQL_PASSWORD@YOUR_MSSQL_HOST:YOUR_MSSQL_PORT/YOUR_MSSQL_DATABASE?driver=ODBC Driver 17 for SQL Server&charset=utf&autocommit=true
                    introspection:
                      whole_table:
                        data_asset_name_suffix: __whole_table
                    """

        #. **Run context.test_yaml_config.**

            .. code-block:: python

                context.test_yaml_config(
                    name="my_mssql_datasource",
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

                    Choosing an example data reference...
                        Reference chosen: {}

                    Fetching batch data...
                    [(58098,)]

                            Showing 5 rows
                       movieId                               title                                         genres
                    0        1                    Toy Story (1995)  Adventure|Animation|Children|Comedy|Fantasy\r
                    1        2                      Jumanji (1995)                   Adventure|Children|Fantasy\r
                    2        3             Grumpier Old Men (1995)                               Comedy|Romance\r
                    3        4            Waiting to Exhale (1995)                         Comedy|Drama|Romance\r
                    4        5  Father of the Bride Part II (1995)                                       Comedy\r

            This means all has went well and you can proceed with exploring datasets in your new MSSQL datasource.

        #. **Save the config.**

            Once you are satisfied with the config of your new Datasource, you can make it a permanent part of your Great Expectations setup.
            First, create a new entry in the ``datasources`` section of your ``great_expectations/great_expectations.yml`` with the name of your Datasource (which is ``my_mssql_datasource`` in our example).
            Next, copy the yml snippet from Step 4 into the new entry.

            **Note:** Please make sure the yml is indented correctly. This will save you from much frustration.


Additional notes
----------------

The following blog post provides a useful overview of using SqlAlchemy to connect to MSSQL.

* https://medium.com/@anushkamehra16/connecting-to-sql-database-using-sqlalchemy-in-python-2be2cf883f85


Comments
--------

.. discourse::
   :topic_identifier: 295
