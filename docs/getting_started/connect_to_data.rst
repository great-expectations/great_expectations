.. _getting_started__connect_to_data:

Connect to data
===============================================

A :ref:`Datasource<datasource>` is a connection to a compute environment (a backend such as Pandas, Spark, or a SQL-compatible database) and one or more storage environments.

You can have multiple Datasources in a project (Data Context). For example, this is useful if the team’s pipeline consists of both a Spark cluster and a Redshift database.

All the Datasources that your project uses are configured in the project's configuration file ``great_expectations/great_expectations.yml``:


.. code-block:: yaml

    datasources:

      our_product_postgres_database:
        class_name: SqlAlchemyDatasource
        data_asset_type:
          class_name: SqlAlchemyDataset
        credentials: ${prod_db_credentials}

      our_redshift_warehouse:
        class_name: SqlAlchemyDatasource
        data_asset_type:
          class_name: SqlAlchemyDataset
        credentials: ${warehouse_credentials}



The easiest way to add a datasource to the project is to use the :ref:`CLI <command_line>` convenience command:

.. code-block:: bash

    great_expectations datasource new

This command asks for the required connection attributes and tests the connection to the new Datasource.

The intrepid can add Datasources by editing the configuration file, however there are less guardrails around this approach.

A Datasource knows how to load data into the computation environment.
For example, you can use a PySpark Datasource object to load data into a DataFrame from a directory on AWS S3.
This is beyond the scope of this article.

After a team member adds a new Datasource to the Data Context, they commit the updated configuration file into the version control in order to make the change available to the rest of the team.

Because ``great_expectations/great_expectations.yml`` is committed into version control, the :ref:`CLI <command_line>` command **does not store the credentials in this file**.
Instead it saves them in a separate file: ``uncommitted/config_variables.yml`` which is not committed into version control.

This means that that when another team member checks out the updated configuration file with the newly added Datasource, they must add their own credentials to their ``uncommitted/config_variables.yml`` or in environment variables.
