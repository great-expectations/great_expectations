.. _how_to_guides__configuring_datasources__how_to_configure_a_bigquery_datasource:

How to configure a BigQuery Datasource
=========================================================

This guide will help you add a BigQuery project (or a dataset) as a Datasource. This will allow you to validate tables and queries within this project. When you use a BigQuery Datasource, the validation is done in BigQuery itself. Your data is not downloaded.


Steps
-----

.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        .. admonition:: Prerequisites: This how-to guide assumes you have already:

          - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
          - Followed the `Google Cloud library guide <https://googleapis.dev/python/google-api-core/latest/auth.html>`_ for authentication
          - Installed the ``pybigquery`` package for the BigQuery sqlalchemy dialect (``pip install pybigquery``)


        1. Run the following CLI command to begin the interactive Datasource creation process:

        .. code-block:: bash

            great_expectations datasource new


        2. Choose "Big Query" from the list of database engines, when prompted.
        3. Identify the connection string you would like Great Expectations to use to connect to BigQuery, using the examples below and the `PyBigQuery <https://github.com/mxmzdlv/pybigquery>`_ documentation.

            If you want Great Expectations to connect to your BigQuery project (without specifying a particular dataset), the URL should be:

            .. code-block:: bash

                bigquery://project-name


            If you want Great Expectations to connect to a particular dataset inside your BigQuery project, the URL should be:


            .. code-block:: bash

                bigquery://project-name/dataset-name


            If you want Great Expectations to connect to one of the Google's public datasets, the URL should be:

            .. code-block:: bash

                bigquery://project-name/bigquery-public-data

        5. Enter the connection string when prompted (and press Enter when asked "Would you like to proceed? [Y/n]:").

        6. Should you need to modify your connection string, you can manually edit the
           ``great_expectations/uncommitted/config_variables.yml`` file.


    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        .. admonition:: Prerequisites: This how-to guide assumes you have already:

            - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
            - :ref:`Understood the basics of Datasources <reference__core_concepts__datasources>`
            - Learned how to configure a :ref:`Data Context using test_yaml_config <how_to_guides_how_to_configure_datacontext_components_using_test_yaml_config>`
            - Followed the `Google Cloud library guide <https://googleapis.dev/python/google-api-core/latest/auth.html>`_ for authentication
            - Installed the ``pybigquery`` package for the BigQuery sqlalchemy dialect (``pip install pybigquery``)


        1. Run the following CLI command to begin the interactive Datasource creation process:

            .. code-block:: bash

                great_expectations --v3-api datasource new

        2. Choose "Big Query" from the list of database engines, when prompted.

        3. You will be presented with a Jupyter Notebook which will guide you through the steps of creating a Datasource.


        **BigQuery SimpleSqlalchemyDatasource Example**

        Within this notebook, you will have the opportunity to create your own yaml Datasource configuration. The following text walks through an example.


        3a.  Here is a simple example configuration. In the following example, a yaml config is configured for a ``SimpleSqlalchemyDatasource`` with associated credentials passed in as strings.  Great Expectations uses a ``connection_string`` to connect to BigQuery through SQLAlchemy (reference: https://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls).


                .. code-block:: python

                    datasource_name = "my_bq_datasource"
                    config = f"""
                    name: {datasource_name}
                    class_name: SimpleSqlalchemyDatasource
                    connection_string: my_bq_connection_string
                    introspection:
                      whole_table:
                        data_asset_name_suffix: __whole_table
                    """

            **Note**: Additional examples of yaml configurations for various filesystems and databases can be found in the following document: :ref:`How to configure Data Context components using test_yaml_config <how_to_guides_how_to_configure_datacontext_components_using_test_yaml_config>`


        3b. Test your config using ``context.test_yaml_config``.

            .. code-block:: python

                context.test_yaml_config(yaml_config=config)

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


        3c. **Save the config.**
            Once you are satisfied with the config of your new Datasource, you can make it a permanent part of your Great Expectations configuration. The following method will save the new Datasource to your ``great_expectations.yml``:

            .. code-block:: python

                sanitize_yaml_and_save_datasource(context, config, overwrite_existing=False)

            **Note**: This will output a warning if a Datasource with the same name already exists. Use ``overwrite_existing=True`` to force overwriting.

            **Note**: The credentials will be stored in ``uncommitted/config_variables.yml`` to prevent checking them into version control.


Environment variables can be used to store the SQLAlchemy URL instead of the file, if preferred - search documentation for "Managing Environment and Secrets".

Additional resources
--------------------
