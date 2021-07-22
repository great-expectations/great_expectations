.. _how_to_guides__configuring_datasources__how_to_configure_a_self_managed_spark_datasource:

How to configure a self managed Spark Datasource
================================================

This guide will help you add a managed Spark dataset (Spark Dataframe created by Spark SQL Query) as a Datasource. This will allow you to run expectations against tables available within your Spark cluster.

When you use a managed Spark Datasource, the validation is done in Spark itself. Your data is not downloaded.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
  - Installed the pyspark package (``pip install pyspark``)
  - Setup `SPARK_HOME` and `JAVA_HOME` variables for runtime environment

-----
Steps
-----

To enable running Great Expectations against dataframe created by Spark SQL query, follow below steps:

.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        #. **Run datasource new**

            From the command line, run:

            .. code-block:: bash

                great_expectations datasource new

        #. **Choose "Files on a filesystem (for processing with Pandas or Spark)"**

            .. code-block:: bash

                What data would you like Great Expectations to connect to?
                    1. Files on a filesystem (for processing with Pandas or Spark)
                    2. Relational database (SQL)

                : 1

        #. **Choose PySpark**

            .. code-block:: bash

                What are you processing your files with?
                    1. Pandas
                    2. PySpark

                : 2

        #. **Enter /tmp** (it doesn't matter what you enter as we will replace this in a few steps).

            .. code-block:: bash

                Enter the path (relative or absolute) of the root directory where the data files are stored.

                : /tmp

        #. **Enter spark_dataframe**

            .. code-block:: bash

                Give your new Datasource a short name.
                [tmp__dir]: spark_dataframe

        #. **Enter Y**

            .. code-block:: bash

                Would you like to proceed? [Y/n]: Y

        #. **Replace lines in great_expectations.yml file**

        .. code-block:: yaml

            datasources:
              spark_dataframe:
                data_asset_type:
                  class_name: SparkDFDataset
                  module_name: great_expectations.dataset
                batch_kwargs_generators:
                  subdir_reader:
                    class_name: SubdirReaderBatchKwargsGenerator
                    base_directory: /tmp
                class_name: SparkDFDatasource
                module_name: great_expectations.datasource

        with

        .. code-block:: yaml

            datasources:
              spark_dataframe:
                data_asset_type:
                  class_name: SparkDFDataset
                  module_name: great_expectations.dataset
                batch_kwargs_generators:
                  spark_sql_query:
                    class_name: QueryBatchKwargsGenerator
                    queries:
                      ${query_name}: ${spark_sql_query}
                module_name: great_expectations.datasource
                class_name: SparkDFDatasource

        #. **Fill values:**

        * **query_name** - Name by which you want to reference the datasource. For next points we will use `my_first_query` name. You will use this name to select datasource when creating expectations.
        * **spark_sql_query** - Spark SQL Query that will create DataFrame against which GE validations will be run. For next points we will use `select * from mydb.mytable` query.

        Now, when creating new expectation suite, query `main` will be available in the list of datasources.

    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        #. **Run datasource new**

            From the command line, run:

            .. code-block:: bash

                great_expectations --v3-api datasource new

        #. **Choose "Files on a filesystem (for processing with Pandas or Spark)"**

            .. code-block:: bash

                What data would you like Great Expectations to connect to?
                    1. Files on a filesystem (for processing with Pandas or Spark)
                    2. Relational database (SQL)

                : 1

        #. **Choose PySpark**

            .. code-block:: bash

                What are you processing your files with?
                    1. Pandas
                    2. PySpark

                : 2

        #. **Enter /tmp** (it doesn't matter what you enter as we will replace this in a few steps).

            .. code-block:: bash

                Enter the path (relative or absolute) of the root directory where the data files are stored.

                : /tmp

        #. You will be presented with a Jupyter Notebook which will guide you through the steps of creating a Datasource.

----------------
Additional Notes
----------------

.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        #. **Configuring Spark options**

        To provide custom configuration options either:

        1. Create curated `spark-defaults.conf` configuration file in `$SPARK_HOME/conf` directory
        2. Provide `spark_config` dictionary to Datasource config:

            .. code-block:: yaml

                datasources:
                  spark_dataframe:
                    data_asset_type:
                      class_name: SparkDFDataset
                      module_name: great_expectations.dataset
                    batch_kwargs_generators:
                      spark_sql_query:
                        class_name: QueryBatchKwargsGenerator
                        queries:
                          ${query_name}: ${spark_sql_query}
                    module_name: great_expectations.datasource
                    class_name: SparkDFDatasource
                    spark_config:
                      spark.master: local[*]
                      spark.jars.packages: 'org.apache.hadoop:hadoop-aws:2.7.3'

        Full list of Spark configuration options is available here: [https://spark.apache.org/docs/latest/configuration.html](https://spark.apache.org/docs/latest/configuration.html)

        **Spark catalog**

        Running SQL queries requires either registering temporary views or enabling Spark catalog (like Hive metastore).

        This configuration options are enabled using Hive Metastore catalog - an equivalent of `.enableHiveSupport()`.

            .. code-block:: bash

                spark.sql.catalogImplementation     hive
                spark.sql.warehouse.dir             /tmp/hive
                spark.hadoop.hive.metastore.uris    thrift://localhost:9083

    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        #. **Configuring Spark options**

        To provide custom configuration options either:

        1. Create curated `spark-defaults.conf` configuration file in `$SPARK_HOME/conf` directory
        2. Provide `spark_config` dictionary to Datasource config:

        .. code-block:: yaml

            datasources:
              spark_dataframe:
                class_name: Datasource
                execution_engine:
                  class_name: SparkDFExecutionEngine
                  spark_config:
                    spark.master: local[*]
                data_connectors:
                  simple_filesystem_data_connector:
                    class_name: InferredAssetFilesystemDataConnector
                    base_directory: /root/directory/containing/data/files
                    glob_directive: '*'
                    default_regex:
                      pattern: (.+)\.csv
                      group_names:
                      - data_asset_name


        **Fill values:**

        * **base_directory** - Either absolute path or relative path with respect to Great Expectations installation directory is acceptable
        * **class_name** - A different DataConnector class with its corresponding configuration parameters may be substituted into the above snippet as best suitable for the given use case.

        The full list of Spark configuration options is available here: [https://spark.apache.org/docs/latest/configuration.html](https://spark.apache.org/docs/latest/configuration.html)

.. discourse::
    :topic_identifier: 170
