.. _how_to_guides__configuring_datasources__how_to_configure_a_spark_filesystem_datasource:

###############################################
How to configure a Spark/filesystem Datasource
###############################################

This guide shows how to connect to a Spark Datasource such that the data is accessible in the form of files on a local or NFS type of a filesystem.

-----
Steps
-----


.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        .. admonition:: Prerequisites: This how-to guide assumes you have already:

            - :ref:`Set up a working deployment of Great Expectations. <tutorials__getting_started>`
            - Installed the pyspark package. (``pip install pyspark``)
            - Setup ``SPARK_HOME`` and ``JAVA_HOME`` variables for runtime environment.

        To add a filesystem-backed Spark datasource do this:

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

        #. **Specify the directory path for data files**

            .. code-block:: bash

                Enter the path (relative or absolute) of the root directory where the data files are stored.
                : /path/to/directory/containing/your/data/files

        #. **Give your Datasource a name**

            When prompted, provide a custom name for your filesystem-backed Spark data source, or hit Enter to accept the default.

            .. code-block:: bash

                Give your new Datasource a short name.
                 [my_data_files_dir]:

            Great Expectations will now add a new Datasource 'my_data_files_dir' to your deployment, by adding this entry to your great_expectations.yml:

            .. code-block:: bash

                  my_data_files_dir:
                    data_asset_type:
                      class_name: SparkDFDataset
                      module_name: great_expectations.dataset
                    spark_config: {}
                    batch_kwargs_generators:
                      subdir_reader:
                        class_name: SubdirReaderBatchKwargsGenerator
                        base_directory: /path/to/directory/containing/your/data/files
                    class_name: SparkDFDatasource

                    Would you like to proceed? [Y/n]:

        #. **Wait for confirmation**

            If all goes well, it will be followed by the message:

            .. code-block:: bash

                A new datasource 'my_data_files_dir' was added to your project.

            If you run into an error, you will see something like:

            .. code-block:: bash

                Error: Directory '/nonexistent/path/to/directory/containing/your/data/files' does not exist.

                Enter the path (relative or absolute) of the root directory where the data files are stored.
                :

            In this case, please check your data directory path, permissions, etc. and try again.

        #.
            Finally, if all goes well and you receive a confirmation on your Terminal screen, you can proceed with exploring the data sets in your new filesystem-backed Spark data source.

    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        .. admonition:: Prerequisites: This how-to guide assumes you have already:

            - Installed the pyspark package (``pip install pyspark``).
            - Setup ``SPARK_HOME`` and ``JAVA_HOME`` variables for runtime environment.
            - :ref:`Set up a working deployment of Great Expectations. <tutorials__getting_started>`
            - :ref:`Understand the basics of Datasources. <reference__core_concepts__datasources>`
            - Learned how to configure a :ref:`DataContext using test_yaml_config. <how_to_guides_how_to_configure_datacontext_components_using_test_yaml_config>`

        To add a Pandas filesystem datasource, do the following:

        #. **Instantiate a DataContext.**

            Create a new Jupyter Notebook and instantiate a DataContext by running the following lines:

            .. code-block:: python

                import great_expectations as ge
                context = ge.get_context()


        #. **List files in your directory.**

            Use a utility like ``tree`` on the command line or ``glob`` to list files, so that you can see how paths and filenames are formatted. Our example will use the following 3 files in the ``test_directory/`` folder.

            .. code-block:: bash

                test_directory/abe_20201119_200.csv
                test_directory/alex_20201212_300.csv
                test_directory/will_20201008_100.csv

        #.  **Create or copy a yaml config.**

                Parameters can be set as strings, or passed in as environment variables. In the following example, a yaml config is configured for a ``Datasource``, with a ``ConfiguredAssetFilesystemDataConnector`` and ``SparkDFExecutionEngine``.

                The config also defines ``TestAsset``, which has ``name``, ``timestamp`` and ``size`` as ``group_names``, which are informative fields of the filename that are extracted by the regex ``pattern``.

                **Note**: The ``ConfiguredAssetFilesystemDataConnector`` used in this example is closely related to the ``InferredAssetFilesystemDataConnector`` with some key differences. More information can be found in :ref:`How to choose which DataConnector to use. <which_data_connector_to_use>`

                .. code-block:: python

                    config = f"""
                    class_name: Datasource
                    execution_engine:
                      class_name: SparkDFExecutionEngine
                    data_connectors:
                      my_data_connector:
                        class_name: ConfiguredAssetFilesystemDataConnector
                        base_directory: test_directory/
                        glob_directive: "*.csv"
                        assets:
                          TestAsset:
                            pattern: (.+)_(\\d+)_(\\d+)\\.csv
                            group_names:
                              - name
                              - timestamp
                              - size
                    """

                Additional examples of yaml configurations for various filesystems and databases can be found in the following document: :ref:`How to configure DataContext components using test_yaml_config <how_to_guides_how_to_configure_datacontext_components_using_test_yaml_config>`

        #. **Run context.test_yaml_config.**

            .. code-block:: python

                context.test_yaml_config(
                    name="my_sparkdf_datasource",
                    yaml_config=config
                )


            When executed, ``test_yaml_config`` will instantiate the component and run through a ``self_check`` procedure to verify that the component works as expected.

            The resulting output will look something like this:

            .. code-block:: bash

                Attempting to instantiate class from config...
                Instantiating as a Datasource, since class_name is Datasource
                Instantiating class from config without an explicit class_name is dangerous. Consider adding an explicit class_name for None
                    Successfully instantiated Datasource

                Execution engine: SparkDFExecutionEngine
                Data connectors:
                    my_data_connector : ConfiguredAssetFilesystemDataConnector

                    Available data_asset_names (1 of 1):
                        Titanic (3 of 3): ['abe_20201119_200.csv', 'alex_20201212_300.csv', 'will_20201008_100.csv']

                    Unmatched data_references (0 of 0): []

                    Choosing an example data reference...
                        Reference chosen: abe_20201119_200.csv

                        Fetching batch data...

                        Showing 5 rows
                0  None                                           Name  PClass  Age     Sex  Survived  SexCode
                1     1                   Allen, Miss Elisabeth Walton     1st   29  female         1        1
                2     2                    Allison, Miss Helen Loraine     1st    2  female         0        1
                3     3            Allison, Mr Hudson Joshua Creighton     1st   30    male         0        0
                4     4  Allison, Mrs Hudson JC (Bessie Waldo Daniels)     1st   25  female         0        1

            This means all has gone well and you can proceed with exploring data with your new filesystem-backed Pandas data source.

        #. **Save the config.**

            Once you are satisfied with the config of your new Datasource, you can make it a permanent part of your Great Expectations setup.
            First, create a new entry in the ``datasources`` section of your ``great_expectations/great_expectations.yml`` with the name of your Datasource (which is ``my_sparkdf_datasource`` in our example).
            Next, copy the yml snippet from Step 3 into the new entry.

            **Note:** Please make sure the yml is indented correctly. This will save you from much frustration.

----------------
Additional Notes
----------------

#.
    Relative path locations should be specified from the perspective of the directory, in which the

    .. code-block:: bash

        great_expectations datasource new

    command is executed.

#.
    For the V3 (Batch Request) API, relative path locations should be specified from the perspective of the ``great_expectations/`` directory.

--------
Comments
--------

    .. discourse::
        :topic_identifier: 251
