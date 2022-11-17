.. _how_to_guides__configuring_datasources__how_to_configure_a_pandas_filesystem_datasource:

###############################################
How to configure a Pandas/filesystem Datasource
###############################################

This guide shows how to connect to a Pandas Datasource such that the data is accessible in the form of files on a local or NFS type of a filesystem.


-----
Steps
-----

.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        .. admonition:: Prerequisites: This how-to guide assumes you have already:

            - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`


        To add a filesystem-backed Pandas datasource do the following:

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

        #. **Choose Pandas**

            .. code-block:: bash

                What are you processing your files with?
                    1. Pandas
                    2. PySpark
                : 1

        #. **Specify the directory path for data files**

            .. code-block:: bash

                Enter the path (relative or absolute) of the root directory where the data files are stored.
                : /path/to/directory/containing/your/data/files

        #. **Give your Datasource a name**

            When prompted, provide a custom name for your filesystem-backed Pandas data source, or hit Enter to accept the default.

            .. code-block:: bash

                Give your new Datasource a short name.
                 [my_data_files_dir]:

            Great Expectations will now add a new Datasource 'my_data_files_dir' to your deployment, by adding this entry to your great_expectations.yml:

            .. code-block:: bash

                  my_data_files_dir:
                    data_asset_type:
                      class_name: PandasDataset
                      module_name: great_expectations.dataset
                    batch_kwargs_generators:
                      subdir_reader:
                        class_name: SubdirReaderBatchKwargsGenerator
                        base_directory: /path/to/directory/containing/your/data/files
                    class_name: PandasDatasource

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
            Finally, if all goes well and you receive a confirmation on your Terminal screen, you can proceed with exploring the data sets in your new filesystem-backed Pandas data source.


    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        .. admonition:: Prerequisites: This how-to guide assumes you have already:

            - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
            - :ref:`Understand the basics of Datasources <reference__core_concepts__datasources>`
            - Learned how to configure a :ref:`Data Context using test_yaml_config <how_to_guides_how_to_configure_datacontext_components_using_test_yaml_config>`

        To add a Pandas filesystem datasource, do the following:

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

        #. **Choose Pandas**

            .. code-block:: bash

                What are you processing your files with?
                    1. Pandas
                    2. PySpark
                : 1

        #. **Specify the directory path for data files**

            .. code-block:: bash

                Enter the path (relative or absolute) of the root directory where the data files are stored.
                : /path/to/directory/containing/your/data/files

        #. You will be presented with a Jupyter Notebook which will guide you through the steps of creating a Datasource.


        **Pandas Datasource Example.**

            Within this notebook, you will have the opportunity to create your own yaml Datasource configuration. The following text walks through an example.


        #. **List files in your directory.**

            Use a utility like ``tree`` on the command line or ``glob`` to list files, so that you can see how paths and filenames are formatted. Our example will use the following 3 files in the ``test_directory/`` folder, which is a sibling of the ``great_expectations/`` folder in our project directory:

            .. code-block:: bash

                - my_ge_project
                    |- great_expectations
                    |- test_directory
                        |- abe_20201119_200.csv
                        |- alex_20201212_300.csv
                        |- will_20201008_100.csv


        #.  **Create or copy a yaml config.**

                Parameters can be set as strings, or passed in as environment variables. In the following example, a yaml config is configured for a ``DataSource``, with an ``InferredAssetFilesystemDataConnector`` and a ``PandasExecutionEngine``.

                **Note**: The ``base_directory`` path needs to be specified either as an absolute path or relative to the ``great_expectations/`` directory.

                .. code-block:: python

                    datasource_name = "my_file_datasource"
                    config = f"""
                            name: {datasource_name}
                            class_name: Datasource
                            execution_engine:
                              class_name: PandasExecutionEngine
                            data_connectors:
                              my_data_connector:
                                datasource_name: {datasource_name}
                                class_name: InferredAssetFilesystemDataConnector
                                base_directory: ../test_directory/
                                default_regex:
                                  group_names: data_asset_name
                                  pattern: (.*)
                            """

               You can modify the group names and regex pattern to take into account the naming structure of the CSV files in the directory, e.g.

                .. code-block:: python

                        group_names:
                          - name
                          - timestamp
                          - size
                        pattern: (.+)_(\\d+)_(\\d+)\\.csv


            **Note**: The ``InferredAssetS3DataConnector`` used in this example is closely related to the ``ConfiguredAssetS3DataConnector`` with some key differences. More information can be found in :ref:`How to choose which DataConnector to use. <which_data_connector_to_use>`


        #. **Test your config using ``context.test_yaml_config``.**

            .. code-block:: python

                context.test_yaml_config(
                    yaml_config=config
                )

            When executed, ``test_yaml_config`` will instantiate the component and run through a ``self_check`` procedure to verify that the component works as expected.

            The resulting output will look something like this:

            .. code-block:: bash

                Attempting to instantiate class from config...
                Instantiating as a Datasource, since class_name is Datasource
                Instantiating class from config without an explicit class_name is dangerous.
                Consider adding an explicit class_name for None
                    Successfully instantiated Datasource

                Execution engine: PandasExecutionEngine
                Data connectors:
                    my_data_connector : InferredAssetFilesystemDataConnector

                    Available data_asset_names (1 of 1):
                        TestAsset (3 of 3): ['abe_20201119_200.csv', 'alex_20201212_300.csv', 'will_20201008_100.csv']

                    Unmatched data_references (0 of 0): []

            This means all has gone well and you can proceed with configuring your new Datasource. If something about your configuration wasn't set up correctly, ``test_yaml_config`` will raise an error.

            **Note:** Pay attention to the "Available data_asset_names" and "Unmatched data_references" output to ensure that the regex pattern you specified matches your desired data files.


        #. **Save the config.**
            Once you are satisfied with the config of your new Datasource, you can make it a permanent part of your Great Expectations configuration. The following method will save the new Datasource to your ``great_expectations.yml``:

            .. code-block:: python

                sanitize_yaml_and_save_datasource(context, config, overwrite_existing=False)

            **Note**: This will output a warning if a Datasource with the same name already exists. Use ``overwrite_existing=True`` to force overwriting.


----------------
Additional Notes
----------------

#.
    For the V2 (Batch Kwargs) API, relative path locations (e.g. for the ``base_directory``) should be specified from the perspective of the directory, in which the

    .. code-block:: bash

        great_expectations datasource new

    command is executed.


#.
    For the V3 (Batch Request) API, relative path locations  (e.g. for the ``base_directory``) should be specified from the perspective of the ``great_expectations/`` directory.


--------
Comments
--------

    .. discourse::
        :topic_identifier: 167
