.. _how_to_guides__configuring_datasources__how_to_configure_a_pandas_s3_datasource:

#######################################
How to configure a Pandas/S3 Datasource
#######################################

This guide shows how to connect to a Pandas Datasource such that the data is accessible in the form of files located on the AWS S3 service.

-----
Steps
-----

.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        .. admonition:: Prerequisites: This how-to guide assumes you have already:

            - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`

        To add an S3-backed Pandas datasource do the following:

        #. **Edit your great_expectations/great_expectations.yml file**

            Update your ``datasources:`` section to include a ``PandasDatasource``.

            .. code-block:: yaml

                datasources:
                  pandas_s3:
                    class_name: PandasDatasource

        #. **Load data from S3 using native S3 path-based Batch Kwargs.**

            Because Pandas provides native support for reading from S3 paths, this simple configuration will allow loading datasources from S3 using native S3 paths.

            .. code-block:: python

                context = DataContext()
                batch_kwargs = {
                    "datasource": "pandas_s3",
                    "path": "s3a://my_bucket/my_prefix/key.csv",
                }
                batch = context.get_batch(batch_kwargs, "existing_expectation_suite_name")

        #. **Optionally, configure a BatchKwargsGenerator that will allow you to generate Data Assets and Batches from your S3 bucket.**

            Update your datasource configuration to include the new Batch Kwargs Generator:

            .. code-block:: yaml

                datasources:
                  pandas_s3:
                    class_name: PandasDatasource
                    batch_kwargs_generators:
                      pandas_s3_generator:
                        class_name: S3GlobReaderBatchKwargsGenerator
                        bucket: your_s3_bucket # Only the bucket name here (i.e., no prefix)
                        assets:
                          your_first_data_asset_name:
                            prefix: prefix_to_folder_containing_your_first_data_asset_files/ # trailing slash is important
                            regex_filter: .*  # The regex filter will filter the results returned by S3 for the key and prefix to only those matching the regex
                          your_second_data_asset_name:
                            prefix: prefix_to_folder_containing_your_second_data_asset_files/ # trailing slash is important
                            regex_filter: .*  # The regex filter will filter the results returned by S3 for the key and prefix to only those matching the regex
                          your_third_data_asset_name:
                            prefix: prefix_to_folder_containing_your_third_data_asset_files/ # trailing slash is important
                            regex_filter: .*  # The regex filter will filter the results returned by S3 for the prefix to only those matching the regex. Note: construct your regex to match the entire S3 key (including the prefix).
                    module_name: great_expectations.datasource
                    data_asset_type:
                      class_name: PandasDataset
                      module_name: great_expectations.dataset

            Update the configuration of the ``assets:`` section to reflect your project's data storage system.  There is no limit on the number of data assets, but you should only keep the ones that are actually used in the configuration file (i.e., delete the unused ones from the above template and/or add as many as needed for your project).

            Note: Multiple data sources can easily be configured in the Data Context by adding a new configuration block for each in the data sources section.  Each data source name should be at the same level of indentation.

        #. **Optionally, run ``great_expectations suite scaffold`` to verify your new Datasource and BatchKwargsGenerator configurations.**

            Since you edited the Great Expectations configuration file, the updated configuration should be tested to make sure that no errors were introduced.

            #. **From the command line, run:**

                .. code-block:: bash

                    great_expectations suite scaffold name_of_new_expectation_suite

                .. code-block:: bash

                    Select a datasource
                        1. local_filesystem
                        2. some_sql_db
                        3. pandas_s3
                    : 3

                Note: If ``pandas_s3`` is the only available data source, then you will not be offered a choice of the data source; in this case, the ``pandas_s3`` data source will be chosen automatically.

            #. **Choose to see "a list of data assets in this datasource"**

                .. code-block:: bash

                    Would you like to:
                        1. choose from a list of data assets in this datasource
                        2. enter the path of a data file
                    : 1

            #. **Verify that all your data assets appear in the list**

                .. code-block::

                    Which data would you like to use?
                        1. your_first_data_asset_name (file)
                        2. your_second_data_asset_name (file)
                        3. your_third_data_asset_name (file)
                        Don't see the name of the data asset in the list above? Just type it
                    :

                When you select the number corresponding to a data asset, a Jupyter notebook will open, pre-populated with the code for adding expectations to the expectation suite specified on the command line against the data set you selected.

                Check the composition of the ``batch_kwargs`` variable at the top of the notebook to make sure that the S3 file used appropriately corresponds to the data set you selected.
                Repeat this check for all data sets you configured.  An inconsistency is likely due to an incorrect regular expression pattern in the respective data set configuration.


        **Additional Notes**

        #.
            Additional options are available for a more fine-grained customization of the S3-backed Pandas data sources.

            .. code-block:: yaml

                delimiter: "/"  # This is the delimiter for the bucket keys (paths inside the buckets).  By default, it is "/".

                boto3_options:
                  endpoint_url: ${S3_ENDPOINT} # Uses the S3_ENDPOINT environment variable to determine which endpoint to use.

                reader_options:  # Note that reader options can be specified globally or per-asset.
                    sep: ","

                max_keys: 100  # The maximum number of keys to fetch in a single request to S3 (default is 100).

        #.  Errors in generated BatchKwargs during configuration of the S3GlobReaderBatchKwargsGenerator are likely due to an incorrect regular expression pattern in the respective data set configuration.

        #.
            The default values of the various options satisfy the vast majority of scenarios.  However, in certain cases, the developers may need to override them.
            For instance, ``reader_options``, which can be specified globally and/or at the per-asset level, provide a mechanism for customizing the separator character inside *CSV* files.

        #.
            Note that specifying the ``--no-jupyter`` flag on the command line will initialize the specified expectation suite in the ``great_expectations/expectations`` directory, but suppress the launching of the Jupyter notebook.

            .. code-block:: bash

                great_expectations suite scaffold name_of_new_expectation_suite --no-jupyter

            If you resume editing the given expectation suite at a later time, please first verify that the ``batch_kwargs`` contain the correct S3 path for the intended data source.


    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        .. admonition:: Prerequisites: This how-to guide assumes you have already:

            - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
            - :ref:`Understand the basics of Datasources <reference__core_concepts__datasources>`
            - Learned how to configure a :ref:`Data Context using test_yaml_config <how_to_guides_how_to_configure_datacontext_components_using_test_yaml_config>`

        To add an S3-backed Pandas datasource do the following:

        #. **Install the required modules.**

            If you haven't already, install these modules for connecting to S3.

            .. code-block:: bash

                pip install boto3
                pip install fsspec
                pip install s3fs

        #. **Run datasource new.**

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

            For an s3 datasource, we will change this in the notebook so feel free to enter anything at this prompt.

            .. code-block:: bash

                Enter the path (relative or absolute) of the root directory where the data files are stored.
                : /path/to/directory/containing/your/data/files

        #. You will be presented with a Jupyter Notebook which will guide you through the steps of creating a Datasource.



        **S3-backed Pandas Datasource Example.**

            Within this notebook, you will have the opportunity to create your own yaml Datasource configuration. The following text walks through an example.


        #. **List files in your directory.**

            Use a utility to list files, so that you can see how paths and filenames are formatted. Our example will use the following 3 files in the ``my_s3_folder/`` folder in our bucket ``my_s3_bucket``:

            .. code-block:: bash

                - my_s3_bucket
                    |- my_s3_folder
                        |- abe_20201119_200.csv
                        |- alex_20201212_300.csv
                        |- will_20201008_100.csv


        #.  **Create or copy a yaml config.**

                Parameters can be set as strings, or passed in as environment variables. In the following example, a yaml config is configured for a ``DataSource``, with a ``InferredAssetS3DataConnector`` and a ``PandasExecutionEngine``. The S3-``bucket`` name and ``prefix`` are passed in as strings.

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
                                class_name: InferredAssetS3DataConnector
                                bucket: my_s3_bucket
                                prefix: my_s3_folder/
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
                    my_data_connector : InferredAssetS3DataConnector

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


        **Additional Notes**

        #.
            Additional options are available for a more fine-grained customization of the S3-backed Pandas data sources.

            .. code-block:: yaml

                delimiter: "/"  # This is the delimiter for the bucket keys (paths inside the buckets).  By default, it is "/".

                boto3_options:
                  endpoint_url: ${S3_ENDPOINT} # Uses the S3_ENDPOINT environment variable to determine which endpoint to use.

                reader_options:  # Note that reader options can be specified globally or per-asset.
                    sep: ","

                max_keys: 100  # The maximum number of keys to fetch in a single request to S3 (default is 100).

        #.
            The default values of the various options satisfy the vast majority of scenarios.  However, in certain cases, the developers may need to override them.
            For instance, ``reader_options``, which can be specified globally and/or at the per-asset level, provide a mechanism for customizing the separator character inside *CSV* files.

        #.
            Note that specifying the ``--no-jupyter`` flag on the command line will initialize the specified expectation suite in the ``great_expectations/expectations`` directory, but suppress the launching of the Jupyter notebook.

            .. code-block:: bash

                great_expectations --v3-api suite new --no-jupyter


--------
Comments
--------

    .. discourse::
        :topic_identifier: 168
