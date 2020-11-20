.. _how_to_guides__configuring_datasources__how_to_configure_a_pandas_s3_datasource:

#######################################
How to configure a Pandas/S3 Datasource
#######################################

This guide shows how to connect to a Pandas Datasource such that the data is accessible in the form of files located on the AWS S3 service.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`

-----
Steps
-----

To add an S3-backed Pandas datasource do this:

#. **Edit your great_expectations/great_expectations.yml file**

    Update your ``datasources:`` section to include a ``PandasDatasource``.

    .. code-block:: yaml

        datasources:
          pandas_s3:
            class_name: PandasDatasource
            data_asset_type:
              class_name: PandasDataset
              module_name: great_expectations.dataset

#. **Load data from S3 using native S3 path-based Batch Kwargs.**

    Because Pandas provides native support for reading from S3 paths, this simple configuration will allow loading datasources from S3 using native S3 paths.

    .. code-block:: python

        context = DataContext()
        batch_kwargs = {
            "datasource": "pandas_s3",
            "path": "s3a://my_bucket/my_prefix/key.csv",
        }
        batch = context.get_batch(batch_kwargs, "existing_expectation_suite_name")

#. **Optionally, configure a BatchKwargsGenerator that will allow you to generate Data Assets and Partitions from your S3 bucket.**

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

----------------
Additional Notes
----------------

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

--------
Comments
--------

    .. discourse::
        :topic_identifier: 168
