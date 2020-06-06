.. _how_to_guides__configuring_datasources__how_to_configure_a_pandas_s3_datasource:

#######################################
How to configure a Pandas/S3 Datasource
#######################################

This guide shows how to connect to a Pandas Datasource such that the data is accessible in the form of files located on the AWS S3 service.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <getting_started>`

-----
Steps
-----

To add an S3-backed Pandas datasource do this:

#. **Edit your "great_expectations/great_expectations.yml" file**

    If the ``datasources:`` key does not exist, then enter it on a line by itself.  Note that the colon character (**:**) must included.  If this key and a data source section underneath it already exists, then proceed to the next step.

    In the ``datasources:`` section, paste the following set of YAML configuration directives.  The left-most entry, ``pandas_s3:``, must be indented by 2 characters.

    .. code-block:: yaml

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
                regex_filter: .*  # The regex filter will filter the results returned by S3 for the key and prefix to only those matching the regex
        module_name: great_expectations.datasource
        data_asset_type:
          class_name: PandasDataset
          module_name: great_expectations.dataset

    In the above YAML code snippet, all relative indentations should be two characters wide.  In addition, while the order of data sources in the ``great_expectations/great_expectations.yml`` configuration file does not matter,
    the indentations of all data source sections must be the same.  In other words, if there are multiple data sources, their names must line up:

    .. code-block:: yaml

        datasources:
          local_filesystem:
            [... details ...]
          some_sql_db:
            [... details ...]
          pandas_s3:
            [... details ...]

    In the above example, the three keys in the ``datasources:`` section are indented equally.

    You will probably want to change the names of the data assets in the ``assets:`` section in a way that is meaningful to your project.  There is no limit
    on the number of data assets, but you should only keep the ones that are actually used in the configuration file (i.e., delete the
    unused ones from the above template and/or add as many as needed for your project).

#. **Run suite scaffold**

    Since you edited the Great Expectations configuration file, the updated configuration must be tested to make sure that no errors were introduced.

    From the command line, run:

    .. code-block:: bash

        great_expectations suite scaffold name_of_new_expectation_suite

    .. code-block:: bash

        Select a datasource
            1. local_filesystem
            2. some_sql_db
            3. pandas_s3
        : 3

    If ``pandas_s3`` is the only available data source, then you will not be offered a choice of the data source; in this case, the ``pandas_s3`` data source will be chosen automatically.

#. **Choose to see "a list of data assets in this datasource"**

    .. code-block:: bash

        Would you like to:
            1. choose from a list of data assets in this datasource
            2. enter the path of a data file
        : 1

#. **Verify that all your data assets appear in the list**

    .. code-block:: bash

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

#.
    The default values of the various options satisfy the vast majority of scenarios.  However, in certain cases, the developers may need to override them.
    For instance, ``reader_options``, which can be specified globally and/or at the per-asset level, provide a mechanism for customizing the separator character inside *CSV* files.

#.
    Note that by specifying the ``--no-jupyter`` flag on the command line, as in

    .. code-block:: bash

        great_expectations suite scaffold name_of_new_expectation_suite --no-jupyter

    will initialize the specified expectation suite in the ``great_expectations/expectations`` directory, but suppress the launching of the Jupyter notebook.
    If you resume editing the given expectation suite at a later time, please first verify that the ``batch_kwargs`` contain the correct S3 path for the intended data source.

--------
Comments
--------

    .. discourse::
        :topic_identifier: 168
