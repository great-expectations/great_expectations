.. _how_to_guides_how_to_configure_a_inferredassetdataconnector:

How to configure an InferredAssetDataConnector
==============================================

This guide demonstrates how to configure an InferredAssetDataConnector, and provides several examples you
can use for configuration.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

    - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
    - :ref:`Understand the basics of Datasources in 0.13 or later <reference__core_concepts__datasources>`
    - Learned how to configure a :ref:`DataContext using test_yaml_config <how_to_guides_how_to_configure_datacontext_components_using_test_yaml_config>`

Great Expectations provides two types of ``DataConnector`` classes for connecting to file-system-like data. This includes files on disk,
but also things like S3 object stores, etc:

    - A ConfiguredAssetDataConnector requires an explicit listing of each DataAsset you want to connect to. This allows more fine-tuning, but also requires more setup.
    - An InferredAssetDataConnector infers ``data_asset_naßme`` by using a regex that takes advantage of patterns that exist in the filename or folder structure.

InferredAssetDataConnector has fewer options, so it's simpler to set up. It’s a good choice if you want to connect to a single ``DataAsset``, or several ``DataAssets`` that all share the same naming convention.

If you're not sure which one to use, please check out :ref:`How to choose which DataConnector to use. <which_data_connector_to_use>`

Set up a Datasource
-------------------

All of the examples below assume you’re testing configurations using something like:

.. code-block:: python

    import great_expectations as ge
    context = ge.DataContext()

    context.test_yaml_config("""
    my_data_source:
      class_name: Datasource
      execution_engine:
        class_name: PandasExecutionEngine
      data_connectors:
        my_filesystem_data_connector:
          {data_connector configuration goes here}
    """)


If you’re not familiar with the ``test_yaml_config`` method, please check out: :ref:`How to configure DataContext components using test_yaml_config. <how_to_guides_how_to_configure_datacontext_components_using_test_yaml_config>`


Choose a DataConnector
----------------------

InferredAssetDataConnectors like the ``InferredAssetFilesystemDataConnector`` and ``InferredAssetS3DataConnector``
require the following information to be initialized:

    1. name of data_connector as a ``str`` value
    2. ``class_name`` of data_connector, such as ``InferredAssetFilesystemDataConnector`` or ``InferredAssetS3DataConnector``
    3. ``datasource_name``: name of associated ``Datasource`` as ``str`` value
    4. ``default_regex`` which contains a regex ``pattern`` and a list of capture ``group_names``

The ``default_regex`` is used to take advantage of patterns that exist in the filename or folder structure.

Imagine you have the following files in ``my_directory/``:

.. code-block:: bash

    my_directory/alpha-2020-01-01.csv
    my_directory/alpha-2020-01-02.csv
    my_directory/alpha-2020-01-03.csv


Then we can imagine 2 approaches to analyzing the data.

The simplest approach would be to consider each file to be its own data_asset.  In that case, the configuration would look like the following:

.. code-block:: yaml

    my_data_source:
      class_name: Datasource
      execution_engine:
        class_name: PandasExecutionEngine
      data_connectors:
        my_filesystem_data_connector:
          class_name: InferredAssetFilesystemDataConnector
          datasource_name: my_data_source
          base_directory: my_directory/
          default_regex:
            group_names:
              - data_asset_name
            pattern: (.*).csv

Notice that the ``default_regex`` is configured to have one capture group in the pattern (``(.*)``) which captures the entire filename. That capture group is assigned to ``data_asset_name`` under ``group_names``.

However, a closer look at the file names reveals a pattern that is common to the 3 files. Each have the field ``alpha`` in the name, and have date information following.

These are the types of patterns that InferredAssetDataConnectors allow you to take advantage of. We can consider each file to be a ``batch`` of one ``data_asset`` named ``alpha``, which will allow you to..?

Taking this into consideration, if we were to capture ``alpha`` as the ``data_asset_name`` and add capture groups for the ``year``, ``month`` and ``day`` fields, then our configuration would become:

.. code-block:: yaml

    my_data_source:
      class_name: Datasource
      execution_engine:
        class_name: PandasExecutionEngine
      data_connectors:
        my_filesystem_data_connector:
          class_name: InferredAssetFilesystemDataConnector
          datasource_name: my_data_source
          base_directory: my_directory/
          default_regex:
            group_names:
              - data_asset_name
              - year
              - month
              - day
            pattern: (.*)-(\d{4})-(\d{2})-(\d{2}).csv

**Note** We have chosen to be more specific in the capture groups for the ``year`` ``month`` and ``day`` by specifying the integer value (using ``\d``) and the number of digits, but a simpler capture group like ``(.*)`` would also work.

A corresponding configuration for ``InferredAssetS3DataConnector`` would look similar but would require ``bucket`` and ``prefix`` values instead of ``base_directory``.

.. code-block:: yaml

    my_data_source:
      class_name: Datasource
      execution_engine:
        class_name: PandasExecutionEngine
      data_connectors:
        my_filesystem_data_connector:
          class_name: InferredAssetS3DataConnector
          datasource_name: my_data_source
          bucket: my_s3_bucket
          prefix: my_s3_bucket_prefix
          default_regex:
            group_names:
              - data_asset_name∂
              - year
              - month
              - day
            pattern: (.*)-(\d{4})-(\d{2})-(\d{2}).csv
e
The following examples will show scenarios that InferredAssetDataConnectors can help you analyze, using ``InferredAssetFilesystemDataConnector`` as an example and only show the configuration under ``data_connectors`` for simplicity.


Example 1: Basic configuration for a single DataAsset
-----------------------------------------------------

Continuing the example above, imagine you have the following files in the directory ``my_directory/``:

.. code-block:: bash

    my_directory/alpha-2020-01-01.csv
    my_directory/alpha-2020-01-02.csv
    my_directory/alpha-2020-01-03.csv


Then this configuration...

.. code-block:: yaml

    my_filesystem_data_connector
      class_name: InferredAssetFilesystemDataConnector
      base_directory: my_directory/
      default_regex:
        group_names:
          - data_asset_name
          - year
          - month
          - day
        pattern: (.*)-(\d{4})-(\d{2})-(\d{2}).csv

...will make available the following data_references:

.. code-block::

    Available data_asset_names (1 of 1):
        alpha (3 of 3): [
            'alpha-2020-01-01.csv',
            'alpha-2020-01-02.csv',
            'alpha-2020-01-03.csv'
        ]

    Unmatched data_references (0 of 0): []

Once configured, you can get ``Validators`` from the ``DataContext`` as follows:

.. code-block:: python

    my_validator = my_context.get_validator(
        execution_engine_name="my_execution_engine",
        data_connector_name="my_data_connector",
        data_asset_name="alpha",
        create_expectation_suite_with_name="my_expectation_suite",
    )

Example 2: Basic configuration with more than one DataAsset
-----------------------------------------------------------

Here’s a similar example, but this time two data_assets are mixed together in one folder.

**Note**: For an equivalent configuration using ``ConfiguredAssetFilesSystemDataconnector``, please see Example 2
in :ref:`How to configure an ConfiguredAssetDataConnector <how_to_guides_how_to_configure_a_configuredassetdataconnector>`

.. code-block::

    test_data/alpha-2020-01-01.csv
    test_data/beta-2020-01-01.csv
    test_data/alpha-2020-01-02.csv
    test_data/beta-2020-01-02.csv
    test_data/alpha-2020-01-03.csv
    test_data/beta-2020-01-03.csv

The same configuration as Example 1...

.. code-block:: yaml

    my_filesystem_data_connector
      class_name: InferredAssetFilesystemDataConnector
      base_directory: test_data/
      default_regex:
        group_names:
          - data_asset_name
          - year
          - month
          - day
      pattern: (.*)-(\d{4})-(\d{2})-(\d{2}).csv

...will now make ``alpha`` and ``beta`` both available a DataAssets, with the following data_references:

.. code-block::

    Available data_asset_names (2 of 2):
        alpha (3 of 3): [
            'alpha-2020-01-01.csv',
            'alpha-2020-01-02.csv',
            'alpha-2020-01-03.csv'
        ]

        beta (3 of 3): [
            'beta-2020-01-01.csv',
            'beta-2020-01-02.csv',
            'beta-2020-01-03.csv'
        ]

    Unmatched data_references (0 of 0): []


Example 3: Nested directory structure with the data_asset_name on the inside
----------------------------------------------------------------------------

Here’s a similar example, with a nested directory structure...

.. code-block::

    2020/01/01/alpha.csv
    2020/01/02/alpha.csv
    2020/01/03/alpha.csv
    2020/01/04/alpha.csv
    2020/01/04/beta.csv
    2020/01/05/alpha.csv
    2020/01/05/beta.csv

Then this configuration...

.. code-block:: yaml

    my_filesystem_data_connector
      class_name: InferredAssetFilesystemDataConnector
      base_directory: my_directory/
      default_regex:
        group_names:
          - year
          - month
          - day
          - data_asset_name
        pattern: (\d{4})/(\d{2})/(\d{2})/(.*).csv

...will now make ``alpha`` and ``beta`` both available a DataAssets, with the following data_references:

.. code-block::

    Available data_asset_names (2 of 2):
        alpha (3 of 5): [
            'alpha-2020-01-01.csv',
            'alpha-2020-01-02.csv',
            'alpha-2020-01-03.csv'
        ]

        beta (2 of 2): [
            'beta-2020-01-04.csv',
            'beta-2020-01-05.csv',
        ]

    Unmatched data_references (0 of 0): []


Example 4: Nested directory structure with the data_asset_name on the outside
-----------------------------------------------------------------------------

In the following example, files are placed in a folder structure with the ``data_asset_name`` defined by the folder name (A, B, C, or D)

.. code-block::

    A/A-1.csv
    A/A-2.csv
    A/A-3.csv
    B/B-1.csv
    B/B-2.csv
    B/B-3.csv
    C/C-1.csv
    C/C-2.csv
    C/C-3.csv
    D/D-1.csv
    D/D-2.csv
    D/D-3.csv

Then this configuration...

.. code-block:: yaml

    my_filesystem_data_connector
      class_name: InferredAssetFilesystemDataConnector
      base_directory: /

      default_regex:
        group_names:
          - data_asset_name
          - letter
          - number
        pattern: (\w{1})/(\w{1})-(\d{1}).csv


...will now make ``A`` and ``B`` and ``C`` into data_assets, with each containing 3 data_references

.. code-block::

	Available data_asset_names (3 of 4):
		A (3 of 3): ['test_dir_charlie/A/A-1.csv',
                    'test_dir_charlie/A/A-2.csv',
                    'test_dir_charlie/A/A-3.csv']
		B (3 of 3): ['test_dir_charlie/B/B-1.csv',
                    'test_dir_charlie/B/B-2.csv',
                    'test_dir_charlie/B/B-3.csv']
		C (3 of 3): ['test_dir_charlie/C/C-1.csv',
                    'test_dir_charlie/C/C-2.csv',
                    'test_dir_charlie/C/C-3.csv']

	Unmatched data_references (0 of 0): []


Example 5: Redundant information in the naming convention (S3 Bucket)
----------------------------------------------------------------------

Here’s another example of a nested directory structure with data_asset_name defined in the bucket_name.

.. code-block::

    my_bucket/2021/01/01/log_file-20210101.txt.gz,
    my_bucket/2021/01/02/log_file-20210102.txt.gz,
    my_bucket/2021/01/03/log_file-20210103.txt.gz,
    my_bucket/2021/01/04/log_file-20210104.txt.gz,
    my_bucket/2021/01/05/log_file-20210105.txt.gz,
    my_bucket/2021/01/06/log_file-20210106.txt.gz,
    my_bucket/2021/01/07/log_file-20210107.txt.gz,


Here’s a configuration that will allow all the log files in the bucket to be associated with a single data_asset, ``my_bucket``

.. code-block:: yaml

    my_filesystem_data_connector
      class_name: InferredAssetFilesystemDataConnector
      base_directory: /

      default_regex:
        group_names:
          - year
          - month
          - day
          - data_asset_name
        pattern: (\w{11})/(\d{4})/(\d{2})/(\d{2})/log_file-.*.csv


All the log files will be mapped to a single data_asset named ``my_bucket``.

.. code-block::

    Available data_asset_names (1 of 1):
        my_bucket (3 of 7): [
            'my_bucket/2021/01/03/log_file-*.csv',
            'my_bucket/2021/01/04/log_file-*.csv',
            'my_bucket/2021/01/05/log_file-*.csv'
        ]

    Unmatched data_references (0 of 0): []



Example 6: Random information in the naming convention
-------------------------------------------------------------------------------

In the following example, files are placed in folders according to the date of creation, and given a random hash value in their name.

.. code-block::

    2021/01/01/log_file-2f1e94b40f310274b485e72050daf591.txt.gz
    2021/01/02/log_file-7f5d35d4f90bce5bf1fad680daac48a2.txt.gz
    2021/01/03/log_file-99d5ed1123f877c714bbe9a2cfdffc4b.txt.gz
    2021/01/04/log_file-885d40a5661bbbea053b2405face042f.txt.gz
    2021/01/05/log_file-d8e478f817b608729cfc8fb750ebfc84.txt.gz
    2021/01/06/log_file-b1ca8d1079c00fd4e210f7ef31549162.txt.gz
    2021/01/07/log_file-d34b4818c52e74b7827504920af19a5c.txt.gz


Here’s a configuration that will allow all the log files to be associated with a single data_asset, ``log_file``

.. code-block:: yaml

    my_filesystem_data_connector
      class_name: InferredAssetFilesystemDataConnector
      base_directory: /

      default_regex:
        group_names:
          - year
          - month
          - day
          - data_asset_name
        pattern: (\d{4})/(\d{2})/(\d{2})/(log_file)-.*\.txt\.gz

... will give you the following output

.. code-block::

    Available data_asset_names (1 of 1):
        log_file (3 of 7): [
            '2021/01/03/log_file-*.txt.gz',
            '2021/01/04/log_file-*.txt.gz',
            '2021/01/05/log_file-*.txt.gz'
        ]

    Unmatched data_references (0 of 0): []


Example 7: Redundant information in the naming convention (timestamp of file creation)
--------------------------------------------------------------------------------------

In the following example, files are placed in a single folder, and the name includes a timestamp of when the files were created

.. code-block::

    log_file-2021-01-01-035419.163324.txt.gz
    log_file-2021-01-02-035513.905752.txt.gz
    log_file-2021-01-03-035455.848839.txt.gz
    log_file-2021-01-04-035251.47582.txt.gz
    log_file-2021-01-05-033034.289789.txt.gz
    log_file-2021-01-06-034958.505688.txt.gz
    log_file-2021-01-07-033545.600898.txt.gz


Here’s a configuration that will allow all the log files to be associated with a single data_asset named ``log_file``.

.. code-block:: yaml

    my_filesystem_data_connector

      class_name: InferredAssetFilesystemDataConnector
      base_directory: /

      default_regex:
        group_names:
          - data_asset_name
          - year
          - month
          - day
        pattern: (log_file)-(\\d{{4}})-(\\d{{2}})-(\\d{{2}})-.*\\.*\\.txt\\.gz


All the log files will be mapped to the data_asset ``log_file``.

.. code-block::

    Available data_asset_names (1 of 1):
        some_bucket (3 of 7): [
            'some_bucket/2021/01/03/log_file-*.txt.gz',
            'some_bucket/2021/01/04/log_file-*.txt.gz',
            'some_bucket/2021/01/05/log_file-*.txt.gz'
    ]

    Unmatched data_references (0 of 0): []


.. discourse::
   :topic_identifier: 522
