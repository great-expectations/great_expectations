.. _how_to_guides__miscellaneous__how_to_configure_a_singlepartitionfiledataconnector:

How to configure a ``SinglePartitionFileDataConnector``
=======================================================

This guide demonstrates how to configure a ``SinglePartitionFileDataConnector``, and provides several examples you can use for configuration.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
  - Set up a DataContext
  - Understand the basics of ExecutionEnvironments
  - Learned how to use ``test_yaml_config``

Great Expectations provides two ``DataConnector`` classes for connecting to file-system-like data: ``SinglePartitionFileDataConnector`` and ``MultiPartitionFileDataConnector``.

``SinglePartitionFileDataConnector`` has fewer options, so it's simpler to set up. It’s a good choice if you want to connect to a single ``DataAsset``, or several ``DataAssets`` that all share the same naming convention. It's not difficult to migrate from a ``SinglePartitionFileDataConnector`` to a ``MultiPartitionFileDataConnector``, so we recommend starting with ``SinglePartitionFileDataConnector`` unless you're sure you need the extra features of a ``MultiPartitionFileDataConnector``.

Steps
-----

#. **Set up an ExecutionEnvironment with the following configuration**

All of the examples below assume you’re testing configuration using something like:

.. code-block:: python

    import great_expectations as ge
    context = ge.DataContext()

    context.test_yaml_config("""
    class_name: ExecutionEnvironment

    execution_engine:
        class_name: PandasExecutionEngine

    data_connectors:
        my_filesystem_data_connector:
            {data_connector configuration goes here}
    """)


If you’re not familiar with the ``test_yaml_config`` method, please check out [How to configure all sorts of stuff with test_yaml_config]() or the corresponding [video tutorial here]().

Principles for configuring ``SinglePartitionFileDataConnectors``
----------------------------------------------------------------

One of your ``group_names`` must be ``data_asset_name``.


Example 1: Basic configuration for a single DataAsset
-----------------------------------------------------

For example, imagine you have the following files in the directory ``my_directory/``:

.. code-block:: python

    alpha-2020-01-01.csv
    alpha-2020-01-02.csv
    alpha-2020-01-03.csv


Then this configuration...

.. code-block:: yaml

    class_name: SinglePartitionFileDataConnector
    base_directory: my_directory/

    partitioner:
        class_name: RegexPartitioner
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
        partition_request={
            year="2020",
            month="01",
            day="01",
        }
    )

Example 2: Basic configuration with more than one DataAsset
-----------------------------------------------------------

Here’s a similar example, with two different DataAssets mixed together.

.. code-block::

    alpha-2020-01-01.csv
    beta-2020-01-01.csv
    alpha-2020-01-02.csv
    beta-2020-01-02.csv
    alpha-2020-01-03.csv
    beta-2020-01-03.csv

The same configuration as Example 1...

.. code-block:: yaml

    class_name: SinglePartitionFileDataConnector
    base_directory: my_directory/

    partitioner:
        class_name: RegexPartitioner
        group_names:
            - data_asset_name
            - year
            - month
            - day
        pattern: (.*)-(\d{4})-(\d{2})-(\d{2}).csv

...will now make "alpha" and "beta" both available a DataAssets, with the following data_references:

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


Example 4: Nested directory structure with the data_asset_name on the inside
----------------------------------------------------------------------------

Here’s another example...

.. code-block::

    2020/01/01/alpha.csv
    2020/01/02/alpha.csv
    2020/01/03/alpha.csv
    2020/01/04/alpha.csv
    2020/01/04/beta.csv
    2020/01/05/alpha.csv
    2020/01/05/beta.csv

Here’s a configuration...

.. code-block:: yaml

    class_name: SinglePartitionFileDataConnector
    base_directory: my_directory/

    partitioner:
        class_name: RegexPartitioner
        group_names:
            - year
            - month
            - day
            - data_asset_name
        pattern: (\d{4})/(\d{2})/(\d{2})/(.*).csv

...will now make "alpha" and "beta" both available a DataAssets, with the following data_references:

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


Example 5: Nested directory structure with the data_asset_name on the outside
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

Here’s a configuration...

.. code-block:: yaml

    class_name: SinglePartitionerFileDataConnector
    base_directory: /

    partitioner:
        class_name: RegexPartitioner
        group_names:
            - data_asset_name
            - letter
            - number
        pattern: (\w{1})/(\w{1})-(\d{1}).csv


...will now make "A" and "B" and "C" into data_assets, with each containing 3 data_references

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


Example 6: Redundant information in the naming convention (S3 Bucket)
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

    class_name: SinglePartitionerFileDataConnector
    base_directory: /

    partitioner:
        class_name: RegexPartitioner
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



Example 7: Redundant information in the naming convention (random hash in name)
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

    class_name: SinglePartitionerFileDataConnector
    base_directory: /

    partitioner:
        class_name: RegexPartitioner
        config_params:
            regex:
                group_names:
                  - year
                  - month
                  - day
                  - data_asset_name
                pattern: (\d{4})/(\d{2})/(\d{2})/(log_file)-.*\.txt\.gz


.. code-block::

    Available data_asset_names (1 of 1):
        log_file (3 of 7): [
            '2021/01/03/log_file-*.txt.gz',
            '2021/01/04/log_file-*.txt.gz',
            '2021/01/05/log_file-*.txt.gz'
        ]

    Unmatched data_references (0 of 0): []


Example 8: Redundant information in the naming convention (timestamp of file creation)
-------------------------------------------------------------------------------

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

    class_name: SinglePartitionerFileDataConnector
    base_directory: /

    partitioner:
      class_name: RegexPartitioner
      config_params:
      regex:
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


More examples to be written:
---------

* Extraneous files; show "Unmatched data_references"; show how to filter out with the optional glob_directive parameter: test_dir_juliette
* {{{Example to demonstrate sorting}}}
* {{{Example to demonstrate grouping}}}
* {{{Example to demonstrate splitting}}}
* {{{Example to demonstrate sampling}}}
* Be careful with regexes: test_dir_lima
* If there are many files, then `test_yaml_config` will only show three. (<>What's the workflow here?</>): test_dir_november


Additional Resources
--------------------


.. discourse::
   :topic_identifier: NEED TO ADD ID HERE
