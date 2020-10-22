.. _how_to_guides__miscellaneous__how_to_configure_a_FileSystemDataConnector:

How to configure a ``FileSystemDataConnector``
=======================================================

This guide demonstrates how to configure a ``FileSystemDataConnector``, and provides several examples you can use for configuration.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
  - Set up a DataContext
  - Understand the basics of ExecutionEnvironments
  - Learned how to use ``test_yaml_config``


Great Expectations provides two ``DataConnector`` classes for connecting to file-system-like data. ``FileSystemDataConnector`` and ``NamedDataAssetFileSystemDataConnector``. ``FileSystemDataConnector`` is a good choice if you want to connect to several ``DataAssets`` that all share the same naming convention. It's quick to configure, since it lets you make use of information that you've already encoded in your naming conventions.

If you're not sure which one to use, please check out [Reference/Which DataConnector should I use]?


<<< Abe 20201022 : All of this content should go in "Which DataConnector should I use?" >>>

Great Expectations provides two ``DataConnector`` classes for connecting to file-system-like data. ``FileSystemDataConnector`` and ``NamedDataAssetFileSystemDataConnector``. ``FileSystemDataConnector`` is a good choice if you want to connect to several ``DataAssets`` that all share the same naming convention. It's quick to configure, since it lets you make use of information that you've already encoded in your naming conventions.

For example, if you have a ``my_data`` directory structured as follows, and you want to treat the `A-*.csv` files as batches within the A DataAsset, and do the same for B and C:

.. code-block:: bash

    my_data/A/A-1.csv
    my_data/A/A-2.csv
    my_data/A/A-3.csv
    my_data/B/B-1.csv
    my_data/B/B-2.csv
    my_data/B/B-3.csv
    my_data/C/C-1.csv
    my_data/C/C-2.csv
    my_data/C/C-3.csv

In that case, this config...

.. code-block:: yaml

    class_name: FileSystemDataConnector
    base_directory: my_data/

    partitioner:
        class_name: RegexPartitioner
        group_names:
            - data_asset_name
            - id_
        pattern: (.*)/.*-(\d+).csv

...will make available the following DataAssets and data_references:

.. code-block:: bash

    Available data_asset_names (3 of 3):
        A (3 of 3): [
            'A/A-1.csv',
            'A/A-2.csv',
            'A/A-3.csv'
        ]
        B (3 of 3): [
            'B/B-1.csv',
            'B/B-2.csv',
            'B/B-3.csv'
        ]
        C (3 of 3): [
            'C/C-1.csv',
            'C/C-2.csv',
            'C/C-3.csv'
        ]

    Unmatched data_references (0 of 0): []


Note that the ``FileSystemDataConnector`` *infers DataAsset names from the regex you provide.* This is the key difference between ``FileSystemDataConnector``` and ``NamedDataAssetFileSystemDataConnector``. In addition, all of the DataAssets in a ``FileSystemDataConnector`` must share the same Partitioner.

For comparison, ``NamedDataAssetFileSystemDataConnector`` requires an explicit listing of each DataAsset you want to connect to, and each DataAsset can have a different Partitioner configuration. This tends to be helpful when the naming conventions for your DataAssets are less standardized.

.. code-block:: bash

    my_messier_data/1/A-1.csv
    my_messier_data/1/B-1.txt
        
    my_messier_data/2/A-2.csv
    my_messier_data/2/B-2.txt
        
    my_messier_data/3/A-3.csv
    my_messier_data/3/B-3.txt
        
    my_messier_data/2017/C.csv
    my_messier_data/2018/C.csv
    my_messier_data/2019/C.csv
        
    my_messier_data/aaa/D.csv
    my_messier_data/bbb/D.csv
    my_messier_data/ccc/D.csv

<transition text>

.. code-block:: yaml

    config:
        goes:
            here

<transition text>

.. code-block:: bash

    Available data_asset_names (4 of 4):
        A (3 of 3): [
            '1/A-1.csv',
            '2/A-2.csv',
        ]
        B (3 of 3): [
            '1/B-1.csv',
            '2/B-2.csv',
        ]
        C (3 of 3): [
            '2017/C.csv',
            '2018/C.csv',
            '2019/C.csv'
        ]
        D (3 of 3): [
            'aaa/D.csv',
            'bbb/D.csv',
            'ccc/D.csv'
        ]

    Unmatched data_references (0 of 0): []

<<< /end "Which DataConnector should I use?" >>>


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

.. code-block:: bash

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

test_dir_charlie

Example 6: Redundant information in the naming convention
---------------------------------------------------------

Example 3 here: https://github.com/superconductive/design/blob/main/docs/20201015_partitioners_v2.md


More examples to be written:
---------

* Missing information in the naming convention: Examples 1 and 2 here: https://github.com/superconductive/design/blob/main/docs/20201015_partitioners_v2.md
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
