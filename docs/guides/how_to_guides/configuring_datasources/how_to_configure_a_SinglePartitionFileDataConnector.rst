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
