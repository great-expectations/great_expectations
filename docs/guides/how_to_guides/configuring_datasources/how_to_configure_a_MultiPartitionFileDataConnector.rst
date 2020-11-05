.. _how_to_guides__miscellaneous__how_to_configure_a_multipartitionfiledataconnector:

How to configure a ``MultiPartitionFileDataConnector``
=======================================================

This guide demonstrates how to configure a ``MultiPartitionFileDataConnector``, and provides several examples you can use for configuration.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
  - Set up a DataContext
  - Understand the basics of ExecutionEnvironments
  - Learned how to use ``test_yaml_config``

Great Expectations provides two ``DataConnector`` classes for connecting to file-system-like data: ``SinglePartitionerFileDataConnector`` and ``MultiPartitionFileDataConnector``.

``MultiPartitionFileDataConnector`` is more complex, and also more powerful. We recommend starting with ``SinglePartitionerFileDataConnector`` unless you're sure you need the extra features of a ``MultiPartitionFileDataConnector``. <<Link to "how to configure a SinglePartitionerFileDataConnector" here>

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

Principles for configuring ``MultiPartitionFileDataConnectors``
----------------------------------------------------------------

One of your ``group_names`` must be ``data_asset_name``.


Example 1: something something
-----------------------------------------------------

* test_dir_foxtrot
* test_dir_golf
* test_dir_hotel
* test_dir_india

For example, imagine you have the following files in the directory ``my_directory/``:

.. literalinclude:: test.txt

Then this configuration...

.. literalinclude:: multipartition_yaml_example_1.yaml
   :language: yaml

...will make available the following data_references:

.. code-block::

    Available data_asset_names (1 of 1):
        alpha (3 of 3): [
            'alpha-2020-01-01.csv',
            'alpha-2020-01-02.csv',
            'alpha-2020-01-03.csv'
        ]

    Unmatched data_references (0 of 0): []

.. invisible-code-block: python

    >>> import os
    >>> from great_expectations.data_context.util import file_relative_path
    >>> filename = file_relative_path(__file__, "./multipartition_yaml_example_1.yaml")
    >>> print(filename)
    >>> my_yaml_string = open(filename).read()
    >>> my_yaml_dict = yaml.load(my_yaml_string, Loader=yaml.FullLoader)
    >>> assert "baz_2" in my_yaml_dict["foo"]["bar"]

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

    class_name: SinglePartitionerFileDataConnector
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

    class_name: SinglePartitionerFileDataConnector
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
----------------------------

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
