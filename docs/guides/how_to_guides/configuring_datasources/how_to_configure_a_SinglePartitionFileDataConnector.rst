.. _how_to_guides__miscellaneous__how_to_configure_a_singlepartitionfiledataconnector:

How to configure a ``SinglePartitionFileDataConnector``
=======================================================

Great Expectations provides two ``DataConnector`` classes for connecting to file-system-like data:

	1. ``SinglePartitionFileDataConnector``
	2. ``MultiPartitionFileDataConnector``

``SinglePartitionFileDataConnector`` is less flexible, but simpler to configure. It’s a good choice if you want to connect to a single ``DataAsset``, or several ``DataAssets`` that all share the same naming convention.

This guide demonstrates how to configure a ``SinglePartitionFileDataConnector``, and provides several examples you can use for configuration.

This guide assumes that you have already

	* Set up a DataContext
	* Configured an ExecutionEnvironment
	* Learned how to use ``test_yaml_config``

All of the examples below assume you’re testing configuration using something like:

.. code-block:: python

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


Example 1
---------

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

...will make available the following DataAsset and data_references:

.. code-block::

	Available data_asset_names (2 of 2):
		alpha (3 of 3): [
			'alpha-2020-01-01.csv',
			'alpha-2020-01-02.csv',
			'alpha-2020-01-03.csv'
		]

	Unmatched data_references (0 of 0): []

Once configured, you can get Validators from the DataContext as follows:

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

Example 2
---------

Here’s a similar example, with two different data_assets mixed together.

.. code-block::

    alpha-2020-01-01.csv
    beta-2020-01-01.csv
    alpha-2020-01-02.csv
    beta-2020-01-02.csv
    alpha-2020-01-03.csv
    beta-2020-01-03.csv

This configuration...

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

...will make available the following DataAssets and data_references:

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

Example 3
---------

Here’s another example...

.. code-block::

    2020/01/01/alpha.csv
    2020/01/01/beta.csv
    2020/01/02/alpha.csv
    2020/01/02/beta.csv
    2020/01/03/alpha.csv
    2020/01/03/beta.csv

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


Example 4
---------
...


Example 5
---------
...


Additional Resources
--------------------


.. discourse::
   :topic_identifier: NEED TO ADD ID HERE
