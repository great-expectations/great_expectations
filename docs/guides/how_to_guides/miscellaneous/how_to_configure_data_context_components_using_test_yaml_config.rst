.. _how_to_guides__miscellaneous__how_to_configure_a_multipartitionfiledataconnector:

How to configure DataContext components using ``test_yaml_config``
==================================================================

``test_yaml_config`` is a convenience method for configuring the moving parts of a Great Expectations deployment. It allows you to quickly test out configs for Datasources, Checkpoints, and each type of Store (ExpectationStores, ValidationResultStores, and MetricsStores). For many deployments of Great Expectations, these components (plus Expectations) are the only ones you'll need.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
  - Set up a DataContext
  - Understand the basics of ExecutionEnvironments
  - Learned how to use ``test_yaml_config``

``test_yaml_config`` is primarily intended for use within a notebook, where you can iterate through an edit-run-check loop in seconds.

Steps
-----

#. **Instantiate a DataContext**

    .. code-block:: python

        import great_expectations as ge
        context = ge.get_context()

#. **Create or copy a yaml config**

    You can create your own, or copy an example. For this example, we'll demonstrate using a Datasource that connects to postgresql.

    .. code-block:: python

        my_config = """
        class_name: StreamlinedSqlExecutionEnvironment
        credentials:
            drivername: postgresql
            username: postgres
            password: ""
            host: localhost
            port: 5432
            database: test_ci

        introspection:
            whole_table: {}
        """

#. **Run context.test_yaml_config.**

    .. code-block:: python

        context.test_yaml_config(
            name="my_postgresql_datasource",
            yaml_config=yaml_config
        )

    When executed, ``test_yaml_config`` will instantiate the component and run through a ``self_check`` procedure to verify that the component works as expected.
    
    In the case of a Datasource, this means

        1. confirming that the connection works,
        2. gathering a list of available DataAssets (e.g. tables in SQL; files or folders in a filesystem), and
        3. verify that it can successfully fetch at least one Batch from the source.

    The output will look something like this:

    .. code-block:: bash

        Attempting to instantiate class from config...
            Instantiating as a ExecutionEnvironment, since class_name is ExecutionEnvironment
            Successfully instantiated ExecutionEnvironment

        Execution engine: PandasExecutionEngine
        Data connectors:
            my_filesystem_data_connector : InferredAssetFilesystemDataConnector

            Available data_asset_names (1 of 1):
                DEFAULT_ASSET_NAME (3 of 10): ['abe_20200809_1040.csv', 'alex_20200809_1000.csv', 'alex_20200819_1300.csv']

            Unmatched data_references (0 of 0): []

            Choosing an example data reference...
                Reference chosen: alex_20200819_1300.csv

                Fetching batch data...
                Successfuly fetched 100 rows of data.

    If something about your configuration wasn't set up correctly, ``test_yaml_config`` will raise an error.  Whenever possible, test_yaml_config provides helpful warnings and error messages. It can't solve every problem, but it can solve many.


    .. code-block:: bash

        Attempting to instantiate class from config...
            Instantiating as a ExecutionEnvironment, since class_name is StreamlinedSqlExecutionEnvironment
        ---------------------------------------------------------------------------
        DatabaseError                             Traceback (most recent call last)
        ~/anaconda2/envs/py3/lib/python3.7/site-packages/sqlalchemy/engine/base.py in _wrap_pool_connect(self, fn, connection)
        2338         try:
        -> 2339             return fn()
        2340         except dialect.dbapi.Error as e:

        ...

        DatabaseError: (snowflake.connector.errors.DatabaseError) 250001 (08001): Failed to connect to DB: oca29081.us-east-1.snowflakecomputing.com:443. Incorrect username or password was specified.
        (Background on this error at: http://sqlalche.me/e/13/4xp6)


#. **Iterate as necessary.**



#. **Iterate as necessary.**

.. code-block:: yaml

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
