.. _how_to_guides_how_to_configure_a_configuredassetdataconnector:

How to configure a ConfiguredAssetDataConnector
===============================================

This guide demonstrates how to configure a ConfiguredAssetDataConnector, and provides several examples you can use for configuration.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

    - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
    - :ref:`Understand the basics of Datasources in 0.13 or later <reference__core_concepts__datasources>`
    - Learned how to configure a :ref:`Data Context using test_yaml_config <how_to_guides_how_to_configure_datacontext_components_using_test_yaml_config>`

Great Expectations provides two ``DataConnector`` classes for connecting to file-system-like data. This includes files on disk,
but also S3 object stores, etc:

    - A ConfiguredAssetDataConnector requires an explicit listing of each DataAsset you want to connect to. This allows more fine-tuning, but also requires more setup.
    - An InferredAssetDataConnector infers ``data_asset_name`` by using a regex that takes advantage of patterns that exist in the filename or folder structure.

If you're not sure which one to use, please check out :ref:`How to choose which DataConnector to use. <which_data_connector_to_use>`

Set up a Datasource
-------------------

All of the examples below assume you’re testing configuration using something like:

.. code-block:: python

    import great_expectations as ge
    context = ge.get_context()
    config = f"""
      class_name: Datasource
      execution_engine:
        class_name: PandasExecutionEngine
      data_connectors:
        my_filesystem_data_connector:
          {data_connector configuration goes here}
    """
    context.test_yaml_config(
        name="my_pandas_datasource",
        yaml_config=config
    )

If you’re not familiar with the ``test_yaml_config`` method, please check out: :ref:`How to configure Data Context components using test_yaml_config. <how_to_guides_how_to_configure_datacontext_components_using_test_yaml_config>`

Choose a DataConnector
----------------------

ConfiguredAssetDataConnectors like  ``ConfiguredAssetFilesystemDataConnector`` and ``ConfiguredAssetS3DataConnector`` require DataAssets to be
explicitly named. Each DataAsset can have their own regex ``pattern`` and ``group_names``, and if configured, will override any
``pattern`` or ``group_names`` under ``default_regex``.

Imagine you have the following files in ``my_directory/``:

.. code-block:: bash

    my_directory/alpha-1.csv
    my_directory/alpha-2.csv
    my_directory/alpha-3.csv


We could create a DataAsset ``alpha`` that contains 3 data_references (``alpha-1.csv``, ``alpha-2.csv``, and ``alpha-3.csv``).
In that case, the configuration would look like the following:

.. code-block:: yaml

    my_data_source:
      class_name: Datasource
      execution_engine:
        class_name: PandasExecutionEngine
      data_connectors:
        my_filesystem_data_connector:
          class_name: ConfiguredAssetFilesystemDataConnector
          base_directory: my_directory/
          default_regex:
          assets:
            alpha:
              pattern: alpha-(.*)\.csv
              group_names:
                - index

Notice that we have specified a pattern that captures the number after ``alpha-`` in the filename and assigns it to the ``group_name`` ``index``.

The configuration would also work with a regex capturing the entire filename (ie ``pattern: (.*)\\.csv``).  However, capturing the index on its own allows for ``batch_identifiers`` to be used to retrieve a specific Batch of the Data Asset.

Later on we could retrieve the data in ``alpha-2.csv`` of ``alpha`` as its own batch using ``context.get_batch()`` by specifying ``{"index": "2"}`` as the ``batch_identifier``.

.. code-block:: python

    my_batch = context.get_batch(
        datasource_name="my_data_source",
        data_connector_name="my_filesystem_data_connector",
        data_asset_name="alpha",
        batch_identifiers={"index": "2"}
        )


This ability to access specific Batches using ``batch_identifiers`` is very useful when validating DataAssets that span multiple files.
For more information on ``batches`` and ``batch_identifiers``, please refer to the :ref:`Core Concepts document. <reference__core_concepts>`

A corresponding configuration for ``ConfiguredAssetS3DataConnector`` would look similar but would require ``bucket`` and ``prefix`` values instead of ``base_directory``.

.. code-block:: yaml

    class_name: ConfiguredAssetS3DataConnector
    bucket: MY_S3_BUCKET
    prefix: MY_S3_BUCKET_PREFIX
    default_regex:
    assets:
        alpha:
          pattern: alpha-(.*)\.csv
          group_names:
            - index

The following examples will show scenarios that ConfiguredAssetDataConnectors can help you analyze, using ``ConfiguredAssetFilesystemDataConnector``.

**Note**: The examples will only only show the configuration for ``data_connectors`` for simplicity.

Example 1: Basic Configuration for a single DataAsset
-----------------------------------------------------

Continuing the example above, imagine you have the following files in the directory ``my_directory/``:

.. code-block::

    test/alpha-1.csv
    test/alpha-2.csv
    test/alpha-3.csv

Then this configuration...

.. code-block:: yaml

    class_name: ConfiguredAssetFilesystemDataConnector
    base_directory: test/
    default_regex:
    assets:
        alpha:
          pattern: alpha-(.*)\.csv
          group_names:
            - index

...will make available ``alpha`` as a single DataAsset with the following data_references:

.. code-block:: bash

    Available data_asset_names (1 of 1):
        alpha (3 of 3): [
            'alpha-1.csv',
            'alpha-2.csv',
            'alpha-3.csv'
        ]

Once configured, you can get a ``Validator`` from the ``Data Context`` as follows:

.. code-block:: python

    my_validator = context.get_validator(
        datasource_name="my_data_source",
        data_connector_name="my_filesystem_data_connector",
        data_asset_name="alpha",
        batch_identifiers={
            "index": "2"
        },
        expectation_suite_name="my_expectation_suite" # the suite with this name must exist by the time of this call

    )

But what if the regex does not match any files in the directory?

Then this configuration...

.. code-block:: yaml

    class_name: ConfiguredAssetFilesystemDataConnector
    base_directory: test/
    default_regex:
    assets:
        alpha:
          pattern: beta-(.*)\.csv
          group_names:
            - index

...will give you this output

.. code-block:: yaml

    Successfully instantiated ConfiguredAssetFilesystemDataConnector
    Available data_asset_names (1 of 1):
        alpha (0 of 0): []

    Unmatched data_references (3 of 3): ['alpha-1.csv', 'alpha-2.csv', 'alpha-3.csv']

Notice that ``alpha`` has 0 data_references, and there are 3 `Unmatched data_references` listed.
This would indicate that some part of the configuration is incorrect and would need to be reviewed.
In our case, changing ``pattern`` to : ``alpha-(.*)\\.csv`` will fix our problem and give the same output to above.


Example 2: Basic configuration with more than one DataAsset
-----------------------------------------------------------

Here’s a similar example, but this time two data_assets are mixed together in one folder.

**Note**: For an equivalent configuration using ``InferredAssetFileSystemDataConnector``, please see Example 2 in :ref:`How to configure an InferredAssetDataConnector <how_to_guides_how_to_configure_a_inferredassetdataconnector>`

.. code-block::

    test_data/alpha-2020-01-01.csv
    test_data/beta-2020-01-01.csv
    test_data/alpha-2020-01-02.csv
    test_data/beta-2020-01-02.csv
    test_data/alpha-2020-01-03.csv
    test_data/beta-2020-01-03.csv

Then this configuration...

.. code-block:: yaml

    class_name: ConfiguredAssetFilesystemDataConnector
    base_directory: test_data/
    assets:
        alpha:
            group_names:
                - name
                - year
                - month
                - day
            pattern: alpha-(\d{4})-(\d{2})-(\d{2})\.csv
        beta:
            group_names:
                - name
                - year
                - month
                - day
            pattern: beta-(\d{4})-(\d{2})-(\d{2})\.csv

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

Example 3: Example with Nested Folders
--------------------------------------------------

In the following example, files are placed folders that match the ``data_asset_names`` we want: ``A``, ``B``, ``C``, and ``D``.

.. code-block::

    test_dir/A/A-1.csv
    test_dir/A/A-2.csv
    test_dir/A/A-3.csv
    test_dir/B/B-1.txt
    test_dir/B/B-2.txt
    test_dir/B/B-3.txt
    test_dir/C/C-2017.csv
    test_dir/C/C-2018.csv
    test_dir/C/C-2019.csv
    test_dir/D/D-aaa.csv
    test_dir/D/D-bbb.csv
    test_dir/D/D-ccc.csv
    test_dir/D/D-ddd.csv
    test_dir/D/D-eee.csv


.. code-block:: yaml

    module_name: great_expectations.datasource.data_connector
    class_name: ConfiguredAssetFilesystemDataConnector
    base_directory: test_dir/
    assets:
        A:
            base_directory: A/
        B:
            base_directory: B/
            pattern: (.*)-(.*)\.txt
            group_names:
                - part_1
                - part_2
        C:
            glob_directive: "*"
            base_directory: C/
        D:
            glob_directive: "*"
            base_directory: D/
    default_regex:
        pattern: (.*)-(.*)\.csv
        group_names:
            - part_1
            - part_2

...will now make ``A``, ``B``, ``C`` and ``D``  available a DataAssets, with the following data_references:

.. code-block:: bash

    Available data_asset_names (4 of 4):
        A (3 of 3): [
            'A-1.csv',
            'A-2.csv',
            'A-3.csv',
        ]
        B (3 of 3):  [
            'B-1',
            'B-2',
            'B-3',
        ]
        C (3 of 3): [
            'C-2017',
            'C-2018',
            'C-2019',
        ]
        D (5 of 5): [
            'D-aaa.csv',
            'D-bbb.csv',
            'D-ccc.csv',
            'D-ddd.csv',
            'D-eee.csv',
        ]


Example 4: Example with Explicit data_asset_names and more complex nesting
--------------------------------------------------------------------------

In this example, the assets ``alpha``, ``beta`` and ``gamma`` are being explicitly defined in the configuration, and have a more complex nesting pattern.

.. code-block::

    my_base_directory/alpha/files/go/here/alpha-202001.csv
    my_base_directory/alpha/files/go/here/alpha-202002.csv
    my_base_directory/alpha/files/go/here/alpha-202003.csv
    my_base_directory/beta_here/beta-202001.txt
    my_base_directory/beta_here/beta-202002.txt
    my_base_directory/beta_here/beta-202003.txt
    my_base_directory/beta_here/beta-202004.txt
    my_base_directory/gamma-202001.csv
    my_base_directory/gamma-202002.csv
    my_base_directory/gamma-202003.csv
    my_base_directory/gamma-202004.csv
    my_base_directory/gamma-202005.csv

The following configuration...

.. code-block:: yaml

    class_name: ConfiguredAssetFilesystemDataConnector
    base_directory: my_base_directory/
    default_regex:
        pattern: ^(.+)-(\d{4})(\d{2})\.(csv|txt)$
        group_names:
            - data_asset_name
            - year_dir
            - month_dir
    assets:
        alpha:
            base_directory: my_base_directory/alpha/files/go/here/
            glob_directive: "*.csv"
        beta:
            base_directory: my_base_directory/beta_here/
            glob_directive: "*.txt"
        gamma:
            glob_directive: "*.csv"

...will make ``alpha``, ``beta`` and ``gamma``  available a DataAssets, with the following data_references:

.. code-block::

    Available data_asset_names (3 of 3):
        alpha (3 of 3): [
            'alpha-202001.csv',
            'alpha-202002.csv',
            'alpha-202003.csv'
        ]
        beta (4 of 4):  [
            'beta-202001.txt',
            'beta-202002.txt',
            'beta-202003.txt',
            'beta-202004.txt'
        ]
        gamma (5 of 5): [
            'gamma-202001.csv',
            'gamma-202002.csv',
            'gamma-202003.csv',
            'gamma-202004.csv',
            'gamma-202005.csv',
        ]


Additional Resources
--------------------

.. discourse::
   :topic_identifier: 521
