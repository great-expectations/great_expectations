.. _which_data_connector_to_use:

How to choose which ``DataConnector`` to use
==============================================

Great Expectations provides two types of ``DataConnector`` classes for connecting to file-system-like data. This includes files on disk, but also S3 object stores, etc:

    - A ConfiguredAssetDataConnector allows users to have the most fine-tuning, and requires an explicit listing of each DataAsset you want to connect to. Examples of this type of ``DataConnector`` include ``ConfiguredAssetFilesystemDataConnector`` and ``ConfiguredAssetS3DataConnector``.
    - An InferredAssetDataConnector infers ``data_asset_name`` by using a regex that takes advantage of patterns that exist in the filename or folder structure. Examples of this type of ``DataConnector`` include ``InferredAssetFilesystemDataConnector`` and ``InferredAssetS3DataConnector``.

The following examples will use ``DataConnector`` classes designed to connect to files on disk, namely ``InferredAssetFilesystemDataConnector`` and ``ConfiguredAssetFilesystemDataConnector``.

------------------------------------------
When to use an InferredAssetDataConnector
------------------------------------------

If you have the following ``my_data/`` directory in your filesystem, and you want to treat the ``A-*.csv`` files as batches within the ``A`` DataAsset, and do the same for ``B`` and ``C``:

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

This config...

.. code-block:: yaml

    class_name: Datasource
    data_connectors:
        my_data_connector:
            class_name: InferredAssetFilesystemDataConnector
            base_directory: my_data/
            default_regex:
                pattern: (.*)/.*-(\d+)\.csv
                group_names:
                    - data_asset_name
                    - id

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

Note that the ``InferredAssetFileSystemDataConnector`` **infers** ``data_asset_names`` **from the regex you provide.** This is the key difference between InferredAssetDataConnector and ConfiguredAssetDataConnector, and also requires that one of the ``group_names`` in the ``default_regex`` configuration be ``data_asset_name``.

------------------------------------------
When to use a ConfiguredAssetDataConnector
------------------------------------------

On the other hand, ``ConfiguredAssetFilesSystemDataConnector`` requires an explicit listing of each DataAsset you want to connect to. This tends to be helpful when the naming conventions for your DataAssets are less standardized.

If you have the following ``my_messier_data/`` directory in your filesystem,

.. code-block:: bash

    my_messier_data/1/A-1.csv
    my_messier_data/1/B-1.txt

    my_messier_data/2/A-2.csv
    my_messier_data/2/B-2.txt

    my_messier_data/2017/C-1.csv
    my_messier_data/2018/C-2.csv
    my_messier_data/2019/C-3.csv

    my_messier_data/aaa/D-1.csv
    my_messier_data/bbb/D-2.csv
    my_messier_data/ccc/D-3.csv

Then this config...

.. code-block:: yaml

    class_name: Datasource
    execution_engine:
        class_name: PandasExecutionEngine
    data_connectors:
        my_data_connector:
            class_name: ConfiguredAssetFilesystemDataConnector
            glob_directive: "*/*"
            base_directory: my_messier_data/
            assets:
                A:
                    pattern: (.+A)-(\d+)\.csv
                    group_names:
                        - name
                        - id
                B:
                    pattern: (.+B)-(\d+)\.txt
                    group_names:
                        - name
                        - val
                C:
                    pattern: (.+C)-(\d+)\.csv
                    group_names:
                        - name
                        - id
                D:
                    pattern: (.+D)-(\d+)\.csv
                    group_names:
                        - name
                        - id


...will make available the following DataAssets and data_references:

.. code-block:: bash

    Available data_asset_names (4 of 4):
        A (2 of 2): [
            '1/A-1.csv',
            '2/A-2.csv'
        ]
        B (2 of 2): [
            '1/B-1.txt',
            '2/B-2.txt'
        ]
        C (3 of 3): [
            '2017/C-1.csv',
            '2018/C-2.csv',
            '2019/C-3.csv'
        ]
        D (3 of 3): [
            'aaa/D-1.csv',
            'bbb/D-2.csv',
            'ccc/D-3.csv'
        ]

----------------
Additional Notes
----------------

    - Additional examples and configurations for ``ConfiguredAssetFilesystemDataConnectors`` can be found here: :ref:`How to configure a ConfiguredAssetDataConnector <how_to_guides_how_to_configure_a_configuredassetdataconnector>`
    - Additional examples and configurations for ``InferredAssetFilesystemDataConnectors`` can be found here: :ref:`How to configure an InferredAssetDataConnector <how_to_guides_how_to_configure_a_inferredassetdataconnector>`

.. discourse::
   :topic_identifier: 520
