.. _how_to_guides__how_to_sort_batches:

How to sort ``Batches``
=======================

.. warning::

  Sorters are configured through a ``DataConnector`` which a concept that was introduced in the V3 API.  Therefore the following how-to-guide is not supported by the V2 (batch_kwargs) API.

This guide demonstrates how to sort :ref:`batches <reference__core_concepts__glossary__datasources>` of data that Great Expectations can validate from a filesystem using a configured ``DataConnector``.
To read more about DataConnectors please refer to the doc: :ref:`How to choose which DataConnector to use <which_data_connector_to_use>`.   For this how-to-guide, we will be using a
``ConfiguredAssetFilesystemDataConnector`` as an example, but the sorting batches is also supported by the ``SqlDataConnector``.  GE currently supports ``LexicographicalSorter``, ``NumericSorter``, ``DateTimeSorter``,
``CustomListSorter``, and you can write your own sorters by extending the ``Sorter`` class.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

    - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
    - :ref:`Understand the basics of Datasources in 0.13 or later <reference__core_concepts__datasources>`
    - Learned how to configure a :ref:`DataContext using test_yaml_config <how_to_guides_how_to_configure_datacontext_components_using_test_yaml_config>`

-----
Steps
-----

.. content-tabs::

    .. tab-container:: tab0
        :title: LexiographicalSorter

        If you have the following ``lexicographic_example`` directory in your filesystem, and you want to treat ``*.csv`` files as batches within the ``my_data_asset`` DataAsset:

          .. code-block:: bash

            lexicographic_example/test_aaa.csv
            lexicographic_example/test_bbb.csv
            lexicographic_example/test_ccc.csv
            lexicographic_example/test_ddd.csv
            lexicographic_example/test_eee.csv


        1. **Load or create a DataContext**

          .. code-block:: python

            import great_expectations as ge
            from great_expectations.cli.datasource import sanitize_yaml_and_save_datasource
            from great_expectations.core.batch import BatchRequest

            context = ge.get_context()

        2. **Configure a Datasource**

          In the following configuration, a Datasource is configured with a ``PandasExecutionEngine`` and ``ConfiguredAssetFilesystemDataConnector``. The DataConnector is configured with a single DataAsset named ``my_reports``.
          It has the ``base_directory`` set to ``reports/`` and the regex ``pattern`` is set to capture two ``group_names``, ``name`` and ``num``. A ``LexicographicalSorter`` is configured for the ``letter`` capture group, which
          captures the section of the file name that looks like : ``aaa``.

          .. code-block:: python

            config = f"""
                name: mydatasource
                class_name: Datasource
                execution_engine:
                    class_name: PandasExecutionEngine
                data_connectors:
                  my_data_connector:
                      module_name: great_expectations.datasource.data_connector
                      class_name: ConfiguredAssetFilesystemDataConnector
                      glob_directive: "*.csv"
                      base_directory: /Users/work/Development/GE_data/lexicographic_example/
                      default_regex:
                          pattern: (.+)_(.+)\\.csv
                          group_names:
                              - name
                              - letter
                      sorters:
                          - orderby: desc
                            class_name: LexicographicSorter
                            name: letter
                      assets:
                          my_data_asset:
              """

        3. **(Optional) run** ``test_yaml_config()`` **to ensure that your configuration is working.**

          .. code-block:: python

            context.test_yaml_config(
                yaml_config=config
            )

          If the configuration is correct you should see output similar to this. Notice that the data asset names start with ``test_eee.csv``, showing that the Batches have been sorted correctly.

          .. code-block:: bash

            Attempting to instantiate class from config...
              Instantiating as a Datasource, since class_name is Datasource
              Successfully instantiated Datasource

            ExecutionEngine class name: PandasExecutionEngine
            Data Connectors:
              my_data_connector : ConfiguredAssetFilesystemDataConnector

              Available data_asset_names (1 of 1):
                my_data_asset (3 of 5): ['test_eee.csv', 'test_ddd.csv', 'test_ccc.csv']

                Unmatched data_references (0 of 0): []


        4. **Save Configuration**

          .. code-block:: python

            sanitize_yaml_and_save_datasource(context, config, overwrite_existing=False)
            context = ge.get_context()


        5. **Obtain an ExpectationSuite**

          Your DataContext can be used to create or retrieve an ExpectationSuite.

          .. code-block:: python

            suite = context.get_expectation_suite("insert_your_expectation_suite_name_here")

          Alternatively, if you have not already created a suite, you can do so now.

          .. code-block:: python

            suite = context.create_expectation_suite("insert_your_expectation_suite_name_here")


        6. **Construct a** ``BatchRequest``.

          The following ``BatchRequest`` will retrieve a the first Batch corresponding to ``test_eee.csv`` by using index ``0`` as the  ``data_connector_query``.

          .. code-block:: python

            batch_request = BatchRequest(
              datasource_name="mydatasource",
              data_connector_name="my_data_connector",
              data_asset_name="my_reports",
              data_connector_query={
                  "index": 0
                  }
                )

        7. **Construct a Validator**

          The ``BatchRequest`` and ExpectationSuite can be used to create a Validator.

          .. code-block:: python

            my_validator = context.get_validator(
              batch_request=batch_request,
              expectation_suite=suite
            )


        8. **Check your Validator**

          You can check to see if the correct Batch was retrieved by checking the ``active_batch``'s ``batch_definition``.

          .. code-block:: python

            my_validator.active_batch.batch_definition

          The expected output should show ``batch_identifiers`` corresponding to ``test_eee.csv`` namely ``"{'name': 'test', 'letter': 'eee'}"}``

          .. code-block:: python

            {'datasource_name': 'mydatasource', 'data_connector_name': 'my_data_connector', 'data_asset_name': 'my_reports', 'batch_identifiers': "{'name': 'test', 'letter': 'eee'}"}



    .. tab-container:: tab1
        :title: NumericalSorter

        If you have the following ``numeric_example`` directory in your filesystem, and you want to treat ``*.csv`` files as batches within the ``my_data_asset`` DataAsset:

          .. code-block:: bash

            numeric_example/test_111.csv
            numeric_example/test_222.csv
            numeric_example/test_333.csv
            numeric_example/test_444.csv
            numeric_example/test_555.csv


        1. **Load or create a DataContext**

          .. code-block:: python

            import great_expectations as ge
            from great_expectations.cli.datasource import sanitize_yaml_and_save_datasource
            from great_expectations.core.batch import BatchRequest

            context = ge.get_context()

        2. **Configure a Datasource**

          In the following configuration, a Datasource is configured with a ``PandasExecutionEngine`` and ``ConfiguredAssetFilesystemDataConnector``.
          The DataConnector is configured with a single DataAsset named ``my_reports``. It has the ``base_directory`` set to ``reports/``
          and the regex ``pattern`` is set to capture two ``group_names``, ``name`` and ``num``. A ``NumericSorter`` is configured for the ``number`` capture group, which
          captures the section of the file name that looks like : ``111``.

          .. code-block:: python

            config = f"""
                name: mydatasource
                class_name: Datasource
                execution_engine:
                    class_name: PandasExecutionEngine
                data_connectors:
                  my_data_connector:
                      module_name: great_expectations.datasource.data_connector
                      class_name: ConfiguredAssetFilesystemDataConnector
                      glob_directive: "*.csv"
                      base_directory: /Users/work/Development/GE_data/lexicographic_example/
                      default_regex:
                          pattern: (.+)_(\\d.*)\\.csv
                          group_names:
                              - name
                              - number
                      sorters:
                          - orderby: desc
                            class_name: NumericSorter
                            name: number
                      assets:
                          my_data_asset:
              """

        3. **(Optional) run** ``test_yaml_config()`` **to ensure that your configuration is working.**

          .. code-block:: python

            context.test_yaml_config(
                yaml_config=config
            )

          If the configuration is correct you should see output similar to this. Notice that the data asset names start with ``test_555.csv``, showing that the Batches have been sorted correctly.

          .. code-block:: bash

            Attempting to instantiate class from config...
              Instantiating as a Datasource, since class_name is Datasource
              Successfully instantiated Datasource

            ExecutionEngine class name: PandasExecutionEngine
            Data Connectors:
              my_data_connector : ConfiguredAssetFilesystemDataConnector

              Available data_asset_names (1 of 1):
                my_data_asset (3 of 5): ['test_555.csv', 'test_444.csv', 'test_333.csv']

                Unmatched data_references (0 of 0): []

        4. **Save Configuration**

          .. code-block:: python

            sanitize_yaml_and_save_datasource(context, config, overwrite_existing=False)
            context = ge.get_context()


        5. **Obtain an ExpectationSuite**

          Your DataContext can be used to create or retrieve an ExpectationSuite.

          .. code-block:: python

            suite = context.get_expectation_suite("insert_your_expectation_suite_name_here")

          Alternatively, if you have not already created a suite, you can do so now.

          .. code-block:: python

            suite = context.create_expectation_suite("insert_your_expectation_suite_name_here")


        6. **Construct a** ``BatchRequest``.

          The following ``BatchRequest`` will retrieve a the first Batch corresponding to ``test_555.csv`` by using index ``0`` as the  ``data_connector_query``.

          .. code-block:: python

            batch_request = BatchRequest(
              datasource_name="mydatasource",
              data_connector_name="my_data_connector",
              data_asset_name="my_reports",
              data_connector_query={
                  "index": 0
                  }
                )

        7. **Construct a Validator**

          The ``BatchRequest`` and ExpectationSuite can be used to create a Validator.

          .. code-block:: python

            my_validator = context.get_validator(
              batch_request=batch_request,
              expectation_suite=suite
            )


        8. **Check your Validator**

          You can check to see if the correct Batch was retrieved by checking the ``active_batch``'s ``batch_definition``.

          .. code-block:: python

            my_validator.active_batch.batch_definition

          The expected output should show ``batch_identifiers`` corresponding to ``test_555.csv`` namely ``"{'name': 'test', 'number': '555'}"}``

          .. code-block:: python

            {'datasource_name': 'mydatasource', 'data_connector_name': 'my_data_connector', 'data_asset_name': 'my_reports', 'batch_identifiers': "{'name': 'test', 'number': '555'}"}




    .. tab-container:: tab2
        :title: DatetimeSorter

        If you have the following ``datetime_example`` directory in your filesystem, and you want to treat ``*.csv`` files as batches within the ``my_data_asset`` DataAsset:

          .. code-block:: bash

            datetime_example/test_20201229.csv
            datetime_example/test_20201230.csv
            datetime_example/test_20201231.csv
            datetime_example/test_20210101.csv
            datetime_example/test_20210102.csv


        1. **Load or create a DataContext**

          .. code-block:: python

            import great_expectations as ge
            from great_expectations.cli.datasource import sanitize_yaml_and_save_datasource
            from great_expectations.core.batch import BatchRequest

            context = ge.get_context()

        2. **Configure a Datasource**

          In the following configuration, a Datasource is configured with a ``PandasExecutionEngine`` and ``ConfiguredAssetFilesystemDataConnector``. The DataConnector is configured with a single DataAsset named ``my_reports``. It has the ``base_directory`` set to ``reports/`` and the
          regex ``pattern`` is set to capture two ``group_names``, ``name`` and ``date``.  A ``DateTimeSorter`` is configured for the ``date`` capture group, which
          captures the section of the file name that looks like : ``20210102``. A ``DateTimeSorter`` also includes a ``datetime_format`` parameter, which specifies the pattern in ``datetime`` format.

          .. code-block:: python

            config = f"""
                name: mydatasource
                class_name: Datasource
                execution_engine:
                    class_name: PandasExecutionEngine
                data_connectors:
                  my_data_connector:
                      module_name: great_expectations.datasource.data_connector
                      class_name: ConfiguredAssetFilesystemDataConnector
                      glob_directive: "*.csv"
                      base_directory: /Users/work/Development/GE_data/lexicographic_example/
                      default_regex:
                          pattern: (.+)_(.+)\\.csv
                          group_names:
                              - name
                              - date
                      sorters:
                        - orderby: desc
                           class_name: DateTimeSorter
                           datetime_format: "%Y%m%d"
                           name: date
                      assets:
                          my_data_asset:
              """

        3. **(Optional) run** ``test_yaml_config()`` **to ensure that your configuration is working.**

          .. code-block:: python

            context.test_yaml_config(
                yaml_config=config
            )

          If the configuration is correct you should see output similar to this. Notice that the data asset names start with ``test_20210102.csv``, showing that the Batches have been sorted correctly.

          .. code-block:: bash

            Attempting to instantiate class from config...
              Instantiating as a Datasource, since class_name is Datasource
              Successfully instantiated Datasource

            ExecutionEngine class name: PandasExecutionEngine
            Data Connectors:
              my_data_connector : ConfiguredAssetFilesystemDataConnector

              Available data_asset_names (1 of 1):
                my_data_asset (3 of 5): ['test_20210102.csv', 'test_20210101.csv', 'test_20201231.csv']

                Unmatched data_references (0 of 0): []

        4. **Save Configuration**

          .. code-block:: python

            sanitize_yaml_and_save_datasource(context, config, overwrite_existing=False)
            context = ge.get_context()


        5. **Obtain an ExpectationSuite**

          Your DataContext can be used to create or retrieve an ExpectationSuite.

          .. code-block:: python

            suite = context.get_expectation_suite("insert_your_expectation_suite_name_here")

          Alternatively, if you have not already created a suite, you can do so now.

          .. code-block:: python

            suite = context.create_expectation_suite("insert_your_expectation_suite_name_here")


        6. **Construct a** ``BatchRequest``.

          The following ``BatchRequest`` will retrieve a the first Batch corresponding to ``test_20210102.csv`` by using index ``0`` as the  ``data_connector_query``.

          .. code-block:: python

            batch_request = BatchRequest(
              datasource_name="mydatasource",
              data_connector_name="my_data_connector",
              data_asset_name="my_reports",
              data_connector_query={
                  "index": 0
                  }
                )

        7. **Construct a Validator**

          The ``BatchRequest`` and ExpectationSuite can be used to create a Validator.

          .. code-block:: python

            my_validator = context.get_validator(
              batch_request=batch_request,
              expectation_suite=suite
            )


        8. **Check your Validator**

          You can check to see if the correct Batch was retrieved by checking the ``active_batch``'s ``batch_definition``.

          .. code-block:: python

            my_validator.active_batch.batch_definition

          The expected output should show ``batch_identifiers`` corresponding to ``test_20210102.csv`` namely ``"{'name': 'test', 'date': '20210102'}"}``

          .. code-block:: python

            {'datasource_name': 'mydatasource', 'data_connector_name': 'my_data_connector', 'data_asset_name': 'my_reports', 'batch_identifiers': "{'name': 'test', 'date': '20210102'}"}


    .. tab-container:: tab3
        :title: CustomListSorter

        Great Expectations also allows sorters to be configured against a custom ordering. If you have the following ``elements`` directory in your filesystem, and you want to treat ``*.csv`` files as batches within the ``my_data_asset`` DataAsset:

          .. code-block:: bash

            elements/test_H.csv
            elements/test_He.csv
            elements/test_Li.csv
            elements/test_Be.csv
            elements/test_B.csv
            elements/test_C.csv

        1. **Load or create a DataContext**

          .. code-block:: python

            import great_expectations as ge
            from great_expectations.cli.datasource import sanitize_yaml_and_save_datasource
            from great_expectations.core.batch import BatchRequest

            context = ge.get_context()

        2. **Configure a Datasource**

          In the following configuration, a Datasource is configured with a ``PandasExecutionEngine`` and ``ConfiguredAssetFilesystemDataConnector``. The DataConnector is configured with a single DataAsset named ``my_reports``
          It has the ``base_directory`` set to ``reports/`` and the regex ``pattern`` is set to capture two ``group_names``, ``name`` and ``element``.

          In this configuration we are also passing in a custom list containing the order for first 6 elements in the Periodic Table of Elements.

          .. code-block:: python

            # custom list that we are passing containing the ordering for the first 6 elements
            my_custom_list = ["H", "He", "Li", "Be", "B", "C"]

            config = f"""
                name: mydatasource
                class_name: Datasource
                execution_engine:
                    class_name: PandasExecutionEngine
                data_connectors:
                  my_data_connector:
                      module_name: great_expectations.datasource.data_connector
                      class_name: ConfiguredAssetFilesystemDataConnector
                      glob_directive: "*.csv"
                      base_directory: /Users/work/Development/GE_data/lexicographic_example/
                      default_regex:
                          pattern: (.+)_(.+)\\.csv
                          group_names:
                              - name
                              - element
                      sorters:
                        - orderby: asc
                          class_name: CustomListSorter
                          reference_list: {my_custom_list}
                          name: element
                      assets:
                          my_data_asset:
              """

        3. **(Optional) run** ``test_yaml_config()`` **to ensure that your configuration is working.**

          .. code-block:: python

            context.test_yaml_config(
                yaml_config=config
            )

          If the configuration is correct you should see output similar to this. Notice that the data asset names start with ``test_H.csv``, showing that the Batches have been sorted correctly.

          .. code-block:: bash

            Attempting to instantiate class from config...
              Instantiating as a Datasource, since class_name is Datasource
              Successfully instantiated Datasource

            ExecutionEngine class name: PandasExecutionEngine
            Data Connectors:
              my_data_connector : ConfiguredAssetFilesystemDataConnector

              Available data_asset_names (1 of 1):
                my_data_asset (3 of 5): ['test_H.csv', 'test_He.csv', 'test_Li.csv']

                Unmatched data_references (0 of 0): []


        4. **Save Configuration**

          .. code-block:: python

            sanitize_yaml_and_save_datasource(context, config, overwrite_existing=False)
            context = ge.get_context()


        5. **Obtain an ExpectationSuite**

          Your DataContext can be used to create or retrieve an ExpectationSuite.

          .. code-block:: python

            suite = context.get_expectation_suite("insert_your_expectation_suite_name_here")

          Alternatively, if you have not already created a suite, you can do so now.

          .. code-block:: python

            suite = context.create_expectation_suite("insert_your_expectation_suite_name_here")


        6. **Construct a** ``BatchRequest``.

          The following ``BatchRequest`` will retrieve a the first Batch corresponding to ``test_H.csv`` by using index ``0`` as the  ``data_connector_query``.

          .. code-block:: python

            batch_request = BatchRequest(
              datasource_name="mydatasource",
              data_connector_name="my_data_connector",
              data_asset_name="my_reports",
              data_connector_query={
                  "index": 0
                  }
                )

        7. **Construct a Validator**

          The ``BatchRequest`` and ExpectationSuite can be used to create a Validator.

          .. code-block:: python

            my_validator = context.get_validator(
              batch_request=batch_request,
              expectation_suite=suite
            )


        8. **Check your Validator**

          You can check to see if the correct Batch was retrieved by checking the ``active_batch``'s ``batch_definition``.

          .. code-block:: python

            my_validator.active_batch.batch_definition

          The expected output should show ``batch_identifiers`` corresponding to ``test_H.csv`` namely ``"{'name': 'test', 'element': 'H'}"}``

          .. code-block:: python

            {'datasource_name': 'mydatasource', 'data_connector_name': 'my_data_connector', 'data_asset_name': 'my_reports', 'batch_identifiers': "{'name': 'test', 'element': 'H'}"}



    .. tab-container:: tab4
        :title: Multiple Sorters

        If your configuration contains multiple sorters, they will be applied in sequence according to their configuration. So in the following example, we will first have the ``DateTimeSorter`` applied, then ``LexicographicSorter`` then ``NumericSorter``.

        .. code-block:: bash

          elements/test_AAA_111_20201230.csv
          elements/test_BBB_222_20201231.csv
          elements/test_CCC_333_20210101.csv
          elements/test_DDD_444_20210102.csv
          elements/test_EEE_555_20210103.csv

        1. **Load or create a DataContext**

          .. code-block:: python

            import great_expectations as ge
            from great_expectations.cli.datasource import sanitize_yaml_and_save_datasource
            from great_expectations.core.batch import BatchRequest

            context = ge.get_context()

        2. **Configure a Datasource**

        In the following configuration, a Datasource is configured with a ``PandasExecutionEngine`` and ``ConfiguredAssetFilesystemDataConnector``. The DataConnector is configured with a single DataAsset named ``my_reports``
        It has the ``base_directory`` set to ``reports/`` and the regex ``pattern`` is set to capture two ``group_names``, ``name``,  ``letter``, ``number`` and ``datetime``.

        We also have 3 Sorters configured, first for a ``DateTimeSorter`` for the ``datetime`` field, a ``LexicographicSorter`` for the ``letter`` field and a ``NumericSorter`` for the ``number`` field.

          .. code-block:: python

            config = f"""
              name: mydatasource
              class_name: Datasource
              execution_engine:
                class_name: PandasExecutionEngine
              data_connectors:
                my_data_connector:
                  module_name: great_expectations.datasource.data_connector
                  class_name: ConfiguredAssetFilesystemDataConnector
                  glob_directive: "*.csv"
                  base_directory: /Users/work/Development/GE_data/combination_example/
                  default_regex:
                    pattern: (.+)_(.+)_(\\d.*)_(.+)\\.csv
                    group_names:
                        - name
                        - letter
                        - number
                        - datetime
                  sorters:
                    - orderby: asc
                      class_name: DateTimeSorter
                      name: datetime
                    - orderby: desc
                      class_name: LexicographicSorter
                      name: letter
                    - orderby: desc
                      class_name: NumericSorter
                      name: number

                  assets:
                    combination_asset:
              """


        3. **(Optional) run** ``test_yaml_config()`` **to ensure that your configuration is working.**

          .. code-block:: python

            context.test_yaml_config(
                yaml_config=config
            )

          If the configuration is correct you should see output similar to this. Notice that the data asset names start with ``test_AAA_111_20201230.csv``, showing that the Batches have been sorted correctly.

          .. code-block:: bash

            Attempting to instantiate class from config...
              Instantiating as a Datasource, since class_name is Datasource
              Successfully instantiated Datasource

            ExecutionEngine class name: PandasExecutionEngine
            Data Connectors:
              my_data_connector : ConfiguredAssetFilesystemDataConnector

              Available data_asset_names (1 of 1):
                my_data_asset (3 of 5): ['test_AAA_111_20201230.csv', 'test_BBB_222_20201231.csv', 'test_CCC_333_20210101.csv']

                Unmatched data_references (0 of 0): []


        4. **Save Configuration**

          .. code-block:: python

            sanitize_yaml_and_save_datasource(context, config, overwrite_existing=False)
            context = ge.get_context()


        5. **Obtain an ExpectationSuite**

          Your DataContext can be used to create or retrieve an ExpectationSuite.

          .. code-block:: python

            suite = context.get_expectation_suite("insert_your_expectation_suite_name_here")

          Alternatively, if you have not already created a suite, you can do so now.

          .. code-block:: python

            suite = context.create_expectation_suite("insert_your_expectation_suite_name_here")


        6. **Construct a** ``BatchRequest``.

          The following ``BatchRequest`` will retrieve a the first Batch corresponding to ``test_H.csv`` by using index ``0`` as the  ``data_connector_query``.

          .. code-block:: python

            batch_request = BatchRequest(
              datasource_name="mydatasource",
              data_connector_name="my_data_connector",
              data_asset_name="my_reports",
              data_connector_query={
                  "index": 0
                  }
                )

        7. **Construct a Validator**

          The ``BatchRequest`` and ExpectationSuite can be used to create a Validator.

          .. code-block:: python

            my_validator = context.get_validator(
              batch_request=batch_request,
              expectation_suite=suite
            )


        8. **Check your Validator**

          You can check to see if the correct Batch was retrieved by checking the ``active_batch``'s ``batch_definition``.

          .. code-block:: python

            my_validator.active_batch.batch_definition

            - name
            - letter
            - number
            - datetime

          The expected output should show ``batch_identifiers`` corresponding to ``test_AAA_111_20201230.csv`` namely ``"{'name': 'test', 'letter': 'AAA', 'number': '111', 'datetime': '20201230'}"}``

          .. code-block:: python

            {'datasource_name': 'mydatasource', 'data_connector_name': 'my_data_connector', 'data_asset_name': 'my_reports', 'batch_identifiers': "{'name': 'test', 'element': 'H'}"}


    .. tab-container:: tab5
        :title: Sorters with Custom Filters

        You can also use Sorters in combination with custom filter functions. If you have the following ``year_reports`` directory in your filesystem, and you want to treat ``*.csv`` files as batches within the ``my_data_asset`` DataAsset,
        and we only wanted to consider the reports on or after 2000, but in ascending order:

        .. code-block:: bash

          year_reports/1980.csv
          year_reports/1990.csv
          year_reports/2000.csv
          year_reports/2010.csv
          year_reports/2020.csv


        1. **Load or create a DataContext**

          .. code-block:: python

            import great_expectations as ge
            from great_expectations.cli.datasource import sanitize_yaml_and_save_datasource
            from great_expectations.core.batch import BatchRequest

            context = ge.get_context()

        2. **Configure a Datasource**

        In the following configuration, a Datasource is configured with a ``PandasExecutionEngine`` and ``ConfiguredAssetFilesystemDataConnector``. The DataConnector is configured with a single DataAsset named ``my_reports``
        It has the ``base_directory`` set to ``reports/`` and the regex ``pattern`` is set to capture two ``group_names``, ``name``,  ``year`` .

        We also have a ``NumericSorter`` configured, to sort the ``year`` field in ascending order.


          .. code-block:: python

            config = f"""
              name: mydatasource
              class_name: Datasource
              execution_engine:
                class_name: PandasExecutionEngine
              data_connectors:
                my_data_connector:
                  module_name: great_expectations.datasource.data_connector
                  class_name: ConfiguredAssetFilesystemDataConnector
                  glob_directive: "*.csv"
                  base_directory: year_reports/
                  default_regex:
                    pattern: (.+)_(\\d.*)\\.csv
                    group_names:
                        - name
                        - year
                  sorters:
                    - orderby: asc
                      class_name: NumericSorter
                      name: year
                  assets:
                    combination_asset:
              """

        6. **Construct a** ``BatchRequest``.

          The following ``BatchRequest`` will retrieve a Batch corresponding to ``2000.csv``.  The ``my_custom_batch_selector`` takes in the ``batch_identifiers`` as a dictionary, and applies a filter on the ``year`` key. 
          .. code-block:: python

            # only select files from on or after 2000
            def my_custom_batch_selector(batch_identifiers: dict) -> bool:
              return int(batch_identifiers["year"]) >= 2000

            batch_request: Union[dict, BatchRequest] = {
              "datasource_name": "my_datasource",
              "data_connector_name": "my_data_connector",
              "data_asset_name": "YearTest",
              "data_connector_query": {
                  "custom_filter_function": my_custom_batch_selector,
              },
            }


        7. **Construct a Validator**

          The ``BatchRequest`` and ExpectationSuite can be used to create a Validator.

          .. code-block:: python

            my_validator = context.get_validator(
              batch_request=batch_request,
              expectation_suite=suite
            )


        8. **Check your Validator**

          You can check to see if the correct Batch was retrieved by checking the ``active_batch``'s ``batch_definition``.

          .. code-block:: python

            my_validator.active_batch.batch_definition


          The expected output should show ``batch_identifiers`` corresponding to ``2000.csv`` namely ``"{'name': 'test', 'year': 2000}"}``

          .. code-block:: python

            {'datasource_name': 'mydatasource', 'data_connector_name': 'my_data_connector', 'data_asset_name': 'my_reports', 'batch_identifiers': "{'name': 'test', 'year': '2000'}"}
