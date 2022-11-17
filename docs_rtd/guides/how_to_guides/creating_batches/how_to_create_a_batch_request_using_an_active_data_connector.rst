.. _how_to_guides__creating_batches__create_a_batch_using_an_active_data_connector:

How to load a Batch using an active Data Connector
===================================================

.. warning::

  Data Connector is a concept that was introduced in the V3 (BatchRequest) API, so the following configuration is not supported by the V2 (batch_kwargs) API.

This guide demonstrates how to get a :ref:`batch <reference__core_concepts__glossary__datasources>` of data that Great Expectations can validate from a filesystem using an active Data Connector. A ``FilesystemDataConnector``, or ``SqlDataConnector``
becomes active when we load the DataContext into memory and use the configured Datasource to retrieve a Batch of data from a filesystem or database.  For this how-to-guide, we will be using a ``ConfiguredAssetFilesystemDataConnector``.

You can read more about the differences between ``ConfiguredAssetDataConnector`` and ``InferredAssetDataConnectors`` :ref:`here <which_data_connector_to_use>`, and the :ref:`Datasources reference <reference__core_concepts__datasources>` for more information.


.. admonition:: Prerequisites: This how-to guide assumes you have already:

    - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
    - :ref:`Understand the basics of Datasources in 0.13 or later <reference__core_concepts__datasources>`
    - Learned how to configure a :ref:`DataContext using test_yaml_config <how_to_guides_how_to_configure_datacontext_components_using_test_yaml_config>`

Steps
-----

If you have the following ``reports`` directory in your filesystem, and you want to treat ``*.csv`` files as batches within the ``reports`` DataAsset:

**Note** : In our example, the ``base_directory`` is set to ``../``. If we are running this Notebook in the same folder as Great Expectations home directory (ie ``great_expectations/``), GE will begin looking for the files in the parent directory.

  .. code-block:: bash

    reports/yellow_tripdata_sample_2019-01.csv
    reports/yellow_tripdata_sample_2019-02.csv


1. **Load or create a DataContext**

  .. code-block:: python

    import great_expectations as ge
    from great_expectations.cli.datasource import sanitize_yaml_and_save_datasource
    from great_expectations.core.batch import BatchRequest

    context = ge.get_context()


2. **Configure a Datasource**

  In the following configuration, a Datasource is configured with a ``PandasExecutionEngine`` and ``ConfiguredAssetFilesystemDataConnector``. The Data Connector is configured with a single DataAsset named ``my_reports``. It has the ``base_directory`` set to ``reports/`` and the regex ``pattern`` is set to capture three ``group_names``, ``name``, ``year`` and ``month``.

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
          base_directory: ../
          glob_directive: "*.csv"
          assets:
            my_reports:
               base_directory: reports/
               pattern: (.+)_(\\d.*)-(\\d.*)\\.csv
               group_names:
                 - name
                 - year
                 - month

    """


3. **(Optional) run** ``test_yaml_config()`` **to ensure that your configuration is working.**

  .. code-block:: python

    context.test_yaml_config(
        yaml_config=config
    )


  If the configuration is correct you should see output similar to this.

  .. code-block:: bash

    Attempting to instantiate class from config...
      Instantiating as a Datasource, since class_name is Datasource
      Successfully instantiated Datasource

    ExecutionEngine class name: PandasExecutionEngine
    Data Connectors:
      my_data_connector : ConfiguredAssetFilesystemDataConnector

      Available data_asset_names (1 of 1):
        my_reports (2 of 2): ['yellow_tripdata_sample_2019-01.csv', 'yellow_tripdata_sample_2019-02.csv']

        Unmatched data_references (0 of 0): []


4. **Save Configuration**

  .. code-block:: python

    # save the configuration and re-instantiate the data context with our newly configured datasource
    sanitize_yaml_and_save_datasource(context, config, overwrite_existing=False)
    context = ge.get_context()

5. **Obtain an ExpectationSuite**

  Your DataContext can be used to create or retrieve an ExpectationSuite.

  .. code-block:: python

    suite = context.get_expectation_suite("insert_your_expectation_suite_name_here")

  Alternatively, if you have not already created a suite, you can do so now.

  .. code-block:: python

    suite = context.create_expectation_suite("insert_your_expectation_suite_name_here")


6. **Construct a BatchRequest**.

  The following BatchRequest will retrieve a Batch corresponding to ``yellow_tripdata_sample_2019-01.csv`` by using ``batch_filter_parameters`` as a ``data_connector_query``.  Additional examples of ``data_connector_query`` like ``index`` can be found below.

  .. code-block:: python

    batch_request = BatchRequest(
      datasource_name="mydatasource",
      data_connector_name="my_data_connector",
      data_asset_name="my_reports",
      data_connector_query={
      "batch_filter_parameters":{
        "year": "2019",
        "month": "01"
            }
          }
        )


7. **Construct a Validator**

  The BatchRequest and ExpectationSuite can be used to create a Validator.

  .. code-block:: python

    my_validator = context.get_validator(
      batch_request=batch_request,
      expectation_suite=suite
    )


8. **Check your Validator**

  You can check to see if the correct Batch was retrieved by checking the ``active_batch``'s ``batch_definition``.

  .. code-block:: python

    my_validator.active_batch.batch_definition

  The expected output should show ``batch_identifiers`` corresponding to ``yellow_tripdata_sample_2019-01.csv`` namely ``"{'name': 'yellow_tripdata_sample', 'year': '2019', 'month': '01'}"}``

  .. code-block:: python

    {'datasource_name': 'mydatasource', 'data_connector_name': 'my_data_connector', 'data_asset_name': 'my_reports', 'batch_identifiers': "{'name': 'yellow_tripdata_sample', 'year': '2019', 'month': '01'}""}


  You can also check that the first few lines of your Batch are what you expect by running:

  .. code-block:: python

    my_validator.active_batch.head()


  Now that you have a Validator, you can use it to create Expectations or validate the data.


Additional Notes
----------------

BatchRequest can also support ``index`` in the ``data_connector_query``.

  Using the same ``reports`` directory as above:

  .. code-block:: bash

    reports/yellow_tripdata_sample_2019-01.csv
    reports/yellow_tripdata_sample_2019-02.csv


  The BatchRequest can retrieve Batches by ``index``. The following examples retrieve the first (``index = 0``)

  .. code-block:: python

    batch_request = BatchRequest(
      datasource_name="mydatasource",
      data_connector_name="my_data_connector",
      data_asset_name="my_reports",
      data_connector_query={
          "index": 0
          }
        )

    my_validator = context.get_validator(
      batch_request=batch_request,
      expectation_suite=suite
    )
    print(my_validator.active_batch.batch_definition)

    # batch corresponding to yellow_tripdata_sample_2019-01.csv
    {'datasource_name': 'mydatasource', 'data_connector_name': 'my_data_connector', 'data_asset_name': 'my_reports', 'batch_identifiers': "{'name': 'yellow_tripdata_sample', 'year': '2019', 'month': '01'}"}

  last (``index=-1``) batches.

  .. code-block:: python

    batch_request = BatchRequest(
      datasource_name="mydatasource",
      data_connector_name="my_data_connector",
      data_asset_name="my_reports",
      data_connector_query={
          "index": -1
          }
        )

    my_validator = context.get_validator(
      batch_request=batch_request,
      expectation_suite=suite
    )

    print(my_validator.active_batch.batch_definition)

    # batch corresponding to yellow_tripdata_sample_2019-02.csv
    {'datasource_name': 'mydatasource', 'data_connector_name': 'my_data_connector', 'data_asset_name': 'my_reports', 'batch_identifiers': "{'name': 'yellow_tripdata_sample', 'year': '2019', 'month': '02'}"}


.. discourse::
    :topic_identifier: 696
