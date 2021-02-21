.. _how_to_guides__creating_batches__how_to_create_a_batch_request_from_an_inmemory_pandas_dataframe:

How to create a Batch Request from an in-memory Pandas DataFrame
================================================================

TODO: insert text

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Configured a Data Context <how_to_guides__configuring_data_contexts>`
  - :ref:`Configured a Datasource in your Data Context <how_to_guides__configuring_datasources>`
  - :ref:`Configured a RuntimeDataConnector in your Datasource <how_to_guides_how_to_configure_a_runtime_dataconnector>`

Steps
-----

#. **Create a BatchRequest.**

    TODO: insert text

    .. code-block:: yaml

        datasources:
          my_pandas_datasource:
            class_name: Datasource
            execution_engine:
              class_name: PandasExecutionEngine
            data_connectors:
              my_runtime_data_connector:
                  module_name: great_expectations.datasource.data_connector
                  class_name: RuntimeDataConnector
                  runtime_keys:
                      - some_key_maybe_pipeline_stage
                      - some_other_key_maybe_run_id


    TODO: insert text


    .. code-block:: python

        from great_expectations.core.batch import BatchRequest

        batch_request = BatchRequest(
            datasource_name="my_pandas_datasource",
            data_connector_name="my_runtime_data_connector",
            batch_data=df,
            data_asset_name="my_data_asset",
        )


    .. admonition:: Note:

        TODO: insert text about adding runtime_keys in partition_request (if your RuntimeDataConnector defined the keys it can accept)

       .. code-block:: python

            from great_expectations.core.batch import BatchRequest

            batch_request = BatchRequest(
                datasource_name="my_pandas_datasource",
                data_connector_name="my_runtime_data_connector",
                batch_data=df,
                data_asset_name="my_data_asset",
                partition_request={"partition_identifiers": {
                   "some_key_maybe_pipeline_stage": "ingestion step 1",
                   "some_other_key_maybe_run_id": "run 18"}}
            )

Additional notes
----------------

TODO: insert text about how would you use the BatchRequest you just created - a link to "how to create a Validator" - this article does not exist yet.

Additional resources
--------------------


Comments
--------

.. discourse::
   :topic_identifier: 99999
