.. _how_to_guides__creating_batches__how_to_create_a_batch_request_using_an_active_data_connector:

How to create a BatchRequest using an active Data Connector
=======================================================================

TODO: insert text that explains that this guide will show how to create a BatchRequest .... since the DataConnector already mapped the data in the
Datasource into data assets and their partitions, the steps and the code in this guide will looks the same regardless of where the data comes from (database, etc.)

:ref:`Data Connector <reference__core_concepts__datasources__data_connector>`


.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Configured a Data Context <how_to_guides__configuring_data_contexts>`
  - :ref:`Configured a Datasource in your Data Context <how_to_guides__configuring_datasources>`
  - Configured one of the active DataConnectors in your Datasource: :ref:`Choose which Data Connector to use <which_data_connector_to_use>`

Steps
-----


#. **Create a BatchRequest**

    TODO: insert text

    .. code-block:: python

        batch_request = BatchRequest(
            datasource_name="my_pandas_datasource",
            data_connector_name="my_filesystem_data_connector",
            data_asset_name="my_data_asset",
            partition_request={"partition_identifiers": {"number": "3"}}
        )

    .. admonition:: Note:

        TODO: insert text about adding batch_spec_passthrough

       .. code-block:: python

        batch_request = BatchRequest(
            datasource_name="my_pandas_datasource",
            data_connector_name="my_filesystem_data_connector",
            data_asset_name="my_data_asset",
            partition_request={"partition_identifiers": {"number": "3"}},
            batch_spec_passthrough={
                "sampling_method": "_sample_using_hash",
                "sampling_kwargs": {
                    "column_name": "date",
                    "hash_function_name": "md5",
                    "hash_value": "f",
                },
            },
        )


Additional notes
----------------

TODO: insert text about how would you use the BatchRequest you just created - a link to "how to create a Validator" - this article does not exist yet.


Additional resources
--------------------


Comments
--------

.. discourse::
   :topic_identifier: 604

