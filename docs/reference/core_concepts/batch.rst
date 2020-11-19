.. _batch:

####################################################################################################
BATCHES (Most of this was taken from 20200813_datasource_configuration.md in github.com/design repo)
####################################################################################################

Most of this was taken from ``20200813_datasource_configuration.md``

********************
What is a ``batch``?
********************

- It is a combination of data and metadata, as the following figure shows

    .. image:: /images/batch_what_is_a_batch.png



- ``Batch`` is assembled at the ``DataSource`` using the following components :

.. code-block:: python

    new_batch = Batch(
        data = batch_data,
        batch_request=batch_request
        batch_definition=batch_definition,
        batch_spec=batch_spec,
        batch_markers=batch_markers,
    )

- the returned ``new_batch`` may look something like this:

.. code-block:: python

    Batch(
        data,
        batch_request: {
            "datasource": "myds"
            "data_connector": "pipeline",
            "data_asset_name": "????",
            "partition_request" : {
                "partition_identifiers" : {
                    "airflow_run_id": "string_airflow_run_id_that_was_provided",
                    "other_key": "string_other_key_that_was_provided",
                }
                "custom_filter_function": my_filter_fn,
                "limit": 10,
                "index": Optional[Union[int, list, tuple, slice, str]],  # examples: 0; "-1"; [3:7]; "[2:4]"
            }
        },
        batch_definition: {
            "datasource": "myds",
            "data_connector": "pipeline",
            "data_asset_name": "????",
            "partition_definition": {
            "airflow_run_id": "string_airflow_run_id_that_was_provided",
            "other_key": "string_other_key_that_was_provided",
          }
        }
    batch_spec: {
            in_memory_df: True
    },
    batch_markers: {
        "pandas_hash": "hash"
        }
    )

***************************
How to we get to ``Batch``?
***************************

- It is a journey from ``batch_request`` to ``batch_definition`` to ``batch_spec`` to ``Batch``, with the outline shown in the following diagram.

    .. image:: /images/batch_life_of_a_batch.png

1. ``batch_request`
    - A `BatchRequest` is the object a user passes to the `DataSource` to request a `Batch` (or `Batches`).
        - It can include `partition_request`(* do we allow `batch_spec_passthrough` here?) params with values relative to the latest batch (e.g. slices). Conceptually, this enables “fetch the latest `Batch`” behavior. It also means that a `BatchRequest` does NOT necessarily uniquely identify the `Batch(es)` to be fetched.
        - when resolved `BatchRequest` may point to many `BatchDefinitions` and Batches.
    - **Note** : A `BatchRequest` can include a `partition_request` that points to many Batches
    - A user will be able to load a `batch` using `get_batch_list_from_batch_request()` from the `DataSource`

.. code-block:: python

    DataSource.get_batch_list_from_batch_request(batch_request={
        "datasource": "myds",
        "data_connector": "pipeline",
        "in_memory_dataset": df,
        "partition_request" : {
        "partition_identifiers" : {
            "airflow_run_id": my_run_id,
            "other_key": my_other_key
        }
        "custom_filter_function": my_filter_fn,
        "limit": 10,
        "index": Optional[Union[int, list, tuple, slice, str]],  # examples: 0; "-1"; [3:7]; "[2:4]"
        },
        "sampling": {
            "limit": 1000,
            "sample": 0.1
        }
    })


2. ``batch_definition``
    -  A ``BatchDefinition`` resolves any ambiguity in ``BatchRequest`` to uniquely identify a single ``Batch`` to be fetched.  `BatchDefinition` is data-source- and engine-agnostic. Its parameters may depend on the configuration of the ``DataSource``, but they do not otherwise depend on the specific datasource type (e.g. filesystem, SQL, etc.) or ExecutionEngine being used to instantiate ``Batches``.
    - the ``DataConnector`` generates the ``batch_definition`` internally, which now has a uniquely identifying section. It will be the result of the ``batch_request`` being processed or digested, and will contain information to identify a single ``Batch`` that will be fetched.

.. code-block:: yaml

    BatchDefinition
        datasource: str
        data_connector: str
        data_asset_name: str
        partition_definition:
            ** persistent, unique identifier for the partition **
            ** contents depend on the configuration of the DataConnector **


3. ``batch_spec``

    - A ``BatchSpec`` is a set of specific instructions for the ``ExecutionEngine`` to fetch the appropriate data.

    - It is built by the ``DataConnector`` from the following :
        1. ``batch_definition`` : passed to it by ``DataSource``
        2. ``pass_through``??? : passed to it by ``DataSource`` or ``BatchRequest``???
        3. additional information (strictly coming from ``PartitionDefinition`` (or ``partition_request``??? <alex to check>) on how to actually get the data (ie. ``FilesDataConnector`` will know about ``path``).
        4. ``BatchMarkers`` that are calculated by the ``ExecutionEngine`` when a ``Batch`` is instantiated. They are metadata that can be used to calculate performance characteristics, ensure reproducibility of (not just queries) but underlying data, etc.

    - The ``batch_spec`` is a a set of specific instructions that are now needed to fetch the appropriate data

##############################
``Partition``-level Terminology
##############################

*******************
``partition_request``
*******************
- ``partition_request``
    - the goal of ``partition_request`` is to identify a single partition, from which a ``Batch`` will be built.
    - Any cases of zero or multiple partitions will be flagged as a bad ``partition_request`` (meaning that the user would be asked to refine this query).

************************
``partition_definition``
************************
    - persistent, unique identifier for the partition
    - contents depend on the configuration of the ``DataConnector``

************************************************************
``runtime_keys`` and their relation to ``partition_definition``
************************************************************
- ``runtime_keys`` are the ``partition_definition`` in the special case of a ``PipelineDataConnector``.
- Reasoning: ``runtime_keys`` perform the same function as ``pipeline_definition``: a persistent, unique identifier for the partition of data included in the ``Batch``. Instead of relying on our own config to retrieve the same data, for a ``PipelineDataConnector``, the closest we can get to reproducibility is "go ask your pipeline runner."

- The new ``runtime_keys`` argument is a user-provided list whose keys are passed to partitioner. In the example below, ``timestamp`` and ``run_id`` are the ``runtime_keys`` for ``test_pipeline_partitioner``. These are used to build the ``partition_definition``. In a nutshell, runtime_keys are included as part of the keys comprising the ``partition_definition``; however, ``partition_definition`` can contain more key-value pairs than only those provided by the user as runtime keys.
-  The runtime keys are controlled in the in the configuration.  This will create a control plane for governance-minded engineers who want to enforce some level of consistency among other GE users.
