.. _reference__core_concepts__datasources:

###################
Datasources
###################

.. _reference__core_concepts__datasources__data_connector:

Data Connectors
===================

A **Data Connector** facilitates access to an external data store, such as a database, filesystem, or cloud storage. The Data Connector can inspect an external data store to *identify available partitions*, *build batch definitions using parameters such as partition names*, and *translate batch definitions to Execution Engine-specific Batch Specs*.

.. admonition:: API Note

  Data Connectors replace Batch Kwargs Generators in the updated Great Expectations API.

There are several types of Data Connectors in Great Expectations, such as the ``ConfiguredAssetFilesystemDataConnector``, ``DatabaseDataConnector``, and ``RuntimeDataConnector``.

Each Data Connector holds configuration for connecting to a different type of external data source, and can connect to and inspect that data source.

- For example, a ``ConfiguredAssetFilesystemDataConnector`` could be configured with the root directory for files on a filesystem or bucket and prefix used to access files from a cloud storage environment.

The simplest ``RuntimeDataConnector`` may simply store lookup information about Data Assets to facilitate running in a pipeline where you already have a DataFrame in memory or available in a cluster. However, Great Expectations makes it possible to configure Data Connectors that offer stronger guarantees about reproducibility, sampling, and compatibility with other tools.

The Data Connector uses ``Partitions`` to identify the available batches available in a Data Asset.

A **Partition** is what differentiates a specific ``Batch`` of data that is part of a Data Asset. The partition uniquely identifies a subset of data based on the purpose for which you validate, such as the most recent delivery. The ``ConfiguredAssetFilesystemDataConnector`` can use a regex strring to match files and prouce named match groups that define unique partitions. Data Connectors use **Sorters** to help define a unique order for partitions, such as sorting files by date or alphabetically.

Batches
=========

The main goal of Data Connectors is to provide useful guarantees about *Batches*, for example ensuring that they cover data from non-overlapping date ranges. A **Batch** is a combination of data and metadata.

    .. image:: /images/batch_what_is_a_batch.png

The ``Datasource`` is responsible for orchestrating the building of a Batch, using the following components:

.. code-block:: python

    new_batch = Batch(
        data = batch_data,
        batch_request=batch_request
        batch_definition=batch_definition,
        batch_spec=batch_spec,
        batch_markers=batch_markers,
    )

Typically, a user need specify only the **BatchRequest**. The BatchRequest is a description of what data Great Expectations should fetch, including the name of the Data Asset and other identifiers (see more detail below).

A **Batch Definition** includes all the information required to precisely identify a set of data from the external data source that should be translated into a Batch. One or more BatchDefinitions are always *returned* from the Datasource, as a result of processing the BatchRequest. A BatchDefinition includes several key components:

- **Partition Definition**: contains information that uniquely identifies a specific partition from the Data Asset, such as the delivery date or query time.
- **Engine Passthrough**: contains information that will be passed directly to the Execution Engine as part of the Batch Spec.
- **Sample Definition**: contains information about sampling or limiting done on the partition to create a Batch.

.. admonition:: Best Practice

   We recommend that you make every Data Asset Name **unique** in your Data Context configuration. Even though a Batch Definition includes the Data Connector Name and Execution Environment Name, choosing a unique data asset name makes it easier to navigate quickly through Data Docs and ensures your logical Data Assets are not confused with any particular view of them provided by an execution engine.

A **Batch Spec** is an Execution Engine-specific description of the Batch. The Data Connector is responsible for working with the Execution Engine to translate the Batch Definition into a spec that enables Great Expectations to access the data using that Execution Engine.

Finally, the **BatchMarkers** are additional pieces of metadata that can be useful to understand reproducibility, such as the time the batch was constructed, or hash of an in-memory DataFrame.

Batch examples
-----------------

Let's explore Batches with some examples:

Given the following BatchDefinition:

.. code-block:: json

    {
      "execution_environment_name": "cloud_spark",
      "data_connector_name": "s3",
      "data_asset_name": "nightly_logs",
      "partition_definition": {
        "year": 2020,
        "month": 9,
        "day": 15
      }
    }

The ``ConfiguredAssetFilesystemDataConnector`` might work with a configured SparkDFExecuionEngine to translate that to the following Batch Spec:

.. code-block:: json

    {
      "paths": [
        "s3a://logs.priv/nightly/2020/09/15/f23b4301-dcfa-4a1d-b054-23659b55c4f2.csv",
        "s3a://logs.priv/nightly/2020/09/15/212becf1-45d4-4cce-a0fb-52d5b7883a89.csv"
      ]
      "reader_method": "read_csv",
      "reader_options": {
        "sep": "\t"
      }
    }



[[Diagram: External Datasource -> Partition Definition -> Batch Definition -> Batch Spec -> Execution  Engine -> External Datasource ]]

The Datasource can then query the ExecutionEngine to fetch data and BatchMarkers, producing a ``new_batch`` that may look something like this:

.. code-block:: python

    Batch(
        data,
        batch_request: {
            "datasource": "myds"
            "data_connector": "pipeline",
            "data_asset_name": "my_asset",
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
            "data_asset_name": "my_asset",
            "partition_definition": {
              "airflow_run_id": "string_airflow_run_id_that_was_provided",
              "other_key": "string_other_key_that_was_provided",
          }
        }
    batch_spec: {
            in_memory_df: True
    },
    batch_markers: {
        "pandas_hash": "_______"
        }
    )


A full journey
--------------------------

Let's follow the outline in this diagram to follow the journey from ``BatchRequest`` to ``BatchDefinition`` to ``BatchSpec`` to ``Batch``:

    .. image:: /images/batch_life_of_a_batch.png

1. ``BatchRequest``

- The ``BatchRequest`` is the object a user passes to the ``DataSource`` to request a ``Batch`` (or ``Batches``).
    - It can include ``partition_request`` params with values relative to the latest batch (e.g. the "latest" slice). Conceptually, this enables "fetch the latest `Batch`" behavior. It is the key thing that differentiates a `BatchRequest`, which does NOT necessarily uniquely identify the `Batch(es)` to be fetched, from a BatchDefinition.
    - The BatchRequest can also include a section called ``batch_spec_passthrough`` to make it easy to directly communicate parameters to a specific ExecutionEngine.
    - When resolved, the `BatchRequest` may point to many `BatchDefinitions` and Batches.

- A BatchRequest is the entrypoint to getting a ``Batch`` from a Datasource, using the ``get_batch_list_from_batch_request()`` method:

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


2. ``BatchDefinition``

-  A ``BatchDefinition`` resolves any ambiguity in ``BatchRequest`` to uniquely identify a single ``Batch`` to be fetched.  ``BatchDefinition`` is Datasource- and ExecutionEngine-agnostic. That means that its parameters may depend on the configuration of the ``Datasource``, but they do not otherwise depend on the specific DataConnector type (e.g. filesystem, SQL, etc.) or ExecutionEngine being used to instantiate ``Batches``.

.. code-block:: yaml

    BatchDefinition
        datasource: str
        data_connector: str
        data_asset_name: str
        partition_definition:
            ** contents depend on the configuration of the DataConnector **
            ** provides a persistent, unique identifier for the partition within the context of the data asset **

3. ``BatchSpec``

- A ``BatchSpec`` is a set of specific instructions for the ``ExecutionEngine`` to fetch specific data; it is the ExecutionEngine-specific version of the BatchDefinition. For example, a ``BatchSpec`` could include the path to files, information about headers, or other configuration required to ensure the data is loaded properly for validation.

4. During initilization of the Batch, ``BatchMarkers``, calculated by the ``ExecutionEngine``, are also added. They are metadata that can be used to calculate performance characteristics, ensure reproducibility of validation results, and provide indicators of the state of the underlying data system.


************************************************************
RuntimeDataConnector 
************************************************************

A ``RuntimeDataConnector`` is a special kind of DataConnector that supports easy integration with Pipeline Runners where the data is already available as a reference that needs only a lightweight wrapper to track validations.

In a BatchDefinition produced by a RuntimeDataConnector, the ``partition_definition`` section is replaced with ``runtime_keys``. ``runtime_keys`` perform the same function as ``partition_definition``: a persistent, unique identifier for the partition of data included in the ``Batch``. By relying on user-provided keys, we allow the definition of the specific partition's identifiers to happen at runtime, for example using a run_id from an Airflow DAG run.
-  The specific runtime **keys** to be expected are controlled in the in the DataConnector configuration. Using that configuration creates a control plane for governance-minded engineers who want to enforce some level of consistency between validations.
