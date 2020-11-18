.. _reference__core_concepts__data_access:

###################
Data Access
###################

A **Data Connector** facilitates access to an external data store, such as a database, filesystem, or cloud storage. The Data Connector can inspect an external data store to *identify available partitions*, *build batch definitions using parameters such as partition names*, and *translate batch definitions to Execution Engine-specific Batch Specs*.

.. admonition:: API Note

  Data Connectors replace Batch Kwargs Generators in the updated Great Expectations API.

There are three primary types of Data Connectors in Great Expectations: the ``ConfiguredAssetFilesystemDataConnector``, ``DatabaseDataConnector``, and ``RuntimeDataConnector``.

Each Data Connector holds configuration for connecting to a different type of external data source, and can connect to and inspect that data source.

- For example, a ``ConfiguredAssetFilesystemDataConnector`` could be configured with the root directory for files on a filesystem or bucket and prefix used to access files from a cloud storage environment.

The simplest ``RuntimeDataConnector`` may simply store lookup information about Data Assets. However, Great Expectations makes it possible to configure Data Connectors that offer stronger guarantees about reproducibility, sampling, and compatibility with other tools.

The Data Connector uses Partitions to identify the available batches available in a Data Asset.

A **Partitioner** is a utility configured in a Data Connector that can identify available partitions. For example the ``RegexPartitioner`` can match files by regex pattern and make named match groups available in the partition definitions.

An **Orderer** is a utility configured in a partitioner to help define a unique order for partitions, such as sorting files by date or alphabetically.

A **Batch Definition** includes information required to precisely identify a set of data from the external data source that should be translated into a Batch. Specifically, a Batch Definition includes the Data Asset name, Data Connector name, and Execution Environment name, as well as other information including the Partition Definition.

- **Partition Definition**: contains information that uniquely identifies a specific partition from the Data Asset, such as the delivery date or query time.
- **Engine Passthrough**: contains information that will be passed directly to the Execution Engine as part of the Batch Spec.
- **Sample Definition**: contains information about sampling or limiting done on the partition to create a Batch.

.. admonition:: Best Practice

   We recommend that you make every Data Asset Name **unique** in your Data Context configuration. Even though a Batch Definition includes the Data Connector Name and Execution Environment Name, choosing a unique data asset name makes it easier to navigate quickly through Data Docs and ensures your logical Data Assets are not confused with any particular view of them provided by an execution engine.

A **Batch Spec** is an Execution Engine-specific description of the Batch. The Data Connector is responsible for working with the Execution Engine to translate the batch definition into a spec that enables Great Expectations to access the data using that Execution Engine.

.. admonition:: Note:

    The Batch Spec is not meant to be directly modified by Great Expectations users; instead, we recommend that you use the Batch Definition and Data Connector Configuration.

Consider the following example:

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

Working with a configured spark Execution Engine, the ``ConfiguredAssetFilesystemDataConnector`` might translate that to the following Batch Spec:

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

Note: Great Expectations may also use *mini-batches* to divide computation of a single Batch of data for efficiency.
