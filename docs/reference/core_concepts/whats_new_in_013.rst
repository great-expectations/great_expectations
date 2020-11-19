.. _whats_new_in_013:

######################################
What's new in Great Expectations 0.13
######################################

*********************************************
Initial List of Documentation that is needed:
*********************************************
- ``DataContext``: (should be updated)
- ``Batch`` : (does not exist)

- ``DataSource`` / ``ExecutionEnvironment``: (does not exist)
  - this is what has a ``DataConnector``
    - this is what contains a ``Sorter``
  - this is what has a ``ExecutionEngine``
  - ``Validation``: (exists, and should be updated)
  - ``Metrics`` : (exists, and should be updated from design documentation)

***********************************************************************************************************
Key Roles different components (source : 20200813_datasource document in github.com/superconductive/design)
***********************************************************************************************************

- Execution Engine:
  - interpret batch_spec and loading batches of data
  - Storing information relevant for connecting to the engine
  - (Also, facilitating computation of metrics, but not in scope of Batch Definition)

- Data Connector:
  - translate batch_definition -> batch_spec
  - inspect external datasources to identify available partition_definitions
  - translate partition_definition to sortable partition_id, and, if possible partition_id -> partition_definition

- Note*: This design envisions several significant name changes, including:
 - ``DataSources`` -> ``Execution Environments``
 - ``Dataset/Datasource Classes`` -> ``Execution Engines``
 - ``Batch Kwargs Generators`` -> ``Data Connectors``
 - ``Batch_parameters`` -> ``batch_definition``
 - ``Batch_kwargs`` -> ``batch_spec``
