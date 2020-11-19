.. _execution_environment:


######################
ExecutionEnvironment
######################

************************************************
THERE IS NO FORMAL EXISTING DOCUMENTATION ON THIS
************************************************

- The following was taken from the (20201009_data_connector.md documentation in superconductive/design repo)
- An ExecutionEnvironment is the glue between an ExecutionEngine and a DataConnector.
- **NOTE** : ExecutionEnvironment was updated 10-09-20 to handle ``runtime_keys``, which can include ``timestamp``, and ``run_id``

    .. image:: /images/data_source_high_level_diagram.png
