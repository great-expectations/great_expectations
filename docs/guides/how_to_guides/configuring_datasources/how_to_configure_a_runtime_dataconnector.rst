.. _how_to_guides_how_to_configure_a_runtime_dataconnector:

How to configure a RuntimeDataConnector
==============================================

This guide demonstrates how to configure an RuntimeDataConnector...

TODO: insert text that briefly explains why and in what situations you should use a RuntimeDataConnector


If you're not sure which one to use, please check out :ref:`How to choose which DataConnector to use. <which_data_connector_to_use>`


.. admonition:: Prerequisites: This how-to guide assumes you have already:

    - :ref:`Understand the basics of Datasources in 0.13 or later <reference__core_concepts__datasources>`
    - Learned how to configure a :ref:`DataContext using test_yaml_config <how_to_guides_how_to_configure_datacontext_components_using_test_yaml_config>`
    - :ref:`Configured a Datasource <how_to_guides__configuring_datasources>`


Steps
-----


#. **Add the DataConnector configuration to the configuration of the Datasource.**

    TODO: insert text here

    .. code-block:: yaml

        datasources:
          my_spark_datasource:
            class_name: Datasource
            execution_engine:
              class_name: SparkDFExecutionEngine
            data_connectors:


    TODO: insert text here

    .. code-block:: yaml

        my_runtime_data_connector:
          module_name: great_expectations.datasource.data_connector
          class_name: RuntimeDataConnector
          runtime_keys:
              - some_key_maybe_pipeline_stage
              - some_other_key_maybe_run_id


    TODO: insert text here

    .. code-block:: yaml

        datasources:
          my_spark_datasource:
            class_name: Datasource
            execution_engine:
              class_name: SparkDFExecutionEngine
            data_connectors:
              my_runtime_data_connector:
                  module_name: great_expectations.datasource.data_connector
                  class_name: RuntimeDataConnector



#. **Optional: Configure runtime_keys for your new DataConnector.**

    TODO: insert text here

    .. code-block:: yaml

        my_runtime_data_connector:
          module_name: great_expectations.datasource.data_connector
          class_name: RuntimeDataConnector
          runtime_keys:
              - some_key_maybe_pipeline_stage
              - some_other_key_maybe_run_id



Additional notes
----------------


Additional resources
--------------------


Comments
--------

.. discourse::
   :topic_identifier: 99999