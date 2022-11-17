.. _data_context:


#############
Data Contexts
#############

A Data Context represents a Great Expectations project. It organizes storage and access for Expectation Suites, Datasources, notification settings, and data fixtures.

The Data Context is configured via a yml file or directly in code. The configuration and managed expectation suites should be stored in version control.

Data Contexts manage connections to your data and compute resources, and support integration with execution frameworks (such as airflow, Nifi, dbt, or dagster) to describe and produce batches of data ready for analysis. Those features enable fetching, validation, profiling, and documentation of your data in a way that is meaningful within your existing infrastructure and work environment.

Data Contexts also manage Expectation Suites. Expectation Suites combine multiple Expectation Configurations into an overall description of a dataset. Expectation Suites should have names corresponding to the kind of data they define, like “NPI” for National Provider Identifier data or “company.users” for a users table.

The Data Context also provides other services, such as storing and substituting evaluation parameters during validation. See :ref:`data_context_evaluation_parameter_store` for more information.


*****************************************************************************************************
Interactively Testing Configurations
*****************************************************************************************************

Especially during the beginning of a Great Expecations project, it is often incredibly useful to rapidly iterate over configurations of key Data Context components. The ``test_yaml_config`` feature makes that easy.

``test_yaml_config`` is a convenience method for configuring the moving parts of a Great Expectations deployment. It allows you to quickly test out configs for Datasources, Checkpoints, and each type of Store (ExpectationStores, ValidationResultStores, and MetricsStores). For many deployments of Great Expectations, these components (plus Expectations) are the only ones you'll need.

Here's a typical example:

.. code-block:: python

    config = """
    class_name: Datasource
    execution_engine:
        class_name: PandasExecutionEngine
    data_connectors:
        my_data_connector:
            class_name: InferredAssetFilesystemDataConnector
            base_directory: {data_base_directory}
            glob_directive: "*/*.csv"
            default_regex:
                pattern: (.+)/(.+)\\.csv
                group_names:
                    - data_asset_name
                    - partition

    """
    my_context.test_yaml_config(
        config=config
    )
    > Returns some helpful output here

The helpful output can include any result from the "self check" of an artifact produced using that configuration.


.. _reference__core_concepts__data_context__evaluation_parameter_stores:

Evaluation Parameter Stores
======================================

An **Evaluation Parameter Store** is a kind of Metric Store that makes it possible to build expectation suites that depend on values from other batches of data, such as ensuring that the number of rows in a downstream dataset equals the number of unique values from an upstream one. A Data Context can manage a store to facilitate that validation scenario.
