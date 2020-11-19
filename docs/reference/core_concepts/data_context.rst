.. _data_context:


#############
Data Contexts
#############

A Data Context represents a Great Expectations project. It organizes storage and access for
Expectation Suites, Datasources, notification settings, and data fixtures.

The Data Context is configured via a yml file stored in a directory called great_expectations; the configuration file
as well as managed expectation suites should be stored in version control.

Data Contexts manage connections to your data and compute resources, and support integration with execution
frameworks (such as airflow, Nifi, dbt, or dagster) to describe and produce batches of data ready for analysis. Those
features enable fetching, validation, profiling, and documentation of your data in a way that is meaningful within your
existing infrastructure and work environment.

Data Contexts also manage Expectation Suites. Expectation Suites combine multiple Expectation Configurations into an
overall description of a dataset. Expectation Suites should have names corresponding to the kind of data they
define, like “NPI” for National Provider Identifier data or “company.users” for a users table.

The Data Context also provides other services, such as storing and substituting evaluation parameters during validation.
See :ref:`data_context_evaluation_parameter_store` for more information.

See the :ref:`reference__core_concepts__data_contexts` section for more information.



#################################
Data Contexts : Additional pieces
#################################


******************************************
Key Method : ``add_execution_environment``
******************************************

- Add a description of methods that interact with ``ExecutionEnvironment`` and further
- (**Note** Will be renamed to DataSource before 0.13 release)

    .. code-block:: python

        def add_execution_environment(self, execution_environment_name, execution_environment_config):


*****************************************************************************************************
``TestYAML_Config``  (source : 20200928_test_yaml_config document in github.com/superconductive/design)
*****************************************************************************************************

  Goals of TestYAML_Config
  * Speed up onboarding for GE users, especially in the case of deploying GE against materialized data.
  * Signal thoughtfulness in design to users by providing tooling and documentatio to fully support onboarding.
  * Foreshadow and gain experience with workflows for SaaS.
  * Enable much more granular usage stats during configuration sessions, so that we can remove friction from the experience.

  ``test_yaml_config`` is a convenience method for configuring the moving parts of a Great Expectations deployment. It allows you to quickly test out configs for Datasources, Checkpoints, and each type of Store (ExpectationStores, ValidationResultStores, and MetricsStores). For many deployments of Great Expectations, these components (plus Expectations) are the only ones you'll need.

  Here's a typical example:

    .. code-block:: python

        my_config = """
        some:
            config:
                - in
                - yaml

        """
        my_context.text_yaml_config(
        config_type="datasource",
        config=my_config
        )
        > Returns some helpful output here


Amendments after quick design review on 2020/10/8:
  Expected implementation path:

  1. A ``DataContext.test_yaml_config`` method that accepts configs and instantiates using exactly the same methods (and default parameters, etc.) as the DataContext.
  2. Within any given instantiatable class, a ``self_check`` method that can be invoked from ``DataContext.test_yaml_config`` or from a "parent" instantiated class (e.g. Stores are usually parents of StoreBackends)

###################
Questions/Pushback:
###################

  Q: How do we handle latency and repeated calls to (say) S3?
  A: This is exactly why caching is so important. We should be able to cache an underlying inventory of data objects (files, S3 keys, tables, etc.) locally, and the test locig against them.

  Q: Do we have to instantiate full objects (e.g. ExecutionEnvironment( ExecutionEngine, DataConnecters(Partitioners(...))) ), or can we zero in on just the relevant pieces (e.g. a single Partitioner)?
  A: The current design does not allow for zeroing in, but there's probably a way to do this. One issue is how the user the user would mock fixtures (e.g. a list of example files), but maybe we can resolve that by using a cache from a parent object or something...

###########################################################################################################
There are 2 additional variations to `DataContext` that might be relevant (taken directly from DocStrings)
###########################################################################################################

- ``ExplorerDataContext``
    - It contains ``expectation_explorer``

- ``DataContextV3`` (to be renamed)
    - contains ``get_config()`` and ``test_yaml_config()``

    .. code-block:: python

            """

            test_yaml_config()  DOCSTRING

            Convenience method for testing yaml configs
            test_yaml_config is a convenience method for configuring the moving
            parts of a Great Expectations deployment. It allows you to quickly
            test out configs for system components, especially Datasources,
            Checkpoints, and Stores.

            For many deployments of Great Expectations, these components (plus
            Expectations) are the only ones you'll need.

            test_yaml_config is mainly intended for use within notebooks and tests.

            Parameters
            ----------
            yaml_config : str
                A string containing the yaml config to be tested

            name: str
                (Optional) A string containing the name of the component to instantiate

            pretty_print : bool
                Determines whether to print human-readable output

            return_mode : str
                Determines what type of object test_yaml_config will return
                Valid modes are "instantiated_class" and "report_object"

            shorten_tracebacks : bool
                If true, catch any errors during instantiation and print only the
                last element of the traceback stack. This can be helpful for
                rapid iteration on configs in a notebook, because it can remove
                the need to scroll up and down a lot.

            Returns
            -------
            The instantiated component (e.g. a Datasource)
            OR
            a json object containing metadata from the component's self_check method

            The returned object is determined by return_mode.
            """

########################
Proposed work to be done
########################

1. Create a ``DataContext.test_yaml_config`` method, with submethods for the three major non-Expectations components of Great Expectations. (Expectations are special and different):

    * Datasources
    * Checkpoints
    * Stores (ExpectationStores, ValidationResultStores, and MetricsStores)

2. Create and test notebooks that use this method to configure each of these types of objects.

3. Add ``usage_stats`` logging to ``test_yaml_config``, exact data format TBD. The goal is to be able to observe configuration sessions without exposing any private info. Ideally, we'll be able to tell from the logs whether a given session was successful.

Assuming success after thorough testing, we'll probably want to make some corresponding changes to the CLI and docs. Those can be handled separately.

###################################################
Amendments after quick design review on 2020/10/8:
###################################################

Expected implementation path:

    1. A ``DataContext.test_yaml_config`` method that accepts configs and instantiates using exactly the same methods (and default parameters, etc.) as the DataContext.
    2. Within any given instantiatable class, a ``self_check`` method that can be invoked from ``DataContext.test_yaml_config`` or from a "parent" instantiated class (e.g. Stores are usually parents of StoreBackends)

    *******************
    Questions/Pushback:
    *******************

    Q: How do we handle latency and repeated calls to (say) S3?
    A: This is exactly why caching is so important. We should be able to cache an underlying inventory of data objects (files, S3 keys, tables, etc.) locally, and the test locig against them.

    Q: Do we have to instantiate full objects (e.g. ExecutionEnvironment( ExecutionEngine, DataConnecters(Partitioners(...))) ), or can we zero in on just the relevant pieces (e.g. a single Partitioner)?
    A: The current design does not allow for zeroing in, but there's probably a way to do this. One issue is how the user the user would mock fixtures (e.g. a list of example files), but maybe we can resolve that by using a cache from a parent object or something...

    3. ``get_batch()``

        .. code-block:: python
            """
            Get exactly one batch, based on a variety of flexible input types.
                Args:
                    batch_definition
                    batch_request

                    execution_environment_name
                    data_connector_name
                    data_asset_name
                    partition_request

                    partition_identifiers

                    limit
                    index
                    custom_filter_function
                    sampling_method
                    sampling_kwargs

                    **kwargs

                Returns:
                    (Batch) The requested batch

            """

    ``get_batch`` is the main user-facing API for getting batches.
             In contrast to virtually all other methods in the class, it does not require typed or nested inputs.
             Instead, this method is intended to help the user pick the right parameters

             This method attempts returns exactly one batch.
             If 0 or more than batches would be returned, it raises an error.

*Questions/Pushback:*

Q: How do we handle latency and repeated calls to (say) S3?
A: This is exactly why caching is so important. We should be able to cache an underlying inventory of data objects (files, S3 keys, tables, etc.) locally, and the test locig against them.

Q: Do we have to instantiate full objects (e.g. ExecutionEnvironment( ExecutionEngine, DataConnecters(Partitioners(...))) ), or can we zero in on just the relevant pieces (e.g. a single Partitioner)?
A: The current design does not allow for zeroing in, but there's probably a way to do this. One issue is how the user the user would mock fixtures (e.g. a list of example files), but maybe we can resolve that by using a cache from a parent object or something...
