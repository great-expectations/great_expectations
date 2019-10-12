.. _data_context_reference:

############################
Data Context Reference
############################

A :ref:`data_context` manages assets for a project.

*************************
Configuration
*************************


The DataContext configuration file (``great_expectations.yml``) provides fine-grained control over several core features available to the
DataContext to assist in managing resources used by Great Expectations. Key configuration areas include specifying
datasources, Data Docs, and Stores used to manage access to resources such as expectation suites,
validation results, profiling results, evaluation parameters and plugins.

This file is intended to be committed to your repo.

Datasources
=============


Datasources tell Great Expectations where your data lives and how to get it.

Using the CLI command `great_expectations add-datasource` is the easiest way to
add a new datasource.

The `datasources` section declares which :ref:`datasource` objects should be available in the DataContext.
Each datasource definition should include the `class_name` of the datasource, generators, and any other relevant
configuration information. For example, the following simple configuration supports a Pandas-based pipeline:

.. code-block:: yaml

  datasources:
    pipeline:
      type: pandas
      generators:
        default:
          type: memory

The following configuration demonstrates a more complicated configuration for reading assets from s3 into pandas. It
will access the amazon public NYC taxi data and provides access to two assets: 'taxi-green' and 'taxi-fhv' which
represent two public datasets available from the resource.

.. code-block:: yaml

  datasources:
    nyc_taxi:
      type: pandas
      generators:
        s3:
          type: s3
          bucket: nyc-tlc
          delimiter: '/'
          reader_options:
            sep: ','
            engine: python
          assets:
            taxi-green:
              prefix: trip data/
              regex_filter: 'trip data/green.*\.csv'
            taxi-fhv:
              prefix: trip data/
              regex_filter: 'trip data/fhv.*\.csv'
      data_asset_type:
        class_name: PandasDataset

Here is an example for a SQL based pipeline:

.. code-block:: yaml

    datasources:
      edw:
        module_name: great_expectations.datasource
        class_name: SqlAlchemyDatasource
        credentials: ${data_warehouse}
        data_asset_type:
          class_name: SqlAlchemyDataset
        generators:
          default:
            class_name: TableGenerator

Note the ``credentials`` key references a corresponding key in the
``config_variables.yml`` file which is not in source control that would look
like this:

.. code-block:: yaml

    data_warehouse:
      drivername: postgres
      host: warehouse.ourcompany.biz
      port: '5432'
      username: bob
      password: 1234
      database: prod

Note that the datasources section *includes* all defined generators as well as specifying their names. See
:ref:`custom_expectations_in_datasource` for more information about configuring datasources to use custom expectations.


Data Asset Names
===================

Data asset names consist of three parts, a datasource, generator, and generator asset. DataContext functions will
attempt to "normalize" a data_asset_name if they are provided with only a string, by splitting on the delimiter
character (by default '/') and then attempting to identify an unambiguous name. DataContext searches through
names that already have expectation suites first, then considers names provided by generators.

For example:

.. code-block:: python

    # Returns a normalized name with string representation my_datasource/my_generator/my_asset if
    # my_datasource and my_generator uniquely provide an asset called my_asset
    context.normalize_data_asset_name("my_asset")


Data Documentation
=====================

The :ref:`data_docs` section defines how individual sites should be built and deployed. See the detailed
documentation for more information.


Stores
=============

Stores provide a valuable abstraction for making access to critical resources such as expectation suites, validation
results, profiling data, data documentation, and evaluation parameters both easy to configure and to extend and
customize. See the :ref:`stores_reference` for more information.


.. _environment_and_secrets:

*****************************************
Managing Environment and Secrets
*****************************************

In a DataContext configuration, values that should come from the runtime environment or secrets can be injected via
a separate config file or using environment variables. Use the ``${var}`` syntax in a config file to specify a variable
to be substituted.

Config Variables File
========================

DataContext accepts a parameter called ``config_variables_file_path`` which can
include a file path from which variables to substitute should be read. The file
needs to define top-level keys which are available to substitute into a
DataContext configuration file. Keys from the config variables file can be
defined to represent complex types such as a dictionary or list, which is often
useful for configuring database access.

Variable substitution enables: 1) keeping secrets out of source control & 2)
environment-based configuration changes such as staging vs prod.

When GE encounters substitution syntax (like ``my_key: ${my_value}`` or
``my_key: $my_value``) in the config file it will attempt to replace the value
of ``my_key`` with the value from an environment variable ``my_value`` or a
corresponding key read from the file specified using ``config_variables_file_path``.



.. code-block:: yaml

  prod_credentials:
    type: postgresql
    host: secure_server
    port: 5432
    username: username
    password: sensitive_password
    database: ge

  dev_credentials:
    type: postgresql
    host: localhost
    port: 5432
    username: dev
    password: dev
    database: ge

If the substitution value comes from the config variables file, it can be a
simple (non-nested) value or a nested value such as a dictionary. If it comes
from an environment variable, it must be a simple value.

Environment Variable Substitution
====================================

Environment variables will be substituted into a DataContext config with higher priority than values from the
config variables file.

****************************************************
Default Out of Box Config File
****************************************************

Should you need a clean config file you can run ``great_expectation init`` in a
new directory or use this template:

.. code-block:: yaml

    # Welcome to Great Expectations! Always know what to expect from your data.
    #
    # Here you can define datasources, generators, integrations and more. This file
    # is intended to be committed to your repo. For help with configuration please:
    #   - Read our docs: https://docs.greatexpectations.io/en/latest/reference/data_context_reference.html#configuration
    #   - Join our slack channel: http://greatexpectations.io/slack
    #
    # NOTE: GE uses the names of configured `datasources` and `generators` to manage
    # how `expectations` and other artifacts are stored in the `expectations/` and
    # `datasources/` folders. If you need to rename an existing `datasource` or
    # `generator`, be sure to also update the relevant directory names.

    config_version: 1

    # Datasources tell Great Expectations where your data lives and how to get it.
    # You can use the CLI command `great_expectations add-datasource` to help you
    # add a new datasource. Read more at https://docs.greatexpectations.io/en/latest/features/datasource.html
    datasources: {}
      edw:
        module_name: great_expectations.datasource
        class_name: SqlAlchemyDatasource
        credentials: ${edw}
        data_asset_type:
          class_name: SqlAlchemyDataset
        generators:
          default:
            class_name: TableGenerator

    # This config file supports variable substitution which enables: 1) keeping
    # secrets out of source control & 2) environment-based configuration changes
    # such as staging vs prod.
    #
    # When GE encounters substitution syntax (like `my_key: ${my_value}` or
    # `my_key: $my_value`) in the config file it will attempt to replace the value
    # of `my_key` with the value from an environment variable `my_value` or a
    # corresponding key read from the file specified using
    # `config_variables_file_path`. Environment variables take precedence.
    #
    # If the substitution value comes from the config variables file, it can be a
    # simple (non-nested) value or a nested value such as a dictionary. If it comes
    # from an environment variable, it must be a simple value. Read more at:
    # https://docs.greatexpectations.io/en/latest/reference/data_context_reference.html#managing-environment-and-secrets
    config_variables_file_path: uncommitted/config_variables.yml

    # The plugins_directory will be added to your python path for custom modules
    # used to override and extend Great Expectations.
    plugins_directory: plugins/

    # Validation Operators are customizable workflows that bundle the validation of
    # one or more expectation suites and subsequent actions. The example below
    # stores validations and send a slack notification. To read more about
    # customizing and extending these, read: https://docs.greatexpectations.io/en/latest/features/validation_operators_and_actions.html
    validation_operators:
      action_list_operator:
        # To learn how to configure sending Slack notifications during evaluation
        # (and other customizations), read: https://docs.greatexpectations.io/en/latest/reference/validation_operators/perform_action_list_validation_operator.html
        class_name: ActionListValidationOperator
        action_list:
          - name: store_validation_result
            action:
              class_name: StoreAction
          - name: store_evaluation_params
            action:
              class_name: ExtractAndStoreEvaluationParamsAction
          - name: update_data_docs
            action:
              class_name: UpdateDataDocsAction
          - name: send_slack_notification_on_validation_result
            action:
              class_name: SlackNotificationAction
              slack_webhook: ${validation_notification_slack_webhook}
              notify_on: all
              renderer:
                module_name: great_expectations.render.renderer.slack_renderer
                class_name: SlackRenderer
    stores:
    # Stores are configurable places to store things like Expectations, Validations
    # Data Docs, and more. These are for advanced users only - most users can simply
    # leave this section alone.
    #
    # Three stores are required: expectations, validations, and
    # evaluation_parameters, and must exist with a valid store entry. Additional
    # stores can be configured for uses such as data_docs, validation_operators, etc.
      expectations_store:
        class_name: ExpectationsStore
        store_backend:
          class_name: FixedLengthTupleFilesystemStoreBackend
          base_directory: expectations/
      validations_store:
        class_name: ValidationsStore
        store_backend:
          class_name: FixedLengthTupleFilesystemStoreBackend
          base_directory: uncommitted/validations/
      evaluation_parameter_store:
        # Evaluation Parameters enable dynamic expectations. Read more here:
        # https://docs.greatexpectations.io/en/latest/reference/evaluation_parameters.html
        class_name: InMemoryEvaluationParameterStore
    expectations_store_name: expectations_store
    validations_store_name: validations_store
    evaluation_parameter_store_name: evaluation_parameter_store

    data_docs_sites:
      # Data Docs make it simple to visualize data quality in your project. These
      # include Expectations, Validations & Profiles. The are built for all
      # Datasources from JSON artifacts in the local repo including validations &
      # profiles from the uncommitted directory. Read more at https://docs.greatexpectations.io/en/latest/features/data_docs.html
      local_site:
        class_name: SiteBuilder
        store_backend:
          class_name: FixedLengthTupleFilesystemStoreBackend
          base_directory: uncommitted/data_docs/local_site/


