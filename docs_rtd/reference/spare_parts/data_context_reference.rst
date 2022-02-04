.. _data_context_reference:

.. warning:: This doc is spare parts: leftover pieces of old documentation.
  It's potentially helpful, but may be incomplete, incorrect, or confusing.


############################
Data Context Reference
############################

A :ref:`data_context` manages assets for a project.

*************************
Configuration
*************************


The DataContext configuration file (``great_expectations.yml``) provides fine-grained control over several core
features available to the DataContext to assist in managing resources used by Great Expectations. Key
configuration areas include specifying Datasources, Data Docs, Validation Operators, and Stores used to manage access
to resources such as expectation suites, validation results, profiling results, evaluation parameters and plugins.

This file is intended to be committed to your repo.

Datasources
=============

Datasources tell Great Expectations where your data lives and how to get it.

Using the :ref:`CLI <command_line>` command ``great_expectations datasource new`` is the easiest way to
add a new datasource.

The `datasources` section declares which :ref:`datasource <reference__core_concepts__datasources>` objects should be available in the DataContext.
Each datasource definition should include the `class_name` of the datasource, generators, and any other relevant
configuration information. For example, the following simple configuration supports a Pandas-based pipeline:

.. code-block:: yaml

  datasources:
    pipeline:
      class_name: PandasDatasource
      generators:
        default:
          class_name: InMemoryGenerator

The following configuration demonstrates a more complicated configuration for reading assets from S3 into pandas. It
will access the amazon public NYC taxi data and provides access to two assets: 'taxi-green' and 'taxi-fhv' which
represent two public datasets available from the resource.

.. code-block:: yaml

  datasources:
    nyc_taxi:
      class_name: PandasDatasource
      generators:
        s3:
          class_name: S3GlobReaderBatchKwargsGenerator
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
        class_name: SqlAlchemyDatasource
        credentials: ${data_warehouse}
        data_asset_type:
          class_name: SqlAlchemyDataset
        generators:
          default:
            class_name: TableBatchKwargsGenerator

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
------------------

Data asset names consist of three parts, a datasource, generator, and generator asset. DataContext functions will
attempt to "normalize" a data_asset_name if they are provided with only a string, by splitting on the delimiter
character (by default '/') and then attempting to identify an unambiguous name. DataContext searches through
names that already have expectation suites first, then considers names provided by generators.

For example:

.. code-block:: python

    # Returns a normalized name with string representation my_datasource/my_generator/my_asset if
    # my_datasource and my_generator uniquely provide an asset called my_asset
    context.normalize_data_asset_name("my_asset")


Data Docs
=====================

The :ref:`data_docs` section defines how individual sites should be built and deployed. See the detailed
documentation for more information.


Stores
=============

A DataContext requires three :ref:`stores <reference__core_concepts__data_context__stores>` to function properly: an `expectations_store`,
`validations_store`, and `evaluation_parameter_store`. Consequently a minimal store configuration for a DataContext
would include the following:

.. code-block:: yaml

    expectations_store_name: expectations_store
    validations_store_name: validations_store
    evaluation_parameter_store_name: evaluation_parameter_store

    stores:
      expectations_store:
        class_name: ExpectationsStore
        store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: expectations/
      validations_store:
        class_name: ValidationsStore
        store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: uncommitted/validations/
      evaluation_parameter_store:
        class_name: EvaluationParameterStore

The `expectations_store` provides access to expectations_suite objects, using the DataContext's namespace; the
`validations_store` does the same for validations. See :ref:`evaluation_parameters` for more information on the
evaluation parameters store.

Stores can be referenced in other objects in the DataContext. They provide a common API for accessing data
independently of the backend where it is stored. For example, on a team that uses S3 to store expectation suites and
validation results, updating the configuration to use cloud storage requires only changing the store class_name and
providing the bucket/prefix combination:

.. code-block:: yaml

    expectations_store_name: expectations_store
    validations_store_name: validations_store
    evaluation_parameter_store_name: evaluation_parameter_store

    stores:
      expectations_store:
        class_name: ExpectationsStore
        store_backend:
          class_name: TupleS3StoreBackend
          base_directory: expectations/
          bucket: ge.my_org.com
          prefix:
      validations_store:
        class_name: ValidationsStore
        store_backend:
          class_name: TupleS3StoreBackend
          bucket: ge.my_org.com
          prefix: common_validations
      evaluation_parameter_store:
        class_name: EvaluationParameterStore

GE uses `boto3 <https://boto3.amazonaws.com/v1/documentation/api/latest/index.html>`_ to access AWS, so credentials
simply need to be available in any standard place searched by that library. You may also specify keyword arguments
for boto3 to use in the `boto3_options key` of the store_backend configuration.


Validation Operators
=====================

See the :ref:reference__core_concepts__validation__validation_operator for more information regarding configuring and using validation operators.

.. _environment_and_secrets:

*****************************************
Managing Environment and Secrets
*****************************************

Values can be injected in a DataContext configuration file by using the
``${var}`` syntax to specify a variable to be substituted.

The injected value can come from three sources:

1. A config variables file
2. Environment variables
3. A dictionary passed to the DataContext constructor.

Each source above will override variables set in a previous source.

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
``my_key: $my_value``) in the great_expectations.yml config file it will attempt to replace the value
of ``my_key`` with the value from an environment variable ``my_value`` or a
corresponding key read from the file specified using ``config_variables_file_path``, which is located in uncommitted/config_variables.yml by default. This is an example of a config_variables.yml file:


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

**Note**: Substitution of environment variables is currently only supported in the great_expectations.yml, but not in a config variables file. See [this Discuss post](https://discuss.greatexpectations.io/t/environment-variable-substitution-is-not-working-for-me-when-connecting-ge-to-my-database/72) for an example.


Passing Values To DataContext
===============================

A dictionary of values can be passed to a DataContext when it is instantiated.
These values will override both values from the config variables file and
from environment variables.

.. code-block:: python

  data_context = DataContext.create(runtime_environment={
      'name_to_replace': 'replacement_value'
    })


****************************************************
Default Out of Box Config File
****************************************************

Should you need a clean config file you can run ``great_expectation init`` in a
new directory or use this template:

.. code-block:: yaml

    # Welcome to Great Expectations! Always know what to expect from your data.
    #
    # Here you can define datasources, batch kwargs generators, integrations and
    # more. This file is intended to be committed to your repo. For help with
    # configuration please:
    #   - Read our docs: https://docs.greatexpectations.io/en/latest/reference/data_context_reference.html#configuration
    #   - Join our slack channel: http://greatexpectations.io/slack

    config_version: 2

    # Datasources tell Great Expectations where your data lives and how to get it.
    # You can use the CLI command `great_expectations datasource new` to help you
    # add a new datasource. Read more at https://docs.greatexpectations.io/en/latest/features/datasource.html
    datasources: {}
      edw:
        class_name: SqlAlchemyDatasource
        credentials: ${edw}
        data_asset_type:
          class_name: SqlAlchemyDataset
        generators:
          default:
            class_name: TableBatchKwargsGenerator

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
              class_name: StoreValidationResultAction
          - name: store_evaluation_params
            action:
              class_name: StoreEvaluationParametersAction
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
          class_name: TupleFilesystemStoreBackend
          base_directory: expectations/
      validations_store:
        class_name: ValidationsStore
        store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: uncommitted/validations/
      evaluation_parameter_store:
        # Evaluation Parameters enable dynamic expectations. Read more here:
        # https://docs.greatexpectations.io/en/latest/reference/evaluation_parameters.html
        class_name: EvaluationParameterStore
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
          class_name: TupleFilesystemStoreBackend
          base_directory: uncommitted/data_docs/local_site/

.. _Usage Statistics:

#################
Usage Statistics
#################

To help us improve the tool, by default we track event data when certain Data Context-enabled commands are run. Our `blog post from April 2020 <https://greatexpectations.io/blog/anonymous-usage-statistics/>`_ explains a little bit more about what we want to capture with usage statistics and why! The usage statistics include things like the OS and python version, and which GE features are used. You can see the exact
schemas for all of our messages `here <https://github.com/great-expectations/great_expectations/blob/develop/great_expectations/core/usage_statistics/schemas.py>`_.

While we hope you'll leave them on, you can easily disable usage statistics for a Data Context by adding the
following to your data context configuration:

.. code-block:: yaml

    anonymous_usage_statistics:
      data_context_id: <randomly-generated-uuid>
      enabled: false

You can also disable usage statistics system-wide by setting the ``GE_USAGE_STATS`` environment variable to
``FALSE`` or adding the following code block to a file called ``great_expectations.conf`` located in ``/etc/`` or
``~/.great_expectations``:

.. code-block::

    [anonymous_usage_statistics]
    enabled=FALSE

As always, please reach out `on Slack <https://greatexpectations.io/slack>`__ if you have any questions or comments.
