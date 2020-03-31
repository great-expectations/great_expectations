.. _data_context_reference:

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

The `datasources` section declares which :ref:`datasource` objects should be available in the DataContext.
Each datasource definition should include the `class_name` of the datasource, generators, and any other relevant
configuration information. For example, the following simple configuration supports a Pandas-based pipeline:

.. code-block:: yaml

  datasources:
    pipeline:
      class_name: PandasDatasource
      generators:
        default:
          class_name: InMemoryGenerator

The following configuration demonstrates a more complicated configuration for reading assets from s3 into pandas. It
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

A DataContext requires three :ref:`stores <stores_reference>` to function properly: an `expectations_store`,
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

See the :ref:`validation_operators` for more information regarding configuring and using validation operators.

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
    # Here you can define datasources, batch kwargsgenerators, integrations and
    # more. This file is intended to be committed to your repo. For help with
    # configuration please:
    #   - Read our docs: https://docs.greatexpectations.io/en/latest/reference/data_context_reference.html#configuration
    #   - Join our slack channel: http://greatexpectations.io/slack

    config_version: 1

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


****************************************************
Usage Statistics
****************************************************


**Why usage statistics?**

We want to build the best version of Great Expectations possible. Prioritizing our development efforts based on real world use will allow us to serve the user community better.

**What's in it for me?**

You will be helping us make Great Expectations better, both for yourself and for the rest of the data practitioner community.

**What data do you collect?**

Events are sent when select DataContext public methods are invoked or CLI commands are run.

These events contain basic platform information (OS + python version).

We take extra precautions to not collect anything that is private and none of our business.
Credentials and the contents of expectations or validation results are never collected.
All user generated names (datasources, expectation suites, etc.) are anonymized through hashing.

We also host images and style sheets on a public CDN and count the number of unique IPs from which resources are fetched. We use CDN fetch rates to get a sense of total community usage of Great Expectations.

**What are you doing with my data?**

We will be tracking the popularity of various features, expectation types and storage/compute backends in order to prioritize our development efforts.

We will be tracking the number of deployments in order to know if our efforts are making Great Expectations more popular and easy to use.

Our privacy policy [link] provides all the legal details.


Great Expectations currently emits usage statistics for the following methods:

* ``data_context.__init__``
* ``data_context.run_validation_operator``
* ``data_context.open_data_docs``
* ``data_context.build_data_docs``
* ``data_asset.validate``
* ``cli.suite.list``
* ``cli.suite.edit``
* ``cli.suite.new``
* ``cli.store.list``
* ``cli.project.check_config``
* ``cli.validation_operator.run``
* ``cli.validation_operator.list``
* ``cli.tap.new``
* ``cli.docs.list``
* ``cli.docs.build``
* ``cli.datasource.profile``
* ``cli.datasource.list``
* ``cli.datasource.new``

These methods are decorated with ``@usage_statistics_enabled_method`` and when called, \
will emit messages with the top-level JSON Schema defined below. All decorated methods will emit the fields specified under \
the "required" key. The only fields unique to each method are the ``event`` (method name) and ``event_payload`` (payload specific to a particular method, which is usually empty) fields.

Click the "Payload/Referenced JSON Schemas" button to see schemas pertaining to particular event payloads or referenced in the top-level schema.

Click the "Message Examples" button to see message examples.

.. content-tabs::

    .. tab-container:: tab0
        :title: Top-Level JSON Schema

        .. code-block:: python

            usage_statistics_record_schema = {
                "$schema": "http://json-schema.org/schema#",
                "definitions": {
                    "anonymized_string": anonymized_string_schema,
                    "anonymized_datasource": anonymized_datasource_schema,
                    "anonymized_store": anonymized_store_schema,
                    "anonymized_class_info": anonymized_class_info_schema,
                    "anonymized_validation_operator": anonymized_validation_operator_schema,
                    "anonymized_action": anonymized_action_schema,
                    "empty_payload": empty_payload_schema,
                    "init_payload": init_payload_schema,
                    "run_validation_operator_payload": run_validation_operator_payload_schema,
                    "anonymized_data_docs_site": anonymized_data_docs_site_schema,
                    "anonymized_batch": anonymized_batch_schema,
                    "anonymized_expectation_suite": anonymized_expectation_suite_schema
                },
                "type": "object",
                "properties": {
                    "version": {
                        "enum": ["1.0.0"]
                    },
                    "event_time": {
                        "type": "string",
                        "format": "date-time"
                    },
                    "data_context_id": {
                        "type": "string",
                        "format": "uuid"
                    },
                    "data_context_instance_id": {
                        "type": "string",
                        "format": "uuid"
                    },
                    "ge_version": {
                        "type": "string",
                        "maxLength": 32
                    },
                    "success": {
                        "type": ["boolean", "null"]
                    },
                },
                "oneOf": [
                    {
                        "type": "object",
                        "properties": {
                            "event": {
                                "enum": ["data_context.__init__"],
                            },
                            "event_payload": {
                                "$ref": "#/definitions/init_payload"
                            }
                        }
                    },
                    {
                        "type": "object",
                        "properties": {
                            "event": {
                                "enum": ["data_context.run_validation_operator"],
                            },
                            "event_payload": {
                                "$ref": "#/definitions/run_validation_operator_payload"
                            },
                        }
                    },
                    {
                        "type": "object",
                        "properties": {
                            "event": {
                                "enum": ["data_asset.validate"],
                            },
                            "event_payload": {
                                "$ref": "#/definitions/anonymized_batch"
                            },
                        }
                    },
                    {
                        "type": "object",
                        "properties": {
                            "event": {
                                "enum": [
                                    "cli.suite.list",
                                    "cli.suite.edit",
                                    "cli.suite.new",
                                    "cli.store.list",
                                    "cli.project.check_config",
                                    "cli.validation_operator.run",
                                    "cli.validation_operator.list",
                                    "cli.tap.new",
                                    "cli.docs.list",
                                    "cli.docs.build",
                                    "cli.datasource.profile",
                                    "cli.datasource.list",
                                    "cli.datasource.new",
                                    "data_context.open_data_docs",
                                    "data_context.build_data_docs"
                                ],
                            },
                            "event_payload": {
                                "$ref": "#/definitions/empty_payload"
                            },
                        }
                    }
                ],
                "required": [
                    "version",
                    "event_time",
                    "data_context_id",
                    "data_context_instance_id",
                    "ge_version",
                    "event",
                    "success",
                    "event_payload"
                ]
            }

    .. tab-container:: tab1
        :title: Payload/Referenced JSON Schemas

        .. code-block:: python

            anonymized_string_schema = {
                "$schema": "http://json-schema.org/schema#",
                "type": "string",
                "minLength": 32,
                "maxLength": 32,
            }

            anonymized_datasource_schema = {
                "$schema": "http://json-schema.org/schema#",
                "title": "anonymized-datasource",
                "definitions": {
                    "anonymized_string": anonymized_string_schema
                },
                "oneOf": [
                    {
                        "type": "object",
                        "properties": {
                            "anonymized_name": {
                                "$ref": "#/definitions/anonymized_string"
                            },
                            "parent_class": {
                                "type": "string",
                                "maxLength": 256
                            },
                            "anonymized_class": {
                                "$ref": "#/definitions/anonymized_string"
                            },
                            "sqlalchemy_dialect": {
                                "type": "string",
                                "maxLength": 256,
                            }
                        },
                        "additionalProperties": False,
                        "required": [
                            "parent_class",
                            "anonymized_name"
                        ]
                    }
                ]
            }

            anonymized_class_info_schema = {
                "$schema": "http://json-schema.org/schema#",
                "title": "anonymized-class-info",
                "definitions": {
                    "anonymized_string": anonymized_string_schema
                },
                "oneOf": [
                    {
                        "type": "object",
                        "properties": {
                            "anonymized_name": {
                                "$ref": "#/definitions/anonymized_string"
                            },
                            "parent_class": {
                                "type": "string",
                                "maxLength": 256
                            },
                            "anonymized_class": {
                                "$ref": "#/definitions/anonymized_string"
                            },
                        },
                        "additionalProperties": True, # we don't want this to be true, but this is required to allow show_cta_footer
                        "required": [
                            "parent_class",
                        ]
                    }
                ]
            }

            anonymized_store_schema = {
                "$schema": "http://json-schema.org/schema#",
                "title": "anonymized-store",
                "definitions": {
                    "anonymized_string": anonymized_string_schema,
                    "anonymized_class_info": anonymized_class_info_schema
                },
                "oneOf": [
                    {
                        "type": "object",
                        "properties": {
                            "anonymized_name": {
                                "$ref": "#/definitions/anonymized_string"
                            },
                            "parent_class": {
                                "type": "string",
                                "maxLength": 256
                            },
                            "anonymized_class": {
                                "$ref": "#/definitions/anonymized_string"
                            },
                            "anonymized_store_backend": {
                                "$ref": "#/definitions/anonymized_class_info"
                            }
                        },
                        "additionalProperties": False,
                        "required": [
                            "parent_class",
                            "anonymized_name"
                        ]
                    }
                ]
            }

            anonymized_action_schema = {
                "$schema": "http://json-schema.org/schema#",
                "title": "anonymized-action",
                "definitions": {
                    "anonymized_string": anonymized_string_schema,
                },
                "oneOf": [
                    {
                        "type": "object",
                        "properties": {
                            "anonymized_name": {
                                "$ref": "#/definitions/anonymized_string"
                            },
                            "parent_class": {
                                "type": "string",
                                "maxLength": 256
                            },
                            "anonymized_class": {
                                "$ref": "#/definitions/anonymized_string"
                            },
                        },
                        "additionalProperties": False,
                        "required": [
                            "parent_class",
                            "anonymized_name"
                        ]
                    }
                ]
            }

            anonymized_validation_operator_schema = {
                "$schema": "http://json-schema.org/schema#",
                "title": "anonymized-validation-operator",
                "definitions": {
                    "anonymized_string": anonymized_string_schema,
                    "anonymized_action": anonymized_action_schema
                },
                "oneOf": [
                    {
                        "type": "object",
                        "properties": {
                            "anonymized_name": {
                                "$ref": "#/definitions/anonymized_string"
                            },
                            "parent_class": {
                                "type": "string",
                                "maxLength": 256
                            },
                            "anonymized_class": {
                                "$ref": "#/definitions/anonymized_string"
                            },
                            "anonymized_action_list": {
                                "type": "array",
                                "maxItems": 1000,
                                "items": {
                                    "$ref": "#/definitions/anonymized_action"
                                },
                            }
                        },
                        "additionalProperties": False,
                        "required": [
                            "parent_class",
                            "anonymized_name"
                        ]
                    }
                ]
            }

            empty_payload_schema = {
                "$schema": "http://json-schema.org/schema#",
                "type": "object",
                "properties": {
                },
                "required": [
                ],
                "additionalProperties": False
            }

            anonymized_data_docs_site_schema = {
                "$schema": "http://json-schema.org/schema#",
                "title": "anonymized-validation-operator",
                "definitions": {
                    "anonymized_string": anonymized_string_schema,
                    "anonymized_class_info": anonymized_class_info_schema
                },
                "oneOf": [
                    {
                        "type": "object",
                        "properties": {
                            "anonymized_name": {
                                "$ref": "#/definitions/anonymized_string"
                            },
                            "parent_class": {
                                "type": "string",
                                "maxLength": 256
                            },
                            "anonymized_class": {
                                "$ref": "#/definitions/anonymized_string"
                            },
                            "anonymized_store_backend": {
                                "$ref": "#/definitions/anonymized_class_info"
                            },
                            "anonymized_site_index_builder": {
                                "$ref": "#/definitions/anonymized_class_info"
                            }
                        },
                        "additionalProperties": False,
                        "required": [
                            "parent_class",
                            "anonymized_name"
                        ]
                    }
                ]
            }

            anonymized_expectation_suite_schema = {
                "$schema": "http://json-schema.org/schema#",
                "title": "anonymized-expectation_suite_schema",
                "definitions": {
                    "anonymized_string": anonymized_string_schema,
                },
                "oneOf": [
                    {
                        "type": "object",
                        "properties": {
                            "anonymized_name": {
                                "$ref": "#/definitions/anonymized_string"
                            },
                            "expectation_count": {
                                "type": "number"
                            },
                            "anonymized_expectation_type_counts": {
                                "type": "object"
                            },
                        },
                        "additionalProperties": False,
                        "required": [
                        ]
                    }
                ]
            }

            init_payload_schema = {
                "$schema": "https://json-schema.org/schema#",
                "definitions": {
                    "anonymized_string": anonymized_string_schema,
                    "anonymized_class_info": anonymized_class_info_schema,
                    "anonymized_datasource": anonymized_datasource_schema,
                    "anonymized_validation_operator": anonymized_validation_operator_schema,
                    "anonymized_data_docs_site": anonymized_data_docs_site_schema,
                    "anonymized_store": anonymized_store_schema,
                    "anonymized_action": anonymized_action_schema,
                    "anonymized_expectation_suite": anonymized_expectation_suite_schema
                },
                "type": "object",
                "properties": {
                    "version": {
                        "enum": ["1.0.0"]
                    },
                    "platform.system": {
                        "type": "string",
                        "maxLength": 256
                    },
                    "platform.release": {
                        "type": "string",
                        "maxLength": 256
                    },
                    "version_info": {
                        "type": "string",
                        "maxLength": 256
                    },
                    "anonymized_datasources": {
                        "type": "array",
                        "maxItems": 1000,
                        "items": {
                            "$ref": "#/definitions/anonymized_datasource"
                        }
                    },
                    "anonymized_stores": {
                        "type": "array",
                        "maxItems": 1000,
                        "items": {
                            "$ref": "#/definitions/anonymized_store"
                        }
                    },
                    "anonymized_validation_operators": {
                        "type": "array",
                        "maxItems": 1000,
                        "items": {
                            "$ref": "#/definitions/anonymized_validation_operator"
                        },
                    },
                    "anonymized_data_docs_sites": {
                        "type": "array",
                        "maxItems": 1000,
                        "items": {
                            "$ref": "#/definitions/anonymized_data_docs_site"
                        },
                    },
                    "anonymized_expectation_suites": {
                        "type": "array",
                        "items": {
                            "$ref": "#/definitions/anonymized_expectation_suite"
                        }
                    }
                },
                "required": [
                    "platform.system",
                    "platform.release",
                    "version_info",
                    "anonymized_datasources",
                    "anonymized_stores",
                    "anonymized_validation_operators",
                    "anonymized_data_docs_sites",
                    "anonymized_expectation_suites"
                ],
                "additionalProperties": False
            }

            anonymized_batch_schema = {
                "$schema": "http://json-schema.org/schema#",
                "title": "anonymized-batch",
                "definitions": {
                    "anonymized_string": anonymized_string_schema,
                },
                "oneOf": [
                    {
                        "type": "object",
                        "properties": {
                            "anonymized_batch_kwarg_keys": {
                                "type": "array",
                                "maxItems": 1000,
                                "items": {
                                    "oneOf": [
                                        {"$ref": "#/definitions/anonymized_string"},
                                        {
                                            "type": "string",
                                            "maxLength": 256
                                        }
                                    ]
                                },
                            },
                            "anonymized_expectation_suite_name": {
                                "$ref": "#/definitions/anonymized_string"
                            },
                            "anonymized_datasource_name": {
                                "$ref": "#/definitions/anonymized_string"
                            }
                        },
                        "additionalProperties": False,
                        "required": [
                            "anonymized_batch_kwarg_keys",
                            "anonymized_expectation_suite_name",
                            "anonymized_datasource_name"
                        ]
                    }
                ]
            }

            run_validation_operator_payload_schema = {
                "$schema": "http://json-schema.org/schema#",
                "definitions": {
                    "anonymized_string": anonymized_string_schema,
                    "anonymized_batch": anonymized_batch_schema
                },
                "type": "object",
                "properties": {
                    "anonymized_operator_name": {
                        "type": "string",
                        "maxLength": 256,
                    },
                    "anonymized_batches": {
                        "type": "array",
                        "maxItems": 1000,
                        "items": {
                            "$ref": "#/definitions/anonymized_batch"
                        }
                    }
                },
                "required": [
                    "anonymized_operator_name"
                ],
                "additionalProperties": False
            }

    .. tab-container:: tab2
        :title: Message Examples

        * ``data_context.__init__``
            .. code-block:: python

                message = {
                    'event': 'data_context.__init__',
                    'event_payload': {
                        'platform.system': 'Darwin',
                        'platform.release': '19.3.0',
                        'version_info': "sys.version_info(major=3, minor=7, micro=4, releaselevel='final', serial=0)",
                        'anonymized_datasources': [
                            {
                                'anonymized_name': 'f57d8a6edae4f321b833384801847498',
                                'parent_class': 'SqlAlchemyDatasource',
                                'sqlalchemy_dialect': 'postgresql'
                            }
                        ],
                        'anonymized_stores': [
                            {
                                'anonymized_name': '078eceafc1051edf98ae2f911484c7f7',
                                'parent_class': 'ExpectationsStore',
                                'anonymized_store_backend': {
                                    'parent_class': 'TupleFilesystemStoreBackend'
                                }
                            },
                            {
                                'anonymized_name': '313cbd9858dd92f3fc2ef1c10ab9c7c8',
                                'parent_class': 'ValidationsStore',
                                'anonymized_store_backend': {
                                    'parent_class': 'TupleFilesystemStoreBackend'
                                }
                            },
                            {
                                'anonymized_name': '2d487386aa7b39e00ed672739421473f',
                                'parent_class': 'EvaluationParameterStore',
                                'anonymized_store_backend': {
                                    'parent_class': 'InMemoryStoreBackend'
                                }
                            }
                        ],
                        'anonymized_validation_operators': [
                            {
                                'anonymized_name': '99d14cc00b69317551690fb8a61aca94',
                                'parent_class': 'ActionListValidationOperator',
                                'anonymized_action_list': [
                                    {
                                        'anonymized_name': '5a170e5b77c092cc6c9f5cf2b639459a',
                                        'parent_class': 'StoreValidationResultAction'
                                    },
                                    {
                                        'anonymized_name': '0fffe1906a8f2a5625a5659a848c25a3',
                                        'parent_class': 'StoreEvaluationParametersAction'
                                    },
                                    {
                                        'anonymized_name': '101c746ab7597e22b94d6e5f10b75916',
                                        'parent_class': 'UpdateDataDocsAction'
                                    }
                                ]
                            }
                        ],
                        'anonymized_data_docs_sites': [
                            {
                                'parent_class': 'SiteBuilder',
                                'anonymized_name': 'eaf0cf17ad63abf1477f7c37ad192700',
                                'anonymized_store_backend': {'parent_class': 'TupleFilesystemStoreBackend'},
                                'anonymized_site_index_builder': {
                                    'parent_class': 'DefaultSiteIndexBuilder',
                                    'show_cta_footer': True
                                }
                            }
                        ],
                        'anonymized_expectation_suites': [
                            {
                                'anonymized_name': '238e99998c7674e4ff26a9c529d43da4',
                                'expectation_count': 8,
                                'anonymized_expectation_type_counts': {
                                    'expect_column_value_lengths_to_be_between': 1,
                                    'expect_table_row_count_to_be_between': 1,
                                    'expect_column_values_to_not_be_null': 2,
                                    'expect_column_distinct_values_to_be_in_set': 1,
                                    'expect_column_kl_divergence_to_be_less_than': 1,
                                    'expect_table_column_count_to_equal': 1,
                                    'expect_table_columns_to_match_ordered_list': 1
                                }
                            }
                        ]
                    },
                    'success': True,
                    'version': '1.0.0',
                    'event_time': '2020-03-28T01:14:21.155Z',
                    'data_context_id': '96c547fe-e809-4f2e-b122-0dc91bb7b3ad',
                    'data_context_instance_id': '445a8ad1-2bd0-45ce-bb6b-d066afe996dd',
                    'ge_version': '0.9.7+244.g56d67e51d.dirty'
                }

        * ``data_context.open_data_docs``, ``data_context.build_data_docs``, ``cli.suite.list``, ``cli.suite.edit``, ``cli.suite.new``, ``cli.store.list``, ``cli.project.check_config``, ``cli.validation_operator.run``, ``cli.validation_operator.list``, ``cli.tap.new``, ``cli.docs.list``, ``cli.docs.build``, ``cli.datasource.profile``, ``cli.datasource.list``, ``cli.datasource.new``
            .. code-block:: python

                message = {
                    'event': 'data_context.open_data_docs',
                    'event_payload': {},
                    'success': True,
                    'version': '1.0.0',
                    'event_time': '2020-03-28T01:14:21.155Z',
                    'data_context_id': '96c547fe-e809-4f2e-b122-0dc91bb7b3ad',
                    'data_context_instance_id': '445a8ad1-2bd0-45ce-bb6b-d066afe996dd',
                    'ge_version': '0.9.7+244.g56d67e51d.dirty'
                }

        * ``data_context.run_validation_operator``
            .. code-block:: python

                message = {
                    'event': 'data_context.run_validation_operator',
                    'event_payload': {
                        'anonymized_operator_name': '50daa62a8739db21009f452f7e36153b',
                        'anonymized_batches': [
                            {
                                'anonymized_batch_kwarg_keys': ['datasource', 'PandasInMemoryDF', 'ge_batch_id'],
                                'anonymized_expectation_suite_name': '6722fe57bb1146340c0ab6d9851cd93a',
                                'anonymized_datasource_name': '760a442fb42732d75528ebdd8696499d'
                            }
                        ]
                    },
                    'success': True,
                    'version': '1.0.0',
                    'event_time': '2020-03-31T02:23:20.011Z',
                    'data_context_id': '705dd2a2-27f8-470f-9ebe-e7058fd7a534',
                    'data_context_instance_id': '3424349a-35ce-4eda-a48f-0281543854a1',
                    'ge_version': '0.9.7+282.g9bbc2ad81.dirty'
                }

        * ``data_asset.validate``
            .. code-block:: python

                message = {
                    'event': 'data_asset.validate',
                    'event_payload': {
                        'anonymized_batch_kwarg_keys': ['datasource', 'PandasInMemoryDF', 'ge_batch_id'],
                        'anonymized_expectation_suite_name': '6722fe57bb1146340c0ab6d9851cd93a',
                        'anonymized_datasource_name': '760a442fb42732d75528ebdd8696499d'
                    },
                    'success': True,
                    'version': '1.0.0', 'event_time': '2020-03-31T02:22:10.284Z',
                    'data_context_id': '705dd2a2-27f8-470f-9ebe-e7058fd7a534',
                    'data_context_instance_id': '3424349a-35ce-4eda-a48f-0281543854a1',
                    'ge_version': '0.9.7+282.g9bbc2ad81.dirty'
                }

We may periodically update messages or add messages for additional methods as necessary to improve the library, but we will include information about such changes here.
Other than standard web request data, we don’t collect any data data that could be used to identify individual users.
You can suppress the images by changing ``static_images_dir`` in ``great_expectations/render/view/templates/top_navbar.j2``.

You can opt out of event tracking at any time by adding the following to the top of your project’s ``great_expectations/great_expectations.yml`` file:

.. code-block:: yaml

    anonymized_usage_statistics:
      enabled: false
      data_context_id: 705dd2a2-27f8-470f-9ebe-e7058fd7a534

To opt out of event tracking when setting up a project using the ``great_expectations init`` cli command, you may pass the flag ``--no-usage-stats``.

If you would like to opt out of usage statistics globally, for all Great Expectations projects on a particular machine, you may do so by doing one of the following:

1. Setting the env variable GE_USAGE_STATS to any of the following: FALSE, False, false, 0.
2. Creating a ~/.great_expectations/great_expectations.conf file and setting the ‘enabled’ option in the [anonymous_usage_statistics] section to any of the following: FALSE, False, false, 0.
3. Creating a /etc/great_expectations.conf file and setting the ‘enabled’ option in the [anonymous_usage_statistics] section to any of the following: FALSE, False, false, 0.

great_expectations.conf opt-out file example:

.. code-block::

    [anonymous_usage_statistics]
    enabled=True

Doing any of the above will override any usage statistics settings found in a project's ``great_expectations.yml`` file and are listed in order of precedence (i.e. env variable value trumps settings found in great_expectations.conf files).

Please reach out `on Slack <https://greatexpectations.io/slack>`__ if you have any questions or comments.
