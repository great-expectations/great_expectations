.. _usage_statistics:


###############
Usage Statistics
###############

We use CDN fetch rates to get a sense of total community usage of Great Expectations. Specifically, we host images and style sheets on a public CDN and count the number of unique IPs from which resources are fetched.

In addition, Great Expectations performs simple event tracking by emitting usage statistics messages when certain Great Expectations methods are called.

We do not track credentials, the contents of Expectations or Validation results, or names.

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
