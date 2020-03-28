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
      "anonymized_datasource": anonymized_datasource_schema,
      "anonymized_store": anonymized_store_schema,
      "anonymized_validation_operator": anonymized_validation_operator_schema,
      "anonymized_data_docs_site": anonymized_data_docs_site_schema,
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
               "enum": ["data_context.build_data_docs"],
            },
            "event_payload": {
               "$ref": "#/definitions/empty_payload"
            },
         }
      },
      {
         "type": "object",
         "properties": {
            "event": {
               "enum": ["data_context.open_data_docs"],
            },
            "event_payload": {
               "$ref": "#/definitions/empty_payload"
            },
         }
      },
      {
         "type": "object",
         "properties": {
            "event": {
               "enum": ["cli.suite.new"],
            },
            "event_payload": {
               "$ref": "#/definitions/empty_payload"
            },
         }
      },
      {
         "type": "object",
         "properties": {
            "event": {
               "enum": ["cli.suite.edit"],
            },
            "event_payload": {
               "$ref": "#/definitions/empty_payload"
            },
         }
      },
      {
         "type": "object",
         "properties": {
            "event": {
               "enum": ["cli.datasource.new"],
            },
            "event_payload": {
               "$ref": "#/definitions/empty_payload"
            },
         }
      },
      {
         "type": "object",
         "properties": {
            "event": {
               "enum": ["cli.datasource.list"],
            },
            "event_payload": {
               "$ref": "#/definitions/empty_payload"
            },
         }
      },
      {
         "type": "object",
         "properties": {
            "event": {
               "enum": ["cli.datasource.profile"],
            },
            "event_payload": {
               "$ref": "#/definitions/empty_payload"
            },
         }
      },
      {
         "type": "object",
         "properties": {
            "event": {
               "enum": ["cli.docs.build"],
            },
            "event_payload": {
               "$ref": "#/definitions/empty_payload"
            },
         }
      },
      {
         "type": "object",
         "properties": {
            "event": {
               "enum": ["cli.docs.list"],
            },
            "event_payload": {
               "$ref": "#/definitions/empty_payload"
            },
         }
      },
      {
         "type": "object",
         "properties": {
            "event": {
               "enum": ["cli.tap.new"],
            },
            "event_payload": {
               "$ref": "#/definitions/empty_payload"
            },
         }
      },
      {
         "type": "object",
         "properties": {
            "event": {
               "enum": ["cli.validation_operator.list"],
            },
            "event_payload": {
               "$ref": "#/definitions/empty_payload"
            },
         }
      },
      {
         "type": "object",
         "properties": {
            "event": {
               "enum": ["cli.validation_operator.run"],
            },
            "event_payload": {
               "$ref": "#/definitions/empty_payload"
            },
         }
      },
      {
         "type": "object",
         "properties": {
            "event": {
               "enum": ["cli.project.check_config"],
            },
            "event_payload": {
               "$ref": "#/definitions/empty_payload"
            },
         }
      },
      {
         "type": "object",
         "properties": {
            "event": {
               "enum": ["cli.store.list"],
            },
            "event_payload": {
               "$ref": "#/definitions/empty_payload"
            },
         }
      },
      {
         "type": "object",
         "properties": {
            "event": {
               "enum": ["cli.suite.new"],
            },
            "event_payload": {
               "$ref": "#/definitions/empty_payload"
            },
         }
      },
      {
         "type": "object",
         "properties": {
            "event": {
               "enum": ["cli.suite.edit"],
            },
            "event_payload": {
               "$ref": "#/definitions/empty_payload"
            },
         }
      },
   {
         "type": "object",
         "properties": {
            "event": {
               "enum": ["cli.suite.list"],
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
