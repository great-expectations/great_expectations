anonymized_name_schema = {
   "$schema": "http://json-schema.org/schema#",
   "type": "string",
   "minLength": 32,
   "maxLength": 32,
}

anonymized_datasource_schema = {
   "$schema": "http://json-schema.org/schema#",
   "title": "anonymized-datasource",
   "definitions": {
      "anonymized_name": anonymized_name_schema
   },
   "oneOf": [
      {
         "type": "object",
         "properties": {
            "anonymized_name": {
               "$ref": "#/definitions/anonymized_name"
            },
            "parent_class": {
               "type": "string",
               "maxLength": 256
            },
            "anonymized_class": {
               "$ref": "#/definitions/anonymized_name"
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


anonymized_store_backend_schema = {
   "$schema": "http://json-schema.org/schema#",
   "title": "anonymized-store-backend",
   "definitions": {
      "anonymized_name": anonymized_name_schema
   },
   "oneOf": [
      {
         "type": "object",
         "properties": {
            "anonymized_name": {
               "$ref": "#/definitions/anonymized_name"
            },
            "parent_class": {
               "type": "string",
               "maxLength": 256
            },
            "anonymized_class": {
               "$ref": "#/definitions/anonymized_name"
            },
         },
         "additionalProperties": False,
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
      "anonymized_name": anonymized_name_schema,
      "anonymized_store_backend": anonymized_store_backend_schema
   },
   "oneOf": [
      {
         "type": "object",
         "properties": {
            "anonymized_name": {
               "$ref": "#/definitions/anonymized_name"
            },
            "parent_class": {
               "type": "string",
               "maxLength": 256
            },
            "anonymized_class": {
               "$ref": "#/definitions/anonymized_name"
            },
            "anonymized_store_backend": {
               "$ref": "#/definitions/anonymized_store_backend"
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
      "anonymized_name": anonymized_name_schema,
   },
   "oneOf": [
      {
         "type": "object",
         "properties": {
            "anonymized_name": {
               "$ref": "#/definitions/anonymized_name"
            },
            "parent_class": {
               "type": "string",
               "maxLength": 256
            },
            "anonymized_class": {
               "$ref": "#/definitions/anonymized_name"
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
      "anonymized_name": anonymized_name_schema,
      "anonymized_action": anonymized_action_schema
   },
   "oneOf": [
      {
         "type": "object",
         "properties": {
            "anonymized_name": {
               "$ref": "#/definitions/anonymized_name"
            },
            "parent_class": {
               "type": "string",
               "maxLength": 256
            },
            "anonymized_class": {
               "$ref": "#/definitions/anonymized_name"
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


init_payload_schema = {
   "$schema": "https://json-schema.org/schema#",
   "definitions": {
      "anonymized_datasource": anonymized_datasource_schema,
      "anonymized_store": anonymized_store_schema,
      "anonymized_validation_operator": anonymized_validation_operator_schema
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
   },
   "required": [
      "platform.system",
      "platform.release",
      "version_info",
      "anonymized_datasources",
   ],
   "additionalProperties": False
}

run_validation_operator_payload_schema = {
   "$schema": "http://json-schema.org/schema#",
   "type": "object",
   "properties": {
      "anonymized_operator_name": {
         "type": "string",
         "maxLength": 256,
      },
      "anonymized_datasource_name": {
         "type": "string",
         "maxLength": 256,
      },
      "anonymized_batch_kwargs": {
         "type": "array",
         "maxItems": 10,
         "items": {
            "type": "string",
            "maxLength": 256,
         }
      },
      "n_assets": {
         "type": "number"
      }
   },
   "required": [
      "anonymized_operator_name",
      # "anonymized_datasource_name",
      # "anonymized_batch_kwargs"
   ],
   "additionalProperties": False
}

build_data_docs_payload_schema = {
   "$schema": "http://json-schema.org/schema#",
   "type": "object",
   "properties": {
   },
   "required": [
   ],
   "additionalProperties": False
}

usage_statistics_record_schema = {
   "$schema": "http://json-schema.org/schema#",
   "definitions": {
      "anonymized_name": anonymized_name_schema,
      "anonymized_datasource": anonymized_datasource_schema,
      "anonymized_store": anonymized_store_schema,
      "init_payload": init_payload_schema,
      "run_validation_operator_payload": run_validation_operator_payload_schema,
      "build_data_docs_payload": build_data_docs_payload_schema
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
               "enum": ["data_context.build_data_docs"],
            },
            "event_payload": {
               "$ref": "#/definitions/build_data_docs_payload"
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
