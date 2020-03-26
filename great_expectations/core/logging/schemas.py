usage_statistics_record_schema = {
   "schema": {
      "definitions": {
         "anonymized_name": {
            "type": "string",
            "minLength": 32,
            "maxLength": 32,
         },
         "anonymized_datasource": {
            "type": "object",
            "properties": {
               "anonymized_name": {
                  "$ref": "#/definitions/anonymized_name"
               },
               "parent_class": {
                  "type": "string",
                  "maxLength": 32
               },
               "anonymized_class": {
                  "$ref": "#/definitions/anonymized_name"
               }
            },
            "additionalProperties": False,
            "required": [
               "anonymized_name",
               "parent_class"
            ]
         },
         "init_payload": {
            "type": "object",
            "properties": {
               "platform.system": {
                  "type": "string",
                  "maxLength": 256
               },
               "platform.release": {
                  "type": "string",
                  "maxLength": 256
               },
               "version_info": {
                  "type": "array",
                  "items": {
                     "anyOf": [
                        {
                           "type": "string",
                           "maxLength": 20
                        },
                        {
                           "type": "number",
                           "minimum": 0
                        }
                     ]
                  },
                  "maxItems": 6
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
                     "type": "object",
                     "properties": {
                        "parent_class": {
                           "type": "string",
                           "maxLength": 32
                        },
                        "custom_class": {
                           "type": "string",
                           "maxLength": 32
                        },
                        "store_backend_parent_class": {
                           "type": "string",
                           "maxLength": 32
                        },
                        "store_backend_custom_class": {
                           "type": "string",
                           "maxLength": 32
                        }
                     },
                     "required": [
                        "parent_class",
                        "store_backend_parent_class"
                     ]
                  }
               },
               "anonymized_validation_operators": {
                  "type": "array",
                  "maxItems": 1000,
                  "items": {
                     "type": "object",
                     "properties": {
                        "parent_class": {
                           "type": "string",
                           "maxLength": 32
                        },
                        "custom_class": {
                           "type": "string",
                           "maxLength": 32
                        },
                        "action_list": {
                           "type": "array",
                           "items": {
                              "type": "object",
                              "properties": {
                                 "parent_class"
                              }
                           }
                        },
                     },
                     "required": [
                        "parent_class",
                        "store_backend_parent_class"
                     ]
                  }
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
      },
      "type": "object",
      "properties": {
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
         "method": {
            "type": "string",
            "maxLength": 256
         },
         "success": {
            "type": ["boolean", "null"]
         },
         "event_payload": {
            "type": "object",
            "maxProperties": 100
         }
      },
      "additionalProperties": False,
      "required": [
         "event_time",
         "data_context_id",
         "data_context_instance_id",
         "ge_version",
         "method",
         "success",
         "event_payload"
      ]
   }
}


run_validation_operator_payload_schema = {
   "schema": {
      "type": "object",
      "properties": {
         "operator_name_hash": {
            "type": "string",
            "maxLength": 256,
         },
         "datasource_name_hash": {
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
      },
      "required": [
         "anonymized_operator_name",
         "anonymized_datasource_name",
         "anonymized_batch_kwargs"
      ],
      "additionalProperties": False
   }
}

