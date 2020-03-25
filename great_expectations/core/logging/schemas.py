usage_statistics_record_schema = {
   "schema": {
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
            "type": "boolean"
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
               "type": "object",
               "properties": {
                  "parent_class": {
                     "type": "string",
                     "maxLength": 32
                  },
                  "custom_class": {
                     "type": "string",
                     "maxLength": 32
                  }
               },
               "required": [
                  "parent_class"
               ]
            }
         },
         "event_payload": {
            "type": "object",
            "maxProperties": 100
         }
      },
      "required": [
         "event_time",
         "data_context_id",
         "data_context_instance_id",
         "ge_version",
         "method",
         "success",
         "platform.system",
         "platform.release",
         "version_info",
         "anonymized_datasources",
         "event_payload"
      ]
   }
}
usage_statistics_mini_record_schema = {
   "schema": {
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
            "type": "boolean"
         },
         "event_payload": {
            "type": "object",
            "maxProperties": 100
         }
      },
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