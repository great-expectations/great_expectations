.. _usage_statistics:


###############
Usage Statistics
###############

We use CDN fetch rates to get a sense of total community usage of Great Expectations. Specifically, we host images and style sheets on a public CDN and count the number of unique IPs from which resources are fetched.

In addition, we also collect usage statistics when certain Great Expectations methods are called. These methods are decorated with `@usage_statistics_enabled_method` and when called, will emit usage statistics messages with the following schema:

.. code-block:: python

    usage_statistics_record_schema = {
       "schema": {
          "type": "object",
          "properties": {
             "event_time": {  # time method was called
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
             "method": {  # name of decorated method that was called
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
             "anonymized_datasources": {  # list of classes of configured DataSources - custom DataSource class names are hashed
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
             "event_payload": {  # payload unique to method called
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

All decorated methods will emit the above required fields; the only field unique to each method is the `event_payload`.

Great Expectations currently emits usage statistics for the following methods:

    * `data_context.__init__`
        * event_payload: `{}`
    * `data_context.run_validation_operator`
          * event_payload:
          .. code-block:: python

              {
                "validation_operator_name": MD5 hash of validation operator name,
                "n_assets": number of assets validated
              }

    * `data_context.build_data_docs`
          * event_payload: `{}`

Other than standard web request data, we don’t collect any data data that could be used to identify individual users. You can suppress the images by changing ``static_images_dir`` in ``great_expectations/render/view/templates/top_navbar.j2``.

Please reach out `on Slack <https://greatexpectations.io/slack>`__ if you have any questions or comments.


