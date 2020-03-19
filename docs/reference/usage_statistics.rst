.. _usage_statistics:


###############
Usage Statistics
###############

We use CDN fetch rates to get a sense of total community usage of Great Expectations. Specifically, we host images and style sheets on a public CDN and count the number of unique IPs from which resources are fetched.

In addition, Great Expectations performs simple event tracking by emitting usage statistics messages when certain Great Expectations methods are called.\
We do not track credentials, the contents of Expectations or Validation results, or names. These methods are decorated with `@usage_statistics_enabled_method` and when called, will emit messages with the following schema:

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

All decorated methods will emit the above fields. The only fields unique to each method are the `method` and `event_payload` fields.

Great Expectations currently emits usage statistics for the following methods:

* `DataContext.__init__`
    * event_payload: `{}`
    * message example:
    .. code-block:: python

        {
             "event_payload": {},
             "method": "data_context.__init__",
             "success": true,
             "event_time": "2020-03-17T23:11:02.042Z",
             "data_context_id": "705dd2a2-27f8-470f-9ebe-e7058fd7a534",
             "data_context_instance_id": "5439e0cc-e95b-4bf2-aa6f-703400697f88",
             "ge_version": "0.9.1+200.g9ae4e7700",
             "platform.system": "Darwin",
             "platform.release": "19.3.0",
             "version_info": [3, 7, 4, "final", 0],
             "anonymized_datasources": [
                {"parent_class": "PandasDatasource"}
             ]
        }

* `DataContext.run_validation_operator`
      * event_payload:
      .. code-block:: python

          {
            "validation_operator_name": MD5 hash of validation operator name, salted with data context id
            "n_assets": number of assets validated
          }
      * message example:
      .. code-block:: python

          {
             'event_payload': {
                  'validation_operator_name': 'e9cadb1ef634807b784f572776d653ba',
                  'n_assets': 1
             },
             'method': 'data_context.run_validation_operator',
             'success': True,
             'event_time': '2020-03-17T23:11:02.441Z',
             'data_context_id': '705dd2a2-27f8-470f-9ebe-e7058fd7a534',
             'data_context_instance_id': '5439e0cc-e95b-4bf2-aa6f-703400697f88',
             'ge_version': '0.9.1+200.g9ae4e7700',
             'platform.system': 'Darwin',
             'platform.release': '19.3.0',
             'version_info': [3, 7, 4, 'final', 0],
             'anonymized_datasources': [
                  {'parent_class': 'PandasDatasource'}
             ]
          }

* `DataContext.build_data_docs`
    * event_payload: `{}`
    * message example:
    .. code-block:: python

        {
             "event_payload": {},
             "method": "data_context.build_data_docs",
             "success": true,
             "event_time": "2020-03-16T23:11:02.042Z",
             "data_context_id": "705dd2a2-27f8-470f-9ebe-e7058fd7a534",
             "data_context_instance_id": "5439e0cc-e95b-4bf2-aa6f-703400697f88",
             "ge_version": "0.9.1+200.g9ae4e7700",
             "platform.system": "Darwin",
             "platform.release": "19.3.0",
             "version_info": [3, 7, 4, "final", 0],
             "anonymized_datasources": [
                {"parent_class": "PandasDatasource"}
             ]
        }
We may periodically update messages or add messages for additional methods as necessary to improve the library, but we will include information about such changes here.
Other than standard web request data, we don’t collect any data data that could be used to identify individual users.
You can suppress the images by changing ``static_images_dir`` in ``great_expectations/render/view/templates/top_navbar.j2``.

You can opt out of event tracking at any time by adding the following to the top of your project’s `great_expectations/great_expectations.yml` file:

.. code-block:: yaml

    anonymized_usage_statistics:
      enabled: false
      data_context_id: 705dd2a2-27f8-470f-9ebe-e7058fd7a534


Please reach out `on Slack <https://greatexpectations.io/slack>`__ if you have any questions or comments.
