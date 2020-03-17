import copy
import datetime
import logging
import requests
import sys
import platform
from hashlib import md5
from functools import wraps
import jsonschema

from great_expectations import __version__ as ge_version
from great_expectations.core import nested_update
from great_expectations.datasource import DatasourceAnonymizer

DEFAULT_USAGE_STATISTICS_URL = "https://4tdy72oi8f.execute-api.us-east-1.amazonaws.com/prod/great_expectations/v1/usage_statistics"
logger = logging.getLogger(__name__)


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


class UsageStatisticsHandler(object):

    def __init__(self, data_context, data_context_id, usage_statistics_url):
        self._datasource_anonymizer = DatasourceAnonymizer()
        self._data_context_id = data_context_id
        self._data_context_instance_id = data_context.instance_id
        self._platform_system = platform.system()
        self._platform_release = platform.release()
        self._version_info = sys.version_info
        self._data_context = data_context
        self._ge_version = ge_version
        self._anonymized_datasources = []
        self._url = usage_statistics_url
        self._enabled = True

    def register_usage_statistics_details(self):
        """Adds information that may be available only after full data context construction, but is useful to
        calculate only one time (for example, anonymization)."""
        self._anonymized_datasources = [
            self._datasource_anonymizer.anonymize_datasource_class_name(datasource["class_name"], datasource["module_name"])
            for datasource in self._data_context.list_datasources()
        ]

    def build_message(self, record):
        message = copy.deepcopy(record)
        message["event_time"] = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        message["data_context_id"] = self._data_context_id
        message["data_context_instance_id"] = self._data_context_instance_id
        message["ge_version"] = self._ge_version
        message["platform.system"] = self._platform_system
        message["platform.release"] = self._platform_release
        message["version_info"] = self._version_info
        message["anonymized_datasources"] = self._anonymized_datasources
        return message

    def validate_record(self, record):
        try:
            jsonschema.validate(record, schema=usage_statistics_record_schema)
            return True
        except jsonschema.ValidationError as e:
            logger.debug("invalid record: " + str(e))
            return False

    def emit(self, record):
        """
        Emit a record.
        """
        if not self._enabled:
            return

        if not self.validate_record(record):
            return

        try:
            message = self.build_message(record)
            requests.post(self._url, json=message)
        # noinspection PyBroadException
        except Exception:
            # We *always* tolerate *any* error in usage statistics
            pass


def get_usage_statistics_handler(args_array):
    try:
        # If the object is usage_statistics-capable, then it will have a usage_statistics_handler
        handler = getattr(args_array[0], "_usage_statistics_handler", None)
        if handler is not None and not isinstance(handler, UsageStatisticsHandler):
            logger.debug("Invalid UsageStatisticsHandler found on object.")
            handler = None
    except IndexError:
        # A wrapped method that is not an object
        handler = None
    except AttributeError:
        # A wrapped method that is not usage_statistics capable
        handler = None
    except Exception as e:
        # An unknown error -- but we still fail silently
        logger.debug("Unrecognized error when trying to find usage_statistics_handler: " + str(e))
        handler = None
    return handler


def usage_statistics_enabled_method(func=None, method_name=None, args_payload_fn=None, result_payload_fn=None):
    if callable(func):
        if method_name is None:
            method_name = func.__name__

        @wraps(func)
        def usage_statistics_wrapped_method(*args, **kwargs):
            # Set event_payload now so it can be updated below
            event_payload = {}
            record = {"event_payload": event_payload, "method": method_name}
            handler = None
            try:
                if args_payload_fn is not None:
                    nested_update(event_payload, args_payload_fn(*args, **kwargs))
                res = func(*args, **kwargs)
                # We try to get the handler only now, so that it *could* be initialized in func, e.g. if it is an
                # __init__ method
                handler = get_usage_statistics_handler(args)
                if result_payload_fn is not None:
                    nested_update(event_payload, result_payload_fn(res))
                record["success"] = True
                if handler is not None:
                    handler.emit(record)
            except Exception:
                record["success"] = False
                if handler:
                    handler.emit(record)
                raise

            return res

        return usage_statistics_wrapped_method
    else:
        def usage_statistics_wrapped_method_partial(func):
            return usage_statistics_enabled_method(func,
                                                   method_name=method_name,
                                                   args_payload_fn=args_payload_fn,
                                                   result_payload_fn=result_payload_fn)
        return usage_statistics_wrapped_method_partial


def run_validation_operator_usage_statistics(
        data_context,  # self
        validation_operator_name,
        assets_to_validate,
        run_id=None,
        **kwargs
):
    payload = {}
    try:
        payload["validation_operator_name"] = md5(validation_operator_name.encode("utf-8")).hexdigest()
    except TypeError as e:
        logger.warning("run_validation_operator_usage_statistics: Unable to create validation_operator_name hash")
    try:
        payload["n_assets"] = len(assets_to_validate)
    except TypeError as e:
        logger.debug("run_validation_operator_usage_statistics: Unable to create n_assets payload field")
    return payload
