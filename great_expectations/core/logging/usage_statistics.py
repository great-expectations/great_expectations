import copy
import datetime
import logging
import requests
import sys
import platform

import jsonschema

from functools import wraps

from great_expectations import __version__ as ge_version
from great_expectations.core import nested_update
from great_expectations.core.logging.anonymizer import Anonymizer
from great_expectations.core.logging.schemas import usage_statistics_mini_payload_schema
from great_expectations.datasource.datasource_anonymizer import DatasourceAnonymizer

logger = logging.getLogger(__name__)

_anonymizers = dict()


class UsageStatisticsHandler(object):

    def __init__(self, data_context, data_context_id, usage_statistics_url):
        self._datasource_anonymizer = DatasourceAnonymizer(data_context_id)
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
            self._datasource_anonymizer.anonymize_datasource_class_name(datasource["class_name"], datasource.get("module_name"))
            for datasource in self._data_context.list_datasources()
        ]

    def build_detailed_payload(self):
        return {
            "platform.system": self._platform_system,
            "platform.release": self._platform_release,
            "version_info": self._version_info,
            "anonymized_datasources": self._anonymized_datasources,
        }

    def build_message(self, record, payload):
        message = copy.deepcopy(record)
        message["event_time"] = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        message["data_context_id"] = self._data_context_id
        message["data_context_instance_id"] = self._data_context_instance_id
        message["ge_version"] = self._ge_version
        message["event_payload"] = payload
        return message

    def validate_record(self, record, schema):
        try:
            jsonschema.validate(record, schema=schema)
            return True
        except jsonschema.ValidationError as e:
            logger.debug("invalid record: " + str(e))
            return False

    def emit(self, record, payload_schema):
        """
        Emit a record.
        """
        if not self._enabled:
            return

        if not self.validate_record(record, payload_schema):
            return

        try:
            if payload_schema == usage_statistics_mini_payload_schema:
                payload = None
            else:
                payload = self.build_detailed_payload()
            message = self.build_message(record, payload)
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
        # TODO this implementation is very focused on objects
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


def usage_statistics_enabled_method(
        func=None,
        method_name=None,
        args_payload_fn=None,
        result_payload_fn=None,
        payload_schema=usage_statistics_mini_payload_schema
):
    """
    A decorator for usage statistics which defaults to the less detailed payload schema.
    """
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
                result = func(*args, **kwargs)
                # We try to get the handler only now, so that it *could* be initialized in func, e.g. if it is an
                # __init__ method
                handler = get_usage_statistics_handler(args)
                if result_payload_fn is not None:
                    nested_update(event_payload, result_payload_fn(result))
                record["success"] = True
                if handler is not None:
                    handler.emit(record, payload_schema)
            except Exception:
                record["success"] = False
                if handler:
                    handler.emit(record, payload_schema)
                raise

            return result

        return usage_statistics_wrapped_method
    else:
        def usage_statistics_wrapped_method_partial(func):
            return usage_statistics_enabled_method(
                func,
                method_name=method_name,
                args_payload_fn=args_payload_fn,
                result_payload_fn=result_payload_fn,
                payload_schema=payload_schema
            )
        return usage_statistics_wrapped_method_partial


def run_validation_operator_usage_statistics(
        data_context,  # self
        validation_operator_name,
        assets_to_validate,
        run_id=None,
        **kwargs
):
    try:
        data_context_id = data_context.data_context_id
    except AttributeError:
        data_context_id = None
    anonymizer = _anonymizers.get(data_context_id, None)
    if anonymizer is None:
        anonymizer = Anonymizer(data_context_id)
        _anonymizers[data_context_id] = anonymizer
    payload = {}
    try:
        payload["validation_operator_name"] = anonymizer.anonymize(validation_operator_name)
    except TypeError as e:
        logger.warning("run_validation_operator_usage_statistics: Unable to create validation_operator_name hash")
    try:
        payload["n_assets"] = len(assets_to_validate)
    except TypeError as e:
        logger.debug("run_validation_operator_usage_statistics: Unable to create n_assets payload field")
    return payload
