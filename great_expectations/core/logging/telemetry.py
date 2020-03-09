import logging
import requests
import sys
import platform
import time
from hashlib import md5
from functools import wraps

from great_expectations import __version__ as ge_version
from great_expectations.core import nested_update
from great_expectations.datasource import anonymize_datasource_class_name

DEFAULT_TELEMETRY_URL = "https://xvqc3q1sdj.execute-api.us-east-1.amazonaws.com/prod/great_expectations/v1/telemetry"
TELEMETRY_DEV_URL = "https://lq3ydlmxy5.execute-api.us-east-1.amazonaws.com/dev/great_expectations/v1/telemetry"
logger = logging.getLogger(__name__)
telemetry_logger = logging.getLogger("great_expectations.telemetry")
telemetry_logger.setLevel(logging.INFO)


class DataContextLoggingFilter(logging.Filter):
    """This Logging filter compute expensive information that will be applied to each telemetry message only once,
    adds it to each record, ensures that proper anonymization has taken place, and filters records to only apply
    allow telemetry-specific messages through."""
    def __init__(self, data_context, data_context_id):
        self._data_context_id = data_context_id
        self._data_context_instance_id = data_context.instance_id
        self._platform_system = platform.system()
        self._platform_release = platform.release()
        self._version_info = sys.version_info
        self._data_context = data_context
        self._ge_version = ge_version
        self._anonymized_datasources = []

    def register_telemetry_details(self):
        """Adds information that may be available only after full data context construction, but is useful to
        calculate only one time (for example, anonymization)."""
        self._anonymized_datasources = [
            anonymize_datasource_class_name(datasource["class_name"], datasource["module_name"])
            for datasource in self._data_context.list_datasources()
        ]

    def filter(self, record: logging.LogRecord) -> int:
        if record.__dict__.get("telemetry") is True:
            record.__dict__["data_context_id"] = self._data_context_id
            record.__dict__["data_context_instance_id"] = self._data_context_instance_id
            record.__dict__["ge_version"] = self._ge_version
            record.__dict__["platform.system"] = self._platform_system
            record.__dict__["platform.release"] = self._platform_release
            record.__dict__["version_info"] = self._version_info
            record.__dict__["anonymized_datasources"] = self._anonymized_datasources
            return True
        return False


class TelemetryRecordFormatter(logging.Formatter):
    """Produce JSON-formatted output for a telemetry-based log record."""
    converter = time.gmtime
    default_time_format = "%Y-%m-%dT%H:%M:%S"
    default_msec_format = "%s.%03dZ"

    def format(self, record):
        return {
            "event_time": self.formatTime(record=record),
            "data_context_id": record.__dict__.get("data_context_id"),
            "data_context_instance_id": record.__dict__.get("data_context_instance_id"),
            "ge_version": record.__dict__.get("ge_version"),
            "method": record.msg,
            "success": record.__dict__.get("success", None),
            "platform.system": record.__dict__.get("platform.system", None),
            "platform.release": record.__dict__.get("platform.release", None),
            "version_info": record.__dict__.get("version_info", None),
            "anonymized_datasources": record.__dict__.get("anonymized_datasources", None),
            "event_payload": record.__dict__.get("event_payload", None)
        }


class HTTPDataHandler(logging.Handler):
    def __init__(self, url):
        super(HTTPDataHandler, self).__init__()
        self._url = url

    def emit(self, record):
        """
        Emit a record.
        """
        try:
            requests.post(self._url, json=self.format(record))
        # noinspection PyBroadException
        except Exception:
            self.handleError(record)


def telemetry_enabled_method(func=None, method_name=None, args_payload_fn=None, result_payload_fn=None):
    if callable(func):
        if method_name is None:
            method_name = func.__name__

        @wraps(func)
        def telemetry_wrapped_method(*args, **kwargs):
            event_payload = {}
            extra = {"telemetry": True, "event_payload": event_payload}
            try:
                if args_payload_fn is not None:
                    nested_update(event_payload, args_payload_fn(*args, **kwargs))
                res = func(*args, **kwargs)
                if result_payload_fn is not None:
                    nested_update(event_payload, result_payload_fn(res))
                extra["success"] = True
                telemetry_logger.info(method_name, extra=extra)
                return res
            except Exception:
                extra["success"] = False
                telemetry_logger.info(method_name, extra=extra)
                raise

        return telemetry_wrapped_method
    else:
        def telemetry_wrapped_method_partial(func):
            return telemetry_enabled_method(func,
                                            method_name=method_name,
                                            args_payload_fn=args_payload_fn,
                                            result_payload_fn=result_payload_fn)
        return telemetry_wrapped_method_partial


def run_validation_operator_telemetry(
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
        logger.warning("run_validation_operator_telemetry: Unable to create validation_operator_name hash")
    try:
        payload["n_assets"] = len(assets_to_validate)
    except TypeError as e:
        logger.debug("run_validation_operator_telemetry: Unable to create n_assets payload field")
    return payload
