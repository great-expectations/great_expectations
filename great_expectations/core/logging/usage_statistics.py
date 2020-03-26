import datetime
import logging
import requests
import sys
import platform
import threading
from queue import Queue
import atexit
import signal

import jsonschema

from functools import wraps

from great_expectations import __version__ as ge_version
from great_expectations.core import nested_update
from great_expectations.core.logging.anonymizer import Anonymizer
from great_expectations.core.logging.schemas import usage_statistics_record_schema
from great_expectations.datasource.datasource_anonymizer import DatasourceAnonymizer

STOP_SIGNAL = object()

logger = logging.getLogger(__name__)

_anonymizers = dict()


class UsageStatisticsHandler(object):

    def __init__(self, data_context, data_context_id, usage_statistics_url):
        self._url = usage_statistics_url

        self._data_context_id = data_context_id
        self._data_context_instance_id = data_context.instance_id
        self._data_context = data_context
        self._ge_version = ge_version

        self._message_queue = Queue()
        self._worker = threading.Thread(target=self._requests_worker, daemon=True)
        self._worker.start()
        self._datasource_anonymizer = DatasourceAnonymizer(data_context_id)
        self._sigterm_handler = signal.signal(signal.SIGTERM, self._teardown)
        self._sigint_handler = signal.signal(signal.SIGINT, self._teardown)
        self._sigquit_handler = signal.signal(signal.SIGQUIT, self._teardown)
        atexit.register(self._close_worker)

    def _teardown(self, signum: int, frame):
        self._close_worker()
        if signum == signal.SIGTERM:
            self._sigterm_handler(signum, frame)
        elif signum == signal.SIGINT:
            self._sigint_handler(signum, frame)
        elif signum == signal.SIGQUIT:
            self._sigquit_handler(signum, frame)

    def _close_worker(self):
        self._message_queue.put(STOP_SIGNAL)
        self._worker.join()

    def _requests_worker(self):
        session = requests.Session()
        while True:
            message = self._message_queue.get()
            if message == STOP_SIGNAL:
                self._message_queue.task_done()
                return
            res = session.post(self._url, json=message)
            logger.debug("usage stats message status: " + str(res.status_code))
            self._message_queue.task_done()

    def build_init_payload(self):
        """Adds information that may be available only after full data context construction, but is useful to
        calculate only one time (for example, anonymization)."""
        return {
            "platform.system": platform.system(),
            "platform.release": platform.release(),
            "version_info": str(sys.version_info),
            "anonymized_datasources": [
                self._datasource_anonymizer.anonymize_datasource_info(datasource)
                for datasource in self._data_context.datasources.values()
            ],
        }

    def build_envelope(self, message):
        message["event_time"] = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        message["data_context_id"] = self._data_context_id
        message["data_context_instance_id"] = self._data_context_instance_id
        message["ge_version"] = self._ge_version
        return message

    def validate_message(self, payload, schema):
        try:
            jsonschema.validate(payload, schema=schema)
            return True
        except jsonschema.ValidationError as e:
            logger.debug("invalid message: " + str(e))
            return False

    def emit(self, message, payload_schema):
        """
        Emit a message.
        """
        try:
            if payload_schema == init_payload_schema:
                message["event_payload"] = self.build_init_payload()

            if not self.validate_message(message["event_payload"], payload_schema):
                return

            message = self.build_envelope(message)
            if not self.validate_message(message, schema=usage_statistics_record_schema):
                return

            self._message_queue.put(message)
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
        # A wrapped method that is not an object; this would be erroneous usage
        logger.debug("usage_statistics enabled decorator should only be used on data context methods")
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
        payload_schema=None,
):
    """
    A decorator for usage statistics which defaults to the less detailed payload schema.
    """
    if callable(func):
        if method_name is None:
            method_name = func.__name__

        if payload_schema is None:
            # TESTING/DEV ONLY
            payload_schema = {"schema": {}}

        @wraps(func)
        def usage_statistics_wrapped_method(*args, **kwargs):
            # Set event_payload now so it can be updated below
            event_payload = {}
            message = {"event_payload": event_payload, "method": method_name}
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
                message["success"] = True
                if handler is not None:
                    handler.emit(message, payload_schema)
            except Exception:
                message["success"] = False
                if handler:
                    handler.emit(message, payload_schema)
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


def send_usage_message(data_context, method, event_payload=None, success=None, payload_schema=None):
    """send a usage statistics message."""
    try:
        handler = getattr(data_context, "_usage_statistics_handler", None)
        message = {
            "method": method,
            "event_payload": event_payload or {},
            "success": success,
        }
        if handler is not None:
            handler.emit(message, payload_schema)
    except Exception:
        pass
