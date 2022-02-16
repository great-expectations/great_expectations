import atexit
import copy
import datetime
import enum
import json
import logging
import platform
import signal
import sys
import threading
import time
from functools import wraps
from queue import Queue
from typing import Optional

import jsonschema
import requests

from great_expectations import __version__ as ge_version
from great_expectations.core import ExpectationSuite
from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer
from great_expectations.core.usage_statistics.anonymizers.batch_anonymizer import (
    BatchAnonymizer,
)
from great_expectations.core.usage_statistics.anonymizers.batch_request_anonymizer import (
    BatchRequestAnonymizer,
)
from great_expectations.core.usage_statistics.anonymizers.checkpoint_run_anonymizer import (
    CheckpointRunAnonymizer,
)
from great_expectations.core.usage_statistics.anonymizers.data_docs_site_anonymizer import (
    DataDocsSiteAnonymizer,
)
from great_expectations.core.usage_statistics.anonymizers.datasource_anonymizer import (
    DatasourceAnonymizer,
)
from great_expectations.core.usage_statistics.anonymizers.execution_engine_anonymizer import (
    ExecutionEngineAnonymizer,
)
from great_expectations.core.usage_statistics.anonymizers.expectation_suite_anonymizer import (
    ExpectationSuiteAnonymizer,
)
from great_expectations.core.usage_statistics.anonymizers.store_anonymizer import (
    StoreAnonymizer,
)
from great_expectations.core.usage_statistics.anonymizers.types.base import (
    CLISuiteInteractiveFlagCombinations,
)
from great_expectations.core.usage_statistics.anonymizers.validation_operator_anonymizer import (
    ValidationOperatorAnonymizer,
)
from great_expectations.core.usage_statistics.schemas import (
    anonymized_usage_statistics_record_schema,
)
from great_expectations.core.util import nested_update
from great_expectations.rule_based_profiler.rule_based_profiler import RuleBasedProfiler

STOP_SIGNAL = object()

logger = logging.getLogger(__name__)

_anonymizers = {}


class UsageStatsExceptionPrefix(enum.Enum):
    EMIT_EXCEPTION = "UsageStatsException"
    INVALID_MESSAGE = "UsageStatsInvalidMessage"


class UsageStatisticsHandler:
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
        self._execution_engine_anonymizer = ExecutionEngineAnonymizer(data_context_id)
        self._store_anonymizer = StoreAnonymizer(data_context_id)
        self._validation_operator_anonymizer = ValidationOperatorAnonymizer(
            data_context_id
        )
        self._data_docs_sites_anonymizer = DataDocsSiteAnonymizer(data_context_id)
        self._batch_request_anonymizer = BatchRequestAnonymizer(data_context_id)
        self._batch_anonymizer = BatchAnonymizer(data_context_id)
        self._expectation_suite_anonymizer = ExpectationSuiteAnonymizer(data_context_id)
        self._checkpoint_run_anonymizer = CheckpointRunAnonymizer(data_context_id)
        try:
            self._sigterm_handler = signal.signal(signal.SIGTERM, self._teardown)
        except ValueError:
            # if we are not the main thread, we don't get to ask for signal handling.
            self._sigterm_handler = None
        try:
            self._sigint_handler = signal.signal(signal.SIGINT, self._teardown)
        except ValueError:
            # if we are not the main thread, we don't get to ask for signal handling.
            self._sigint_handler = None

        atexit.register(self._close_worker)

    def _teardown(self, signum: int, frame):
        self._close_worker()
        if signum == signal.SIGTERM and self._sigterm_handler:
            self._sigterm_handler(signum, frame)
        if signum == signal.SIGINT and self._sigint_handler:
            self._sigint_handler(signum, frame)

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
            try:
                res = session.post(self._url, json=message, timeout=2)
                logger.debug(
                    "Posted usage stats: message status " + str(res.status_code)
                )
                if res.status_code != 201:
                    logger.debug(
                        "Server rejected message: ", json.dumps(message, indent=2)
                    )
            except requests.exceptions.Timeout:
                logger.debug("Timeout while sending usage stats message.")
            except Exception as e:
                logger.debug("Unexpected error posting message: " + str(e))
            finally:
                self._message_queue.task_done()

    def build_init_payload(self):
        """Adds information that may be available only after full data context construction, but is useful to
        calculate only one time (for example, anonymization)."""
        expectation_suites = [
            self._data_context.get_expectation_suite(expectation_suite_name)
            for expectation_suite_name in self._data_context.list_expectation_suite_names()
        ]
        return {
            "platform.system": platform.system(),
            "platform.release": platform.release(),
            "version_info": str(sys.version_info),
            "anonymized_datasources": [
                self._datasource_anonymizer.anonymize_datasource_info(
                    datasource_name, datasource_config
                )
                for datasource_name, datasource_config in self._data_context.project_config_with_variables_substituted.datasources.items()
            ],
            "anonymized_stores": [
                self._store_anonymizer.anonymize_store_info(store_name, store_obj)
                for store_name, store_obj in self._data_context.stores.items()
            ],
            "anonymized_validation_operators": [
                self._validation_operator_anonymizer.anonymize_validation_operator_info(
                    validation_operator_name=validation_operator_name,
                    validation_operator_obj=validation_operator_obj,
                )
                for validation_operator_name, validation_operator_obj in self._data_context.validation_operators.items()
            ],
            "anonymized_data_docs_sites": [
                self._data_docs_sites_anonymizer.anonymize_data_docs_site_info(
                    site_name=site_name, site_config=site_config
                )
                for site_name, site_config in self._data_context.project_config_with_variables_substituted.data_docs_sites.items()
            ],
            "anonymized_expectation_suites": [
                self._expectation_suite_anonymizer.anonymize_expectation_suite_info(
                    expectation_suite
                )
                for expectation_suite in expectation_suites
            ],
        }

    def build_envelope(self, message):
        message["version"] = "1.0.0"
        message["ge_version"] = self._ge_version

        message["data_context_id"] = self._data_context_id
        message["data_context_instance_id"] = self._data_context_instance_id

        message["event_time"] = (
            datetime.datetime.now(datetime.timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S.%f"
            )[:-3]
            + "Z"
        )

        event_duration_property_name: str = f'{message["event"]}.duration'.replace(
            ".", "_"
        )
        if hasattr(self, event_duration_property_name):
            delta_t: int = getattr(self, event_duration_property_name)
            message["event_duration"] = delta_t

        return message

    @staticmethod
    def validate_message(message, schema):
        try:
            jsonschema.validate(message, schema=schema)
            return True
        except jsonschema.ValidationError as e:
            logger.debug(
                f"{UsageStatsExceptionPrefix.INVALID_MESSAGE.value} invalid message: "
                + str(e)
            )
            return False

    def send_usage_message(
        self,
        event: str,
        event_payload: Optional[dict] = None,
        success: Optional[bool] = None,
    ):
        """send a usage statistics message."""
        # noinspection PyBroadException
        try:
            message: dict = {
                "event": event,
                "event_payload": event_payload or {},
                "success": success,
            }
            self.emit(message)
        except Exception:
            pass

    def emit(self, message):
        """
        Emit a message.
        """
        try:
            if message["event"] == "data_context.__init__":
                message["event_payload"] = self.build_init_payload()
            message = self.build_envelope(message=message)
            if not self.validate_message(
                message, schema=anonymized_usage_statistics_record_schema
            ):
                return
            self._message_queue.put(message)
        # noinspection PyBroadException
        except Exception as e:
            # We *always* tolerate *any* error in usage statistics
            log_message: str = (
                f"{UsageStatsExceptionPrefix.EMIT_EXCEPTION.value}: {e} type: {type(e)}"
            )
            logger.debug(log_message)


def get_usage_statistics_handler(args_array):
    try:
        # If the object is usage_statistics-capable, then it will have a usage_statistics_handler
        handler = getattr(args_array[0], "_usage_statistics_handler", None)
        if handler is not None and not isinstance(handler, UsageStatisticsHandler):
            logger.debug("Invalid UsageStatisticsHandler found on object.")
            handler = None
    except IndexError:
        # A wrapped method that is not an object; this would be erroneous usage
        logger.debug(
            "usage_statistics enabled decorator should only be used on data context methods"
        )
        handler = None
    except AttributeError:
        # A wrapped method that is not usage_statistics capable
        handler = None
    except Exception as e:
        # An unknown error -- but we still fail silently
        logger.debug(
            "Unrecognized error when trying to find usage_statistics_handler: " + str(e)
        )
        handler = None
    return handler


def usage_statistics_enabled_method(
    func=None, event_name=None, args_payload_fn=None, result_payload_fn=None
):
    """
    A decorator for usage statistics which defaults to the less detailed payload schema.
    """
    if callable(func):
        if event_name is None:
            event_name = func.__name__

        @wraps(func)
        def usage_statistics_wrapped_method(*args, **kwargs):
            # if a function like `build_data_docs()` is being called as a `dry_run`
            # then we dont want to emit usage_statistics. We just return the function without sending a usage_stats message
            if "dry_run" in kwargs and kwargs["dry_run"]:
                return func(*args, **kwargs)
            # Set event_payload now so it can be updated below
            event_payload = {}
            message = {"event_payload": event_payload, "event": event_name}
            result = None
            time_begin: int = int(round(time.time() * 1000))
            try:
                if args_payload_fn is not None:
                    nested_update(event_payload, args_payload_fn(*args, **kwargs))

                result = func(*args, **kwargs)
                message["success"] = True
            except Exception:
                message["success"] = False
                raise
            finally:
                if not ((result is None) or (result_payload_fn is None)):
                    nested_update(event_payload, result_payload_fn(result))

                time_end: int = int(round(time.time() * 1000))
                delta_t: int = time_end - time_begin

                handler = get_usage_statistics_handler(args)
                if handler:
                    event_duration_property_name: str = (
                        f"{event_name}.duration".replace(".", "_")
                    )
                    setattr(handler, event_duration_property_name, delta_t)
                    handler.emit(message)
                    delattr(handler, event_duration_property_name)

            return result

        return usage_statistics_wrapped_method
    else:

        # noinspection PyShadowingNames
        def usage_statistics_wrapped_method_partial(func):
            return usage_statistics_enabled_method(
                func,
                event_name=event_name,
                args_payload_fn=args_payload_fn,
                result_payload_fn=result_payload_fn,
            )

        return usage_statistics_wrapped_method_partial


# noinspection PyUnusedLocal
def run_validation_operator_usage_statistics(
    data_context,
    validation_operator_name,
    assets_to_validate,
    **kwargs,
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
        payload["anonymized_operator_name"] = anonymizer.anonymize(
            validation_operator_name
        )
    except TypeError as e:
        logger.debug(
            f"{UsageStatsExceptionPrefix.EMIT_EXCEPTION.value}: {e} type: {type(e)}, run_validation_operator_usage_statistics: Unable to create validation_operator_name hash"
        )
    if data_context._usage_statistics_handler:
        # noinspection PyBroadException
        try:
            batch_anonymizer = data_context._usage_statistics_handler._batch_anonymizer
            payload["anonymized_batches"] = [
                batch_anonymizer.anonymize_batch_info(batch)
                for batch in assets_to_validate
            ]
        except Exception as e:
            logger.debug(
                f"{UsageStatsExceptionPrefix.EMIT_EXCEPTION.value}: {e} type: {type(e)}, run_validation_operator_usage_statistics: Unable to create anonymized_batches payload field"
            )

    return payload


# noinspection SpellCheckingInspection
# noinspection PyUnusedLocal
def save_expectation_suite_usage_statistics(
    data_context,
    expectation_suite,
    expectation_suite_name=None,
    **kwargs,
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

    if expectation_suite_name is None:
        if isinstance(expectation_suite, ExpectationSuite):
            expectation_suite_name = expectation_suite.expectation_suite_name
        elif isinstance(expectation_suite, dict):
            expectation_suite_name = expectation_suite.get("expectation_suite_name")

    # noinspection PyBroadException
    try:
        payload["anonymized_expectation_suite_name"] = anonymizer.anonymize(
            expectation_suite_name
        )
    except Exception as e:
        logger.debug(
            f"{UsageStatsExceptionPrefix.EMIT_EXCEPTION.value}: {e} type: {type(e)}, save_expectation_suite_usage_statistics: Unable to create anonymized_expectation_suite_name payload field"
        )

    return payload


def edit_expectation_suite_usage_statistics(
    data_context: "DataContext",  # noqa: F821
    expectation_suite_name: str,
    interactive_mode: Optional[CLISuiteInteractiveFlagCombinations] = None,
):
    try:
        data_context_id = data_context.data_context_id
    except AttributeError:
        data_context_id = None
    anonymizer = _anonymizers.get(data_context_id, None)
    if anonymizer is None:
        anonymizer = Anonymizer(data_context_id)
        _anonymizers[data_context_id] = anonymizer

    if interactive_mode is None:
        payload = {}
    else:
        payload = copy.deepcopy(interactive_mode.value)

    # noinspection PyBroadException
    try:
        payload["anonymized_expectation_suite_name"] = anonymizer.anonymize(
            expectation_suite_name
        )
    except Exception as e:
        logger.debug(
            f"{UsageStatsExceptionPrefix.EMIT_EXCEPTION.value}: {e} type: {type(e)}, edit_expectation_suite_usage_statistics: Unable to create anonymized_expectation_suite_name payload field"
        )

    return payload


def add_datasource_usage_statistics(data_context, name, **kwargs):
    if not data_context._usage_statistics_handler:
        return {}
    try:
        data_context_id = data_context.data_context_id
    except AttributeError:
        data_context_id = None

    # noinspection PyBroadException
    try:
        datasource_anonymizer = (
            data_context._usage_statistics_handler._datasource_anonymizer
        )
    except Exception:
        datasource_anonymizer = DatasourceAnonymizer(data_context_id)

    payload = {}
    # noinspection PyBroadException
    try:
        payload = datasource_anonymizer.anonymize_datasource_info(name, kwargs)
    except Exception as e:
        logger.debug(
            f"{UsageStatsExceptionPrefix.EMIT_EXCEPTION.value}: {e} type: {type(e)}, add_datasource_usage_statistics: Unable to create add_datasource_usage_statistics payload field"
        )

    return payload


# noinspection SpellCheckingInspection
def get_batch_list_usage_statistics(data_context, *args, **kwargs):
    try:
        data_context_id = data_context.data_context_id
    except AttributeError:
        data_context_id = None
    anonymizer = _anonymizers.get(data_context_id, None)
    if anonymizer is None:
        anonymizer = Anonymizer(data_context_id)
        _anonymizers[data_context_id] = anonymizer
    payload = {}

    if data_context._usage_statistics_handler:
        # noinspection PyBroadException
        try:
            batch_request_anonymizer: BatchRequestAnonymizer = (
                data_context._usage_statistics_handler._batch_request_anonymizer
            )
            payload = batch_request_anonymizer.anonymize_batch_request(*args, **kwargs)
        except Exception as e:
            logger.debug(
                f"{UsageStatsExceptionPrefix.EMIT_EXCEPTION.value}: {e} type: {type(e)}, get_batch_list_usage_statistics: Unable to create anonymized_batch_request payload field"
            )

    return payload


# noinspection PyUnusedLocal
def get_checkpoint_run_usage_statistics(checkpoint, *args, **kwargs):
    data_context_id: Optional[str] = None
    try:
        data_context_id = checkpoint.data_context.data_context_id
    except AttributeError:
        data_context_id = None

    anonymizer: Anonymizer = _anonymizers.get(data_context_id, None)
    if anonymizer is None:
        anonymizer = Anonymizer(data_context_id)
        _anonymizers[data_context_id] = anonymizer

    payload: dict = {}

    if checkpoint._usage_statistics_handler:
        # noinspection PyBroadException
        try:
            checkpoint_run_anonymizer: CheckpointRunAnonymizer = (
                checkpoint._usage_statistics_handler._checkpoint_run_anonymizer
            )

            resolved_runtime_kwargs: dict = (
                checkpoint_run_anonymizer.resolve_config_using_acceptable_arguments(
                    *(checkpoint,), **kwargs
                )
            )

            payload = checkpoint_run_anonymizer.anonymize_checkpoint_run(
                *(checkpoint,), **resolved_runtime_kwargs
            )
        except Exception as e:
            logger.debug(
                f"{UsageStatsExceptionPrefix.EMIT_EXCEPTION.value}: {e} type: {type(e)}, get_batch_list_usage_statistics: Unable to create anonymized_checkpoint_run payload field"
            )

    return payload


def get_profiler_run_usage_statistics(
    profiler: RuleBasedProfiler, *args, **kwargs
) -> dict:
    data_context_id: Optional[str] = None
    try:
        data_context_id = profiler._data_context.data_context_id
    except AttributeError:
        data_context_id = None

    anonymizer: Anonymizer = _anonymizers.get(data_context_id, None)
    if anonymizer is None:
        anonymizer = Anonymizer(data_context_id)
        _anonymizers[data_context_id] = anonymizer

    payload: dict = {}

    if profiler._usage_statistics_handler:
        # noinspection PyBroadException
        try:
            # profiler_run_anonymizer: ProfilerRunAnonymizer
            profiler_run_anonymizer = (
                profiler._usage_statistics_handler._profiler_run_anonymizer
            )

            resolved_runtime_kwargs: dict = (
                profiler_run_anonymizer.resolve_config_using_acceptable_arguments(
                    *(profiler,), **kwargs
                )
            )

            payload = profiler_run_anonymizer.anonymize_checkpoint_run(
                *(profiler,), **resolved_runtime_kwargs
            )
        except Exception as e:
            logger.debug(
                f"{UsageStatsExceptionPrefix.EMIT_EXCEPTION.value}: {e} type: {type(e)}, get_batch_list_usage_statistics: Unable to create anonymized_profiler_run payload field"
            )

    return payload


def send_usage_message(
    data_context: "DataContext",  # noqa: F821
    event: str,
    event_payload: Optional[dict] = None,
    success: Optional[bool] = None,
):
    """send a usage statistics message."""
    # noinspection PyBroadException
    try:
        handler: UsageStatisticsHandler = getattr(
            data_context, "_usage_statistics_handler", None
        )
        message: dict = {
            "event": event,
            "event_payload": event_payload,
            "success": success,
        }
        if handler is not None:
            handler.emit(message)
    except Exception:
        pass
