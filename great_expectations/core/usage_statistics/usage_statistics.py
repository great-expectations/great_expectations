from __future__ import annotations

import atexit
import copy
import enum
import json
import logging
import signal
import threading
import time
from functools import wraps
from queue import Queue
from typing import TYPE_CHECKING, Callable

import jsonschema
import requests

from great_expectations import __version__ as gx_version
from great_expectations.core import ExpectationSuite
from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer
from great_expectations.core.usage_statistics.payload_builder import (
    UsageStatisticsPayloadBuilder,
)
from great_expectations.core.usage_statistics.schemas import (
    anonymized_usage_statistics_record_schema,
)
from great_expectations.core.util import nested_update
from great_expectations.data_context.types.base import CheckpointConfig
from great_expectations.rule_based_profiler.config import RuleBasedProfilerConfig

if TYPE_CHECKING:
    import uuid
    from types import FrameType

    from great_expectations.checkpoint.checkpoint import Checkpoint
    from great_expectations.core.usage_statistics.anonymizers.types.base import (
        CLISuiteInteractiveFlagCombinations,
    )
    from great_expectations.core.usage_statistics.events import UsageStatsEvents
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.datasource import LegacyDatasource
    from great_expectations.datasource.new_datasource import BaseDatasource
    from great_expectations.rule_based_profiler.rule_based_profiler import (
        RuleBasedProfiler,
    )


STOP_SIGNAL = object()

logger = logging.getLogger(__name__)

_anonymizers: dict[str | None, Anonymizer] = {}


class UsageStatsExceptionPrefix(enum.Enum):
    EMIT_EXCEPTION = "UsageStatsException"
    INVALID_MESSAGE = "UsageStatsInvalidMessage"


class UsageStatisticsHandler:
    """
    Orchestrates between the UsageStatisticsPayloadBuilder and Anonymizer to construct
    payloads, obfuscate them, and emit them to a target endpoint.
    """

    def __init__(
        self,
        data_context: AbstractDataContext,
        data_context_id: str,
        usage_statistics_url: str,
        oss_id: uuid.UUID | None,
    ) -> None:
        self._url = usage_statistics_url

        self._data_context_id = data_context_id
        self._builder = UsageStatisticsPayloadBuilder(
            data_context=data_context,
            data_context_id=data_context_id,
            oss_id=oss_id,
            gx_version=gx_version,
        )

        self._message_queue: Queue = Queue()
        self._worker = threading.Thread(target=self._requests_worker, daemon=True)
        self._worker.start()

        self._anonymizer = Anonymizer(data_context_id)

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

    @property
    def anonymizer(self) -> Anonymizer:
        return self._anonymizer

    def _teardown(self, signum: int, frame: FrameType | None) -> None:
        self._close_worker()
        if signum == signal.SIGTERM and self._sigterm_handler:
            self._sigterm_handler(signum, frame)  # type: ignore[operator] # mypy thinks signal.signal() returns an int
        if signum == signal.SIGINT and self._sigint_handler:
            self._sigint_handler(signum, frame)  # type: ignore[operator] # mypy thinks signal.signal() returns an int

    def _close_worker(self) -> None:
        self._message_queue.put(STOP_SIGNAL)
        self._worker.join()

    def _requests_worker(self) -> None:
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
                if res.status_code != 201:  # noqa: PLR2004
                    logger.debug(  # noqa: PLE1205
                        "Server rejected message: ", json.dumps(message, indent=2)
                    )
            except requests.exceptions.Timeout:
                logger.debug("Timeout while sending usage stats message.")
            except Exception as e:
                logger.debug("Unexpected error posting message: " + str(e))
            finally:
                self._message_queue.task_done()

    @staticmethod
    def validate_message(message: dict, schema: dict) -> bool:
        try:
            jsonschema.validators.Draft202012Validator(
                schema=schema,
                format_checker=jsonschema.validators.Draft202012Validator.FORMAT_CHECKER,
            ).validate(message)
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
        event_payload: dict | None = None,
        success: bool | None = None,
    ) -> None:
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

    def emit(self, message: dict) -> None:
        """
        Emit a message.
        """
        try:
            if message["event"] == "data_context.__init__":
                init_payload = self._builder.build_init_payload()
                message["event_payload"] = self._anonymizer.anonymize_init_payload(
                    init_payload
                )
            message = self._builder.build_envelope(message=message)
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


def get_usage_statistics_handler(args_array: list) -> UsageStatisticsHandler | None:
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


# Mapping between method's qualified name and the event name it emits
# Used to ensure proper usage stats coverage in tests
ENABLED_METHODS: dict[str, UsageStatsEvents] = {}


def usage_statistics_enabled_method(
    func: Callable | None = None,
    event_name: UsageStatsEvents | None = None,
    args_payload_fn: Callable | None = None,
    result_payload_fn: Callable | None = None,
) -> Callable:
    """
    A decorator for usage statistics which defaults to the less detailed payload schema.
    """
    if func and event_name:
        ENABLED_METHODS[func.__qualname__] = event_name
    if callable(func):
        if event_name is None:
            event_name = func.__name__  # type: ignore[assignment] # Generate a useful name but not part of UsageStatsEvents enum

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
                    args_payload = args_payload_fn(*args, **kwargs) or {}
                    nested_update(event_payload, args_payload)

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

                handler = get_usage_statistics_handler(list(args))
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
    data_context: AbstractDataContext,
    validation_operator_name: str,
    assets_to_validate: list,
    **kwargs,
) -> dict:
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
            obj=validation_operator_name
        )
    except TypeError as e:
        logger.debug(
            f"{UsageStatsExceptionPrefix.EMIT_EXCEPTION.value}: {e} type: {type(e)}, run_validation_operator_usage_statistics: Unable to create validation_operator_name hash"
        )
    if data_context.usage_statistics_handler:
        # noinspection PyBroadException
        try:
            anonymizer = data_context.usage_statistics_handler.anonymizer
            payload["anonymized_batches"] = [
                anonymizer.anonymize(obj=batch) for batch in assets_to_validate
            ]
        except Exception as e:
            logger.debug(
                f"{UsageStatsExceptionPrefix.EMIT_EXCEPTION.value}: {e} type: {type(e)}, run_validation_operator_usage_statistics: Unable to create anonymized_batches payload field"
            )

    return payload


# noinspection SpellCheckingInspection
# noinspection PyUnusedLocal
def save_expectation_suite_usage_statistics(
    data_context: AbstractDataContext,
    expectation_suite: ExpectationSuite | None = None,
    expectation_suite_name: str | None = None,
    **kwargs: dict,
) -> dict:
    """
    Event handler for saving expectation suite with either "ExpectationSuite" object or "expectation_suite_name" string.
    """
    return _handle_expectation_suite_usage_statistics(
        data_context=data_context,
        event_arguments_payload_handler_name="save_expectation_suite_usage_statistics",
        expectation_suite=expectation_suite,
        expectation_suite_name=expectation_suite_name,
        interactive_mode=None,
        **kwargs,
    )


def get_expectation_suite_usage_statistics(
    data_context: AbstractDataContext,
    expectation_suite_name: str,
    **kwargs: dict,
) -> dict:
    """
    Event handler for obtaining expectation suite with "expectation_suite_name" string.
    """
    return _handle_expectation_suite_usage_statistics(
        data_context=data_context,
        event_arguments_payload_handler_name="get_expectation_suite_usage_statistics",
        expectation_suite=None,
        expectation_suite_name=expectation_suite_name,
        interactive_mode=None,
        **kwargs,
    )


def edit_expectation_suite_usage_statistics(
    data_context: AbstractDataContext,
    expectation_suite_name: str,
    interactive_mode: CLISuiteInteractiveFlagCombinations | None = None,
    **kwargs: dict,
) -> dict:
    """
    Event handler for editing expectation suite with "expectation_suite_name" string.
    """
    return _handle_expectation_suite_usage_statistics(
        data_context=data_context,
        event_arguments_payload_handler_name="edit_expectation_suite_usage_statistics",
        expectation_suite=None,
        expectation_suite_name=expectation_suite_name,
        interactive_mode=interactive_mode,
        **kwargs,
    )


def add_datasource_usage_statistics(
    data_context: AbstractDataContext,
    name: str | None = None,
    datasource: LegacyDatasource | BaseDatasource | None = None,
    **kwargs,
) -> dict:
    if not data_context.usage_statistics_handler:
        return {}
    try:
        data_context_id = data_context.data_context_id
    except AttributeError:
        data_context_id = None

    from great_expectations.core.usage_statistics.anonymizers.datasource_anonymizer import (
        DatasourceAnonymizer,
    )

    aggregate_anonymizer = Anonymizer(salt=data_context_id)
    datasource_anonymizer = DatasourceAnonymizer(
        salt=data_context_id, aggregate_anonymizer=aggregate_anonymizer
    )

    payload = {}
    # noinspection PyBroadException
    try:
        if not name and datasource:
            name = datasource.name
        assert (
            name
        ), "Guaranteed to have name from either `name` or `datasource` input args"

        payload = datasource_anonymizer._anonymize_datasource_info(name, kwargs)
    except Exception as e:
        logger.debug(
            f"{UsageStatsExceptionPrefix.EMIT_EXCEPTION.value}: {e} type: {type(e)}, add_datasource_usage_statistics: Unable to create add_datasource_usage_statistics payload field"
        )

    return payload


# noinspection SpellCheckingInspection
def get_batch_list_usage_statistics(
    data_context: AbstractDataContext, *args, **kwargs
) -> dict:
    try:
        data_context_id = data_context.data_context_id
    except AttributeError:
        data_context_id = None

    anonymizer = _anonymizers.get(data_context_id, None)
    if anonymizer is None:
        anonymizer = Anonymizer(data_context_id)
        _anonymizers[data_context_id] = anonymizer

    payload = {}

    if data_context.usage_statistics_handler:
        # noinspection PyBroadException
        try:
            anonymizer = data_context.usage_statistics_handler.anonymizer
            payload = anonymizer.anonymize(*args, **kwargs)
        except Exception as e:
            logger.debug(
                f"{UsageStatsExceptionPrefix.EMIT_EXCEPTION.value}: {e} type: {type(e)}, get_batch_list_usage_statistics: Unable to create anonymized_batch_request payload field"
            )

    return payload


# noinspection PyUnusedLocal
def get_checkpoint_run_usage_statistics(
    checkpoint: Checkpoint,
    *args,
    **kwargs,
) -> dict:
    usage_statistics_handler: UsageStatisticsHandler | None = (
        checkpoint._usage_statistics_handler
    )

    data_context_id: str | None = None
    try:
        data_context_id = checkpoint.data_context.data_context_id
    except AttributeError:
        data_context_id = None

    anonymizer = _anonymizers.get(data_context_id, None)
    if anonymizer is None:
        anonymizer = Anonymizer(data_context_id)
        _anonymizers[data_context_id] = anonymizer

    payload = {}

    if usage_statistics_handler:
        # noinspection PyBroadException
        try:
            anonymizer = usage_statistics_handler.anonymizer

            resolved_runtime_kwargs: dict = (
                CheckpointConfig.resolve_config_using_acceptable_arguments(
                    *(checkpoint,), **kwargs
                )
            )

            payload = anonymizer.anonymize(*(checkpoint,), **resolved_runtime_kwargs)
        except Exception as e:
            logger.debug(
                f"{UsageStatsExceptionPrefix.EMIT_EXCEPTION.value}: {e} type: {type(e)}, get_checkpoint_run_usage_statistics: Unable to create anonymized_checkpoint_run payload field"
            )

    return payload


# noinspection PyUnusedLocal
def get_profiler_run_usage_statistics(
    profiler: RuleBasedProfiler,
    variables: dict | None = None,
    rules: dict | None = None,
    *args: tuple,
    **kwargs: dict,
) -> dict:
    usage_statistics_handler: UsageStatisticsHandler | None = (
        profiler._usage_statistics_handler
    )

    data_context_id: str | None = None
    if usage_statistics_handler:
        data_context_id = usage_statistics_handler._data_context_id

    anonymizer = _anonymizers.get(data_context_id, None)
    if anonymizer is None:
        anonymizer = Anonymizer(data_context_id)
        _anonymizers[data_context_id] = anonymizer

    payload = {}

    if usage_statistics_handler:
        # noinspection PyBroadException
        try:
            anonymizer = usage_statistics_handler.anonymizer

            resolved_runtime_config: RuleBasedProfilerConfig = (
                RuleBasedProfilerConfig.resolve_config_using_acceptable_arguments(
                    profiler=profiler,
                    variables=variables,
                    rules=rules,
                )
            )

            payload = anonymizer.anonymize(obj=resolved_runtime_config)
        except Exception as e:
            logger.debug(
                f"{UsageStatsExceptionPrefix.EMIT_EXCEPTION.value}: {e} type: {type(e)}, get_profiler_run_usage_statistics: Unable to create anonymized_profiler_run payload field"
            )

    return payload


def send_usage_message(
    data_context: AbstractDataContext,
    event: str,
    event_payload: dict | None = None,
    success: bool | None = None,
) -> None:
    """send a usage statistics message."""
    # noinspection PyBroadException
    try:
        handler = data_context.usage_statistics_handler
        if handler is not None:
            message: dict = {
                "event": event,
                "event_payload": event_payload,
                "success": success,
            }
            handler.emit(message)
    except Exception:
        pass


def send_usage_message_from_handler(
    event: str,
    handler: UsageStatisticsHandler | None = None,
    event_payload: dict | None = None,
    success: bool | None = None,
) -> None:
    """Send a usage statistics message using an already instantiated handler."""
    # noinspection PyBroadException
    try:
        if handler:
            message: dict = {
                "event": event,
                "event_payload": event_payload,
                "success": success,
            }
            handler.emit(message)
    except Exception as e:
        logger.debug(
            f"{UsageStatsExceptionPrefix.EMIT_EXCEPTION.value}: {e} type: {type(e)}, Exception encountered while running send_usage_message_from_handler()."
        )


# noinspection SpellCheckingInspection
# noinspection PyUnusedLocal
def _handle_expectation_suite_usage_statistics(
    data_context: AbstractDataContext,
    event_arguments_payload_handler_name: str,
    expectation_suite: ExpectationSuite | None = None,
    expectation_suite_name: str | None = None,
    interactive_mode: CLISuiteInteractiveFlagCombinations | None = None,
    **kwargs,
) -> dict:
    """
    This method anonymizes "expectation_suite_name" for events that utilize this property.
    """
    data_context_id: str | None
    try:
        data_context_id = data_context.data_context_id
    except AttributeError:
        data_context_id = None

    anonymizer = _anonymizers.get(data_context_id, None)
    if anonymizer is None:
        anonymizer = Anonymizer(data_context_id)
        _anonymizers[data_context_id] = anonymizer

    payload: dict

    if interactive_mode is None:
        payload = {}
    else:
        payload = copy.deepcopy(interactive_mode.value)

    assert not (
        expectation_suite_name is None and expectation_suite is None
    ), "Guaranteed to have at least one of these values from context CRUD"

    if expectation_suite_name is None:
        if isinstance(expectation_suite, ExpectationSuite):
            expectation_suite_name = expectation_suite.expectation_suite_name
        elif isinstance(expectation_suite, dict):
            expectation_suite_name = expectation_suite.get("expectation_suite_name")

    # noinspection PyBroadException
    try:
        payload["anonymized_expectation_suite_name"] = anonymizer.anonymize(
            obj=expectation_suite_name
        )
    except Exception as e:
        logger.debug(
            f"{UsageStatsExceptionPrefix.EMIT_EXCEPTION.value}: {e} type: {type(e)}, {event_arguments_payload_handler_name}: Unable to create anonymized_expectation_suite_name payload field."
        )

    return payload
