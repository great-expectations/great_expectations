from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List, Optional, Set, Union

if TYPE_CHECKING:
    from great_expectations.data_context import AbstractDataContext


logger = logging.getLogger(__name__)


def send_usage_message(
    data_context: Optional[AbstractDataContext],
    event: Union[str, None],
    event_payload: Optional[dict] = None,
    api_version: str = "v3",
    success: bool = False,
) -> None:
    if not event:
        event = None

    if not ((event is None) or (data_context is None)):
        if event_payload is None:
            event_payload = {}

        event_payload.update({"api_version": api_version})

        from great_expectations.core.usage_statistics.usage_statistics import (
            send_usage_message as send_usage_stats_message,
        )

        send_usage_stats_message(
            data_context=data_context,
            event=event,
            event_payload=event_payload,
            success=success,
        )


def aggregate_all_core_expectation_types() -> Set[str]:
    from great_expectations.dataset.dataset import Dataset
    from great_expectations.expectations.registry import (
        list_registered_expectation_implementations,
    )

    v2_batchkwargs_api_supported_expectation_types: List[str] = [
        el for el in Dataset.__dict__.keys() if el.startswith("expect_")
    ]

    v3_batchrequest_api_supported_expectation_types: List[
        str
    ] = list_registered_expectation_implementations()

    return set(v2_batchkwargs_api_supported_expectation_types).union(
        set(v3_batchrequest_api_supported_expectation_types)
    )
