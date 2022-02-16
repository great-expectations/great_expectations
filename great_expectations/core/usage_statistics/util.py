import logging
from typing import Optional

from great_expectations.core.usage_statistics.usage_statistics import (
    send_usage_message as send_usage_stats_message,
)
from great_expectations.data_context.data_context import BaseDataContext

logger = logging.getLogger(__name__)


def send_usage_message(
    data_context: Optional[BaseDataContext],
    event: str,
    event_payload: Optional[dict] = None,
    api_version: str = "v3",
    success: bool = False,
):
    if not event:
        event = None

    if not ((event is None) or (data_context is None)):
        if event_payload is None:
            event_payload = {}

        event_payload.update({"api_version": api_version})

        send_usage_stats_message(
            data_context=data_context,
            event=event,
            event_payload=event_payload,
            success=success,
        )
