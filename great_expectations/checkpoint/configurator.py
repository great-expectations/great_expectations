from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, ClassVar, Sequence

from typing_extensions import TypedDict

if TYPE_CHECKING:
    from typing_extensions import NotRequired

logger = logging.getLogger(__name__)


class ActionDetails(TypedDict):
    class_name: str
    site_names: NotRequired[list[str]]
    slack_webhook: NotRequired[Any]
    notify_on: NotRequired[Any]
    notify_with: NotRequired[list[str] | str | None]
    renderer: NotRequired[dict]


class ActionDict(TypedDict):
    name: str
    action: ActionDetails


class ActionDicts:
    STORE_VALIDATION_RESULT: ClassVar[ActionDict] = {
        "name": "store_validation_result",
        "action": {"class_name": "StoreValidationResultAction"},
    }
    UPDATE_DATA_DOCS: ClassVar[ActionDict] = {
        "name": "update_data_docs",
        "action": {"class_name": "UpdateDataDocsAction"},
    }
    DEFAULT_ACTION_LIST: ClassVar[Sequence[ActionDict]] = (
        STORE_VALIDATION_RESULT,
        UPDATE_DATA_DOCS,
    )

    @staticmethod
    def build_slack_action(webhook, notify_on, notify_with) -> ActionDict:
        return {
            "name": "send_slack_notification",
            "action": {
                "class_name": "SlackNotificationAction",
                "slack_webhook": webhook,
                "notify_on": notify_on,
                "notify_with": notify_with,
                "renderer": {
                    "module_name": "great_expectations.render.renderer.slack_renderer",
                    "class_name": "SlackRenderer",
                },
            },
        }
