from typing import ClassVar, List

from great_expectations.analytics.actions import DATA_CONTEXT_INITIALIZED
from great_expectations.analytics.base_event import Action, Event


class DataContextInitializedEvent(Event):
    action = DATA_CONTEXT_INITIALIZED

    _allowed_actions: ClassVar[List[Action]] = [DATA_CONTEXT_INITIALIZED]

    def __init__(self):
        super().__init__(action=self.action)
