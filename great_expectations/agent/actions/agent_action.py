from abc import ABC, abstractmethod
from typing import Sequence

from pydantic import BaseModel

from great_expectations.agent.models import Event
from great_expectations.data_context import CloudDataContext


class CreatedResource(BaseModel):
    type: str
    id: str


class ActionResult(BaseModel):
    id: str
    type: str
    created_resources: Sequence[CreatedResource]


class AgentAction(ABC):
    def __init__(self, context: CloudDataContext):
        self._context = context

    @abstractmethod
    def run(self, event: Event, id: str) -> ActionResult:
        ...
