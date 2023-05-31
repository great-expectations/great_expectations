from abc import abstractmethod
from typing import Generic, Sequence, TypeVar

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


_TEvent = TypeVar("_TEvent", bound=Event)


class AgentAction(Generic[_TEvent]):
    def __init__(self, context: CloudDataContext):
        self._context = context

    @abstractmethod
    def run(self, event: _TEvent, id: str) -> ActionResult:
        ...
