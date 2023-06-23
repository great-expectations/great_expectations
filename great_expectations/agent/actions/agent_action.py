from abc import abstractmethod
from typing import Generic, Optional, Sequence, TypeVar

from pydantic import BaseModel

from great_expectations.agent.models import CreatedResource, Event
from great_expectations.data_context import CloudDataContext


class ActionResult(BaseModel):
    id: str
    type: str
    created_resources: Sequence[CreatedResource]
    details: Optional[Sequence[str]] = None


_EventT = TypeVar("_EventT", bound=Event)


class AgentAction(Generic[_EventT]):
    def __init__(self, context: CloudDataContext):
        self._context = context

    @abstractmethod
    def run(self, event: _EventT, id: str) -> ActionResult:
        ...
