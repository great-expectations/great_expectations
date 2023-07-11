from abc import abstractmethod
from typing import Generic, Sequence, TypeVar

from pydantic import BaseModel

from great_expectations.agent.agent_server_session import AgentServerSession
from great_expectations.agent.models import CreatedResource, Event
from great_expectations.data_context import CloudDataContext


class ActionResult(BaseModel):
    id: str
    type: str
    created_resources: Sequence[CreatedResource]


_EventT = TypeVar("_EventT", bound=Event)


class AgentAction(Generic[_EventT]):
    def __init__(
        self, context: CloudDataContext, agent_server_session: AgentServerSession
    ):
        self._context = context
        self._agent_server_session = agent_server_session

    @abstractmethod
    def run(self, event: _EventT, id: str) -> ActionResult:
        ...
