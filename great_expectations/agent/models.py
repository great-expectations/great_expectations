from typing import Literal, Sequence, Union
from uuid import UUID

from pydantic import BaseModel, Extra, Field
from typing_extensions import Annotated


class AgentBaseModel(BaseModel):
    class Config:
        extra: str = Extra.forbid


class EventBase(AgentBaseModel):
    type: str


class RunOnboardingDataAssistantEvent(EventBase):
    type: Literal[
        "onboarding_data_assistant_request.received"
    ] = "onboarding_data_assistant_request.received"
    datasource_name: str
    data_asset_name: str


class RunCheckpointEvent(EventBase):
    type: Literal["run_checkpoint_request.received"] = "run_checkpoint_request.received"


class UnknownEvent(EventBase):
    type: Literal["unknown_event"] = "unknown_event"


Event = Annotated[
    Union[RunOnboardingDataAssistantEvent, RunCheckpointEvent, UnknownEvent],
    Field(discriminator="type"),
]


class CreatedResource(AgentBaseModel):
    resource_id: UUID
    type: str


class JobStarted(AgentBaseModel):
    status: Literal["started"] = "started"


class JobCompleted(AgentBaseModel):
    status: Literal["complete"] = "complete"
    success: bool
    created_resources: Sequence[CreatedResource] = []
    error_stack_trace: Union[str, None] = None


JobStatus = Union[JobStarted, JobCompleted]
