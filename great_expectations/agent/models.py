from typing import Literal, Union
from uuid import UUID

from pydantic import BaseModel, Field
from typing_extensions import Annotated


class EventBase(BaseModel):
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


class Resource(BaseModel):
    resource_id: UUID
    type: str


class JobStarted(BaseModel):
    status: Literal["started"] = "started"


class JobCompleted(BaseModel):
    status: Literal["complete"] = "complete"
    success: bool
    created_resources: list[Resource] = []
    error_stack_trace: str | None = None
    # error_message: str | None = None


JobStatus = Union[JobStarted, JobCompleted]
