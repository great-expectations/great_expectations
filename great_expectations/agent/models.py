from typing import Literal, Sequence, Union

from pydantic import BaseModel, Extra, Field
from typing_extensions import Annotated

from great_expectations.experimental.column_descriptive_metrics.batch_inspector_agent_action import (
    RunBatchInspectorEvent,
)


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


# TODO: Can these Events be registered or should they be hardcoded here?

Event = Annotated[
    Union[
        RunOnboardingDataAssistantEvent,
        RunCheckpointEvent,
        RunBatchInspectorEvent,
        UnknownEvent,
    ],
    Field(discriminator="type"),
]


class CreatedResource(AgentBaseModel):
    resource_id: str
    type: str


class JobStarted(AgentBaseModel):
    status: Literal["started"] = "started"


class JobCompleted(AgentBaseModel):
    status: Literal["completed"] = "completed"
    success: bool
    created_resources: Sequence[CreatedResource] = []
    error_stack_trace: Union[str, None] = None


JobStatus = Union[JobStarted, JobCompleted]
