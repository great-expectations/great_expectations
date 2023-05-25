from typing import Literal, Union

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


Event = Annotated[
    Union[RunOnboardingDataAssistantEvent, RunCheckpointEvent], Field(discriminator="type")
]
