from typing import Literal, Union

from pydantic import BaseModel, Field
from typing_extensions import Annotated


class EventBase(BaseModel):
    type: str


class RunDataAssistantEvent(EventBase):
    type: Literal[
        "onboarding_data_assistant_request.received"
    ] = "onboarding_data_assistant_request.received"


class RunCheckpointEvent(EventBase):
    type: Literal["run_checkpoint_request.received"] = "run_checkpoint_request.received"


Event = Annotated[
    Union[RunDataAssistantEvent, RunCheckpointEvent], Field(discriminator="type")
]
