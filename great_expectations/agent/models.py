from typing import Literal, Union
from typing_extensions import Annotated

from pydantic import BaseModel, Field


class EventBase(BaseModel):
    type: str


class RunOnboardingDataAssistantEvent(EventBase):
    type: Literal["run-onboarding-data-assistant"] = "run-onboarding-data-assistant"
    datasource_name: str
    data_asset_name: str


class ShutdownEvent(EventBase):
    type: Literal["shutdown"] = "shutdown"


Event = Annotated[
    Union[RunOnboardingDataAssistantEvent, ShutdownEvent], Field(discriminator="type")
]
