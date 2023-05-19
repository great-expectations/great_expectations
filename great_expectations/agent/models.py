from typing import Literal, Union
from typing_extensions import Annotated

from pydantic import BaseModel, Field
from typing_extensions import Annotated


class EventBase(BaseModel):
    type: str


class RunOnboardingDataAssistantEvent(EventBase):
    type: Literal["onboarding_data_assistant_request.received"] = "onboarding_data_assistant_request.received"
    datasource_name: str
    data_asset_name: str

class ShutdownEvent(EventBase):
    type: Literal["shutdown"] = "shutdown"

Event = Annotated[Union[RunOnboardingDataAssistantEvent], Field(discriminator="type")]
