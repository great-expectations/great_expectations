from typing import Literal, Union
from typing_extensions import Annotated

from pydantic import BaseModel, Field


class EventBase(BaseModel):
    type: str


class RunDataAssistantEvent(EventBase):
    type: Literal["run-data-assistant"] = "run-data-assistant"


class ShutdownEvent(EventBase):
    type: Literal["shutdown"] = "shutdown"


Event = Annotated[
    Union[RunDataAssistantEvent, ShutdownEvent], Field(discriminator="type")
]
