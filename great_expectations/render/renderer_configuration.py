from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from great_expectations.core import (
        ExpectationConfiguration,
        ExpectationValidationResult,
    )


@dataclass
class RendererConfiguration:
    """Configuration object built for each renderer."""

    configuration: Union[ExpectationConfiguration, None]
    result: Union[ExpectationValidationResult, None]
    language: Union[str, None]
    runtime_configuration: Union[dict, None]
    include_column_name: Union[bool, None] = field(init=False)
    styling: Union[dict, None] = field(init=False)

    def __post_init__(self) -> None:
        include_column_name: Union[bool, None] = None
        styling: Union[dict, None] = None
        if self.runtime_configuration:
            include_column_name = (
                False
                if self.runtime_configuration.get("include_column_name") is False
                else True
            )
            styling = self.runtime_configuration.get("styling")

        self.include_column_name = include_column_name
        self.styling = styling
