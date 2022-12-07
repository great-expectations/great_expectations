from dataclasses import dataclass, field
from typing import Union

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)


@dataclass(frozen=True)
class RendererConfiguration:
    """Configuration object built for each renderer."""

    configuration: Union[ExpectationConfiguration, None]
    result: Union[ExpectationValidationResult, None]
    runtime_configuration: dict = field(default_factory=dict)
    kwargs: dict = field(init=False)
    include_column_name: bool = field(init=False)
    styling: Union[dict, None] = field(init=False)

    def __post_init__(self) -> None:
        kwargs: dict
        if self.configuration:
            kwargs = self.configuration.kwargs
        elif self.result and self.result.expectation_config:
            kwargs = self.result.expectation_config.kwargs
        else:
            kwargs = {}

        object.__setattr__(self, "kwargs", kwargs)

        include_column_name: bool = True
        styling: Union[dict, None] = None
        if self.runtime_configuration:
            include_column_name = (
                False
                if self.runtime_configuration.get("include_column_name") is False
                else True
            )
            styling = self.runtime_configuration.get("styling")

        object.__setattr__(self, "include_column_name", include_column_name)
        object.__setattr__(self, "styling", styling)
