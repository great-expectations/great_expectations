from typing import TYPE_CHECKING

from great_expectations.render.util import substitute_none_for_missing

if TYPE_CHECKING:
    from typing import List, Optional, Union

    from great_expectations.core import (
        ExpectationConfiguration,
        ExpectationValidationResult,
    )


class RendererConfiguration:
    """Configuration object built by each renderer."""

    def __init__(
        self,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        language: Optional[str] = None,
        runtime_configuration: Optional[dict] = None,
        kwargs_list: Optional[List[str]] = None,
    ):
        include_column_name: Union[bool, None] = None
        styling: Union[dict, None] = None
        if runtime_configuration:
            include_column_name = (
                False
                if runtime_configuration.get("include_column_name") is False
                else True
            )
            styling = runtime_configuration.get("styling")

        self.include_column_name = include_column_name
        self.styling = styling

        kwargs: dict
        if configuration:
            kwargs = configuration.kwargs
        elif result and result.expectation_config:
            kwargs = result.expectation_config.kwargs
        else:
            kwargs = {}

        params: dict = {}
        if kwargs_list:
            params = substitute_none_for_missing(
                kwargs,
                kwargs_list,
            )

        self.params = params

        self.language = language
