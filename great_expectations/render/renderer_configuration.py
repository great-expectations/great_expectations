from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Union

from pydantic import BaseModel, Field, create_model

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)

if TYPE_CHECKING:
    from pydantic.typing import AbstractSetIntStr, DictStrAny, MappingIntStrAny


class RendererParam(BaseModel):
    class Config:
        validate_assignment = True
        arbitrary_types_allowed = True


class RendererParams(BaseModel):
    class Config:
        validate_assignment = True
        arbitrary_types_allowed = True

    def dict(
        self,
        include: Optional[Union[AbstractSetIntStr, MappingIntStrAny]] = None,
        exclude: Optional[Union[AbstractSetIntStr, MappingIntStrAny]] = None,
        by_alias: bool = True,
        skip_defaults: Optional[bool] = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
    ) -> DictStrAny:
        return super().dict(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            skip_defaults=skip_defaults,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
        )


class RendererConfiguration(BaseModel):
    """Configuration object built for each renderer."""

    configuration: Union[ExpectationConfiguration, None] = Field(
        ..., allow_mutation=False
    )
    result: Union[ExpectationValidationResult, None] = Field(..., allow_mutation=False)
    language: Union[str, None] = Field("", allow_mutation=False)
    runtime_configuration: Union[dict, None] = Field({}, allow_mutation=False)
    expectation_type: str = Field("", allow_mutation=False)
    kwargs: dict = Field({}, allow_mutation=False)
    include_column_name: bool = Field(True, allow_mutation=False)
    styling: Union[dict, None] = Field(None, allow_mutation=False)
    params: RendererParams = Field(default_factory=RendererParams, allow_mutation=True)

    def __init__(self, **kwargs):
        if kwargs["configuration"]:
            kwargs["expectation_type"] = kwargs["configuration"].expectation_type
            kwargs["kwargs"] = kwargs["configuration"].kwargs
        elif kwargs["result"] and kwargs["result"].expectation_config:
            kwargs["expectation_type"] = kwargs[
                "result"
            ].expectation_config.expectation_type
            kwargs["kwargs"] = kwargs["result"].expectation_config.kwargs
        else:
            kwargs["expectation_type"] = ""
            kwargs["kwargs"] = {}

        if kwargs["runtime_configuration"]:
            kwargs["include_column_name"] = (
                False
                if kwargs["runtime_configuration"].get("include_column_name") is False
                else True
            )
            kwargs["styling"] = kwargs["runtime_configuration"].get("styling")

        super().__init__(**kwargs)

    class Config:
        validate_assignment = True
        arbitrary_types_allowed = True

    def add_param(self, name: str, schema_type: str, value: Union[Any, None]) -> None:
        renderer_param = create_model(
            name,
            renderer_schema=(dict, Field(..., alias="schema")),
            value=(Union[Any, None], ...),
            __base__=RendererParam,
        )
        renderer_param_definition = {name: (renderer_param, ...)}

        renderer_params = create_model(
            "RendererParams",
            **renderer_param_definition,
            __base__=self.params.__class__,
        )
        renderer_params_definition = {
            **self.params.dict(),
            name: renderer_param(schema={"type": schema_type}, value=value),
        }
        self.params = renderer_params(**renderer_params_definition)
