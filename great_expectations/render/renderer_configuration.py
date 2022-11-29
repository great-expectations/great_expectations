from typing import Any, Union

from pydantic import BaseModel, Field, create_model

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)


class RendererParam(BaseModel):
    class Config:
        validate_assignment = True
        arbitrary_types_allowed = True


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
    params: Union[BaseModel, None] = Field(None, allow_mutation=True)

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

    def add_param(self, name: str, schema_type: str, value: Union[Any, None]):
        renderer_param = create_model(
            name,
            renderer_schema=(dict, Field(..., alias="schema")),
            value=(Union[Any, None], ...),
            __base__=RendererParam,
        )
        renderer_param_definition = {name: (renderer_param, ...)}
        renderer_params = create_model(
            "RendererParams", **renderer_param_definition, __base__=self.params
        )
        renderer_params_definition = {
            name: renderer_param(schema={"type": schema_type}, value=value)
        }
        params = renderer_params(**renderer_params_definition)
        self.params = params
