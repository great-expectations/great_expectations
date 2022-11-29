from typing import Any, Union

from pydantic import BaseModel, Field, create_model, root_validator

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

    @classmethod
    @root_validator(pre=False)
    def _set_fields(cls, values: dict) -> dict:
        if values["configuration"]:
            values["expectation_type"] = values["configuration"].expectation_type
            values["kwargs"] = values["configuration"].kwargs
        elif values["result"] and values["result"].expectation_config:
            values["expectation_type"] = values[
                "result"
            ].expectation_config.expectation_type
            values["kwargs"] = values["result"].expectation_config.kwargs
        else:
            values["expectation_type"] = ""
            values["kwargs"] = {}

        if values["runtime_configuration"]:
            values["include_column_name"] = (
                False
                if values["runtime_configuration"].get("include_column_name") is False
                else True
            )
            values["styling"] = values["runtime_configuration"].get("styling")

        return values

    class Config:
        validate_assignment = True
        arbitrary_types_allowed = True

    def add_param(self, name: str, schema_type: str, value: Union[Any, None]):
        renderer_param = create_model(
            name,
            renderer_schema=Field(dict, alias="schema"),
            value=(Union[Any, None], ...),
            __base__=RendererParam,
        )
        renderer_param_definition = {name: (renderer_param, ...)}
        renderer_params = create_model(
            "RendererParams",
            **renderer_param_definition,
            __config__=self.Config,
            __base__=self.params
        )
        renderer_params_definition = {
            name: renderer_param(renderer_schema={"type": schema_type}, value=value)
        }
        self.params = renderer_params(**renderer_params_definition)
