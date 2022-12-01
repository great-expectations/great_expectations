from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional, Type, TypeVar, Union

from pydantic import BaseModel, Field, create_model

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)

if TYPE_CHECKING:
    from pydantic.typing import AbstractSetIntStr, DictStrAny, MappingIntStrAny


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
        """
        Override BaseModel dict to make the default by_alias True instead of False.
        We need by_alias to be set to True in RendererParams, because we have an existing
        attribute named schema, and schema is already a Pydantic BaseModel attribute.
        In practice this means the renderer implementer doesn't need to use
        .dict(by_alias=True) everywhere.
        """
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
    runtime_configuration: Union[dict, None] = Field({}, allow_mutation=False)
    expectation_type: str = Field("", allow_mutation=False)
    kwargs: dict = Field({}, allow_mutation=False)
    include_column_name: bool = Field(True, allow_mutation=False)
    styling: Union[dict, None] = Field(None, allow_mutation=False)
    params: BaseModel = Field(default_factory=RendererParams, allow_mutation=True)

    class Config:
        validate_assignment = True
        arbitrary_types_allowed = True

    def __init__(self, **kwargs):
        kwargs = RendererConfiguration._set_expectation_type_and_kwargs(kwargs=kwargs)
        kwargs = RendererConfiguration._set_include_column_name_and_styling(
            kwargs=kwargs
        )
        super().__init__(**kwargs)

    @staticmethod
    def _set_expectation_type_and_kwargs(kwargs: dict) -> dict:
        if kwargs["configuration"]:
            kwargs["expectation_type"] = kwargs["configuration"].expectation_type
            kwargs["kwargs"] = kwargs["configuration"].kwargs
        elif kwargs["result"] and kwargs["result"].expectation_config:
            kwargs["expectation_type"] = kwargs[
                "result"
            ].expectation_config.expectation_type
            kwargs["kwargs"] = kwargs["result"].expectation_config.kwargs
        return kwargs

    @staticmethod
    def _set_include_column_name_and_styling(kwargs: dict) -> dict:
        if kwargs["runtime_configuration"]:
            kwargs["include_column_name"] = (
                False
                if kwargs["runtime_configuration"].get("include_column_name") is False
                else True
            )
            kwargs["styling"] = kwargs["runtime_configuration"].get("styling")
        return kwargs

    class RendererParam(BaseModel):
        value: Optional[Any]

        class Config:
            validate_assignment = True
            arbitrary_types_allowed = True

        def __eq__(self, other: Any) -> bool:
            if isinstance(other, BaseModel):
                return self.dict() == other.dict()
            elif isinstance(other, dict):
                return self.dict() == other
            else:
                return self.value == other

        def __bool__(self):
            """
            RendererConfiguration.add_param() will always add a new attribute to RendererConfiguration.params.
            In order to make the truthiness of a given attribute meaningful (not always True), we overload the
            __bool__ and check the truthiness of .value.
            """
            return bool(self.value)

    def add_param(
        self, name: str, schema_type: str, value: Optional[Any] = None
    ) -> None:
        """RendererConfiguration add_param method.

        This method will add a param that can be substituted into a template string during rendering.

        Attributes:
            name (str): A name for the attribute to be added to this RendererConfiguration instance.
            schema_type (str): The type of value being substituted. One of:
                - string
                - number
                - boolean
                - array
            value: Optional[Any]: The value to be substituted into the template string. If no value is
                provided, a value lookup will be attempted in RendererConfiguration.kwargs using the
                provided name.

        Returns:
            None
        """
        renderer_param: Type[BaseModel] = create_model(
            name,
            renderer_schema=(Dict[str, str], Field(..., alias="schema")),
            value=(Union[Any, None], ...),
            __base__=RendererConfiguration.RendererParam,
        )
        renderer_param_definition: Dict[str, Any] = {name: (renderer_param, ...)}

        # As of Nov 30, 2022 there is a bug in autocompletion for pydantic dynamic models
        # See: https://github.com/pydantic/pydantic/issues/3930
        renderer_params: Type[BaseModel] = create_model(
            "RendererParams",
            **renderer_param_definition,
            __base__=self.params.__class__,
        )

        if value is None:
            value = self.kwargs.get(name)

        renderer_params_definition: Dict[str, Any] = {
            **self.params.dict(),
            name: renderer_param(schema={"type": schema_type}, value=value),
        }
        self.params: BaseModel = renderer_params(**renderer_params_definition)
