from __future__ import annotations

from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
    cast,
)

from pydantic import BaseModel, Field, create_model, root_validator
from pydantic.generics import GenericModel

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.render.exceptions import RendererConfigurationError

if TYPE_CHECKING:
    from pydantic.typing import AbstractSetIntStr, DictStrAny, MappingIntStrAny


class RendererSchemaType(str, Enum):
    """Type used in renderer json schema dictionary."""

    STRING = "string"
    NUMBER = "number"
    BOOLEAN = "boolean"
    ARRAY = "array"


class _RendererParamsBase(BaseModel):
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
        exclude_none: bool = True,
    ) -> DictStrAny:
        """
        Override BaseModel dict to make the defaults:
            - by_alias=True because we have an existing attribute named schema, and schema is already a Pydantic
              BaseModel attribute.
            - exclude_none=True to ensure that None values aren't included in the json dict.

        In practice this means the renderer implementer doesn't need to use .dict(by_alias=True, exclude_none=True)
        everywhere.
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


RendererParams = TypeVar("RendererParams", bound=_RendererParamsBase)


class RendererConfiguration(GenericModel, Generic[RendererParams]):
    """Configuration object built for each renderer."""

    configuration: Union[ExpectationConfiguration, None] = Field(
        None, allow_mutation=False
    )
    result: Optional[ExpectationValidationResult] = Field(None, allow_mutation=False)
    runtime_configuration: Optional[dict] = Field({}, allow_mutation=False)
    expectation_type: str = Field("", allow_mutation=False)
    kwargs: dict = Field({}, allow_mutation=False)
    include_column_name: bool = Field(True, allow_mutation=False)
    styling: Optional[dict] = Field(None, allow_mutation=False)
    params: RendererParams = Field(..., allow_mutation=True)
    template_str: str = Field("", allow_mutation=True)
    header_row: List[Dict[str, Optional[Any]]] = Field([], allow_mutation=True)
    table: List[List[Dict[str, Optional[Any]]]] = Field([], allow_mutation=True)
    graph: dict = Field({}, allow_mutation=True)

    class Config:
        validate_assignment = True
        arbitrary_types_allowed = True

    @staticmethod
    def validate_configuration_or_result(values: dict) -> dict:
        if "configuration" not in values and "result" not in values:
            raise RendererConfigurationError(
                "RendererConfiguration must be passed either configuration or result."
            )
        return values

    @root_validator(pre=True)
    def validate_for_expectation_type_and_kwargs(cls, values: dict) -> dict:
        RendererConfiguration.validate_configuration_or_result(values=values)
        if "configuration" in values and values["configuration"]:
            values["expectation_type"] = values["configuration"].expectation_type
            values["kwargs"] = values["configuration"].kwargs
        else:
            values["expectation_type"] = values[
                "result"
            ].expectation_config.expectation_type
            values["kwargs"] = values["result"].expectation_config.kwargs
        return values

    @root_validator(pre=True)
    def validate_for_include_column_name_and_styling(cls, values: dict) -> dict:
        if "runtime_configuration" in values and values["runtime_configuration"]:
            values["include_column_name"] = (
                False
                if values["runtime_configuration"].get("include_column_name") is False
                else True
            )
            values["styling"] = values["runtime_configuration"].get("styling")
        return values

    def __init__(self, **values) -> None:
        values["params"] = _RendererParamsBase()
        super().__init__(**values)

    class _RendererParamBase(BaseModel):
        renderer_schema: Optional[RendererSchemaType] = Field(..., allow_mutation=False)
        value: Optional[Any] = Field(..., allow_mutation=False)

        class Config:
            validate_assignment = True
            arbitrary_types_allowed = True

        def __eq__(self, other: Any) -> bool:
            if isinstance(other, BaseModel):
                return self.dict() == other.dict()
            elif isinstance(other, dict):
                return self.dict() == other
            else:
                return self == other

    def add_param(
        self,
        name: str,
        schema_type: Union[RendererSchemaType, str],
        value: Optional[Any] = None,
    ) -> None:
        """Adds a param that can be substituted into a template string during rendering.

        Attributes:
            name (str): A name for the attribute to be added to this RendererConfiguration instance.
            schema_type (RendererSchemaType or string): The type of value being substituted. One of:
                - string
                - number
                - boolean
                - array
            value (Optional[Any]): The value to be substituted into the template string. If no value is
                provided, a value lookup will be attempted in RendererConfiguration.kwargs using the
                provided name.

        Returns:
            None
        """
        renderer_param: Type[BaseModel] = create_model(
            name,
            renderer_schema=(
                Dict[str, Optional[RendererSchemaType]],
                Field(..., alias="schema"),
            ),
            value=(Union[Any, None], ...),
            __base__=RendererConfiguration._RendererParamBase,
        )
        renderer_param_definition: Dict[str, Any] = {
            name: (Optional[renderer_param], ...)
        }

        # As of Nov 30, 2022 there is a bug in autocompletion for pydantic dynamic models
        # See: https://github.com/pydantic/pydantic/issues/3930
        renderer_params: Type[BaseModel] = create_model(
            "RendererParams",
            **renderer_param_definition,
            __base__=self.params.__class__,
        )

        if value is None:
            value = self.kwargs.get(name)

        renderer_params_definition: Dict[str, Optional[Any]]
        if value is None:
            renderer_params_definition = {
                **self.params.dict(exclude_none=False),
                name: None,
            }
        else:
            renderer_params_definition = {
                **self.params.dict(exclude_none=False),
                name: renderer_param(schema={"type": schema_type}, value=value),
            }

        self.params = cast(
            RendererParams, renderer_params(**renderer_params_definition)
        )
