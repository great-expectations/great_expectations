from __future__ import annotations

import re
from datetime import datetime
from enum import Enum
from numbers import Number
from typing import (
    TYPE_CHECKING,
    Any,
    Collection,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
    cast,
)

import dateutil
from dateutil.parser import ParserError
from pydantic import (
    BaseModel,
    Field,
    ValidationError,
    create_model,
    root_validator,
    validator,
)
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

    ARRAY = "array"
    BOOLEAN = "boolean"
    DATE = "date"
    NUMBER = "number"
    STRING = "string"


class _RendererParamsBase(BaseModel):
    """
    _RendererParamsBase is the base for the generic type RendererParams. It's attributes change as
        RendererConfiguration.add_param() dynamically adds them. It also overrides the default pydantic dict behavior
        due to the use of the reserved word schema in its attributes.
    """

    class Config:
        validate_assignment = True
        arbitrary_types_allowed = True

    def __len__(self) -> int:
        return len(self.__fields__)

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

    configuration: Optional[ExpectationConfiguration] = Field(
        None, allow_mutation=False
    )
    result: Optional[ExpectationValidationResult] = Field(None, allow_mutation=False)
    runtime_configuration: Optional[dict] = Field({}, allow_mutation=False)
    expectation_type: str = Field("", allow_mutation=False)
    kwargs: dict = Field({}, allow_mutation=False)
    include_column_name: bool = Field(True, allow_mutation=False)
    template_str: str = Field("", allow_mutation=True)
    header_row: List[Dict[str, Optional[Any]]] = Field([], allow_mutation=True)
    table: List[List[Dict[str, Optional[Any]]]] = Field([], allow_mutation=True)
    graph: dict = Field({}, allow_mutation=True)
    _raw_kwargs: dict = Field({}, allow_mutation=False)
    _row_condition: str = Field("", allow_mutation=False)
    params: RendererParams = Field(..., allow_mutation=True)

    class Config:
        validate_assignment = True
        arbitrary_types_allowed = True

    @root_validator(pre=True)
    def _validate_configuration_or_result(cls, values: dict) -> dict:
        if ("configuration" not in values or values["configuration"] is None) and (
            "result" not in values or values["result"] is None
        ):
            raise RendererConfigurationError(
                "RendererConfiguration must be passed either configuration or result."
            )
        return values

    def __init__(self, **values) -> None:
        values["params"] = _RendererParamsBase()
        super().__init__(**values)

    class _RendererParamBase(BaseModel):
        """
        _RendererParamBase is the base for a param that is added to RendererParams. It contains the validation logic,
            but it is dynamically renamed in order for the RendererParams attribute to have the same name as the param.
        """

        renderer_schema: Dict[str, RendererSchemaType] = Field(
            ..., allow_mutation=False
        )
        value: Any = Field(..., allow_mutation=False)

        class Config:
            validate_assignment = True
            arbitrary_types_allowed = True

        @root_validator(pre=True)
        def _validate_schema_matches_value(cls, values: dict) -> dict:
            schema_type: RendererSchemaType = values["schema"]["type"]
            value: Any = values["value"]
            if schema_type is RendererSchemaType.STRING:
                try:
                    str(value)
                except Exception as e:
                    raise RendererConfigurationError(
                        f"Value was unable to be represented as a string: {str(e)}"
                    )
            else:
                renderer_configuration_error = RendererConfigurationError(
                    f"Param schema_type: <{schema_type}> does "
                    f"not match value: <{value}>."
                )
                if schema_type is RendererSchemaType.NUMBER:
                    if not isinstance(value, Number):
                        raise renderer_configuration_error
                elif schema_type is RendererSchemaType.DATE:
                    if not isinstance(value, datetime):
                        try:
                            dateutil.parser.parse(value)
                        except ParserError:
                            raise renderer_configuration_error
                elif schema_type is RendererSchemaType.BOOLEAN:
                    if value is not True and value is not False:
                        raise renderer_configuration_error
                else:
                    if not isinstance(value, Iterable):
                        raise renderer_configuration_error
            return values

        def __eq__(self, other: Any) -> bool:
            if isinstance(other, BaseModel):
                return self.dict() == other.dict()
            elif isinstance(other, dict):
                return self.dict() == other
            else:
                return self == other

    @staticmethod
    def _get_renderer_param_base_model_type(
        name: str,
    ) -> Type[BaseModel]:
        return create_model(
            name,
            renderer_schema=(
                Dict[str, RendererSchemaType],
                Field(..., alias="schema"),
            ),
            value=(Union[Any, None], ...),
            __base__=RendererConfiguration._RendererParamBase,
        )

    @staticmethod
    def _get_evaluation_parameter_params_from_raw_kwargs(
        raw_kwargs: Dict[str, Any]
    ) -> Dict[str, Dict[str, Collection[str]]]:
        evaluation_parameter_count = 0
        renderer_params_args = {}
        for key, value in raw_kwargs.items():
            evaluation_parameter_name = f"eval_param__{evaluation_parameter_count}"
            renderer_params_args[evaluation_parameter_name] = {
                "schema": {"type": RendererSchemaType.STRING},
                "value": f'{key}: {value["$PARAMETER"]}',
            }
            evaluation_parameter_count += 1
        return renderer_params_args

    @root_validator()
    def _validate_and_set_renderer_attrs(cls, values: dict) -> dict:
        if (
            "result" in values
            and values["result"] is not None
            and values["result"].expectation_config is not None
        ):
            expectation_configuration: ExpectationConfiguration = values[
                "result"
            ].expectation_config
            values["expectation_type"] = expectation_configuration.expectation_type
            values["kwargs"] = expectation_configuration.kwargs
            raw_configuration: ExpectationConfiguration = (
                expectation_configuration.get_raw_configuration()
            )
            if "_raw_kwargs" not in values:
                values["_raw_kwargs"] = {
                    key: value
                    for key, value in raw_configuration.kwargs.items()
                    if (key, value) not in values["kwargs"].items()
                }
                renderer_params_args: Dict[
                    str, Dict[str, Collection[str]]
                ] = RendererConfiguration._get_evaluation_parameter_params_from_raw_kwargs(
                    raw_kwargs=values["_raw_kwargs"]
                )
                values["_params"] = (
                    {**values["_params"], **renderer_params_args}
                    if "_params" in values and values["_params"]
                    else renderer_params_args
                )
        else:
            values["expectation_type"] = values["configuration"].expectation_type
            values["kwargs"] = values["configuration"].kwargs
        return values

    @root_validator()
    def _validate_for_include_column_name(cls, values: dict) -> dict:
        if "runtime_configuration" in values and values["runtime_configuration"]:
            values["include_column_name"] = (
                False
                if values["runtime_configuration"].get("include_column_name") is False
                else True
            )
        return values

    @staticmethod
    def _get_row_condition_params(
        row_condition_str: str,
    ) -> Dict[str, Dict[str, Collection[str]]]:
        row_condition_str = RendererConfiguration._parse_row_condition_str(
            row_condition_str=row_condition_str
        )
        row_conditions_list: List[
            str
        ] = RendererConfiguration._get_row_conditions_list_from_row_condition_str(
            row_condition_str=row_condition_str
        )
        renderer_params_args = {}
        for idx, condition in enumerate(row_conditions_list):
            name = f"row_condition__{str(idx)}"
            value = condition.replace(" NOT ", " not ")
            renderer_params_args[name] = {
                "schema": {"type": RendererSchemaType.STRING},
                "value": value,
            }
        return renderer_params_args

    @root_validator()
    def _validate_for_row_condition(cls, values: dict) -> dict:
        kwargs: Dict[str, Any]
        if (
            "result" in values
            and values["result"] is not None
            and values["result"].expectation_config is not None
        ):
            kwargs = values["result"].expectation_config.kwargs
        else:
            kwargs = values["configuration"].kwargs

        values["_row_condition"] = kwargs.get("row_condition", "")
        if values["_row_condition"]:
            renderer_params_args: Dict[
                str, Dict[str, Collection[str]]
            ] = RendererConfiguration._get_row_condition_params(
                row_condition_str=values["_row_condition"],
            )
            values["_params"] = (
                {**values["_params"], **renderer_params_args}
                if "_params" in values and values["_params"]
                else renderer_params_args
            )
        return values

    @root_validator()
    def _validate_for_params(cls, values: dict) -> dict:
        if not values["params"]:
            _params: Optional[
                Dict[str, Dict[str, Union[str, Dict[str, RendererSchemaType]]]]
            ] = values.get("_params")
            if _params:
                renderer_param_definitions: Dict[str, Any] = {}
                for name in _params:
                    renderer_param_type: Type[
                        BaseModel
                    ] = RendererConfiguration._get_renderer_param_base_model_type(
                        name=name
                    )
                    renderer_param_definitions[name] = (
                        Optional[renderer_param_type],
                        ...,
                    )
                renderer_params: Type[BaseModel] = create_model(
                    "RendererParams",
                    **renderer_param_definitions,
                    __base__=_RendererParamsBase,
                )
                values["params"] = renderer_params(**_params)
            else:
                values["params"] = _RendererParamsBase()
        return values

    @staticmethod
    def _get_row_conditions_list_from_row_condition_str(
        row_condition_str: str,
    ) -> List[str]:
        # divide the whole condition into smaller parts
        row_conditions_list = re.split(r"AND|OR|NOT(?! in)|\(|\)", row_condition_str)
        row_conditions_list = [
            condition.strip() for condition in row_conditions_list if condition.strip()
        ]
        return row_conditions_list

    @staticmethod
    def _parse_row_condition_str(row_condition_str: str) -> str:
        if not row_condition_str:
            row_condition_str = "True"

        row_condition_str = (
            row_condition_str.replace("&", " AND ")
            .replace(" and ", " AND ")
            .replace("|", " OR ")
            .replace(" or ", " OR ")
            .replace("~", " NOT ")
            .replace(" not ", " NOT ")
        )
        row_condition_str = " ".join(row_condition_str.split())

        # replace tuples of values by lists of values
        tuples_list = re.findall(r"\([^()]*,[^()]*\)", row_condition_str)
        for value_tuple in tuples_list:
            value_list = value_tuple.replace("(", "[").replace(")", "]")
            row_condition_str = row_condition_str.replace(value_tuple, value_list)

        return row_condition_str

    @staticmethod
    def _get_row_condition_string(row_condition_str: str) -> str:
        row_condition_str = RendererConfiguration._parse_row_condition_str(
            row_condition_str=row_condition_str
        )
        row_conditions_list: List[
            str
        ] = RendererConfiguration._get_row_conditions_list_from_row_condition_str(
            row_condition_str=row_condition_str
        )
        for idx, condition in enumerate(row_conditions_list):
            row_condition_str = row_condition_str.replace(
                condition, f"$row_condition__{str(idx)}"
            )
        row_condition_str = row_condition_str.lower()
        return f"if {row_condition_str}"

    @validator("template_str")
    def _set_template_str(cls, v: str, values: dict) -> str:
        if "_row_condition" in values and values["_row_condition"]:
            row_condition_str: str = RendererConfiguration._get_row_condition_string(
                row_condition_str=values["_row_condition"]
            )
            v = f"{row_condition_str}, then {v}"

        if "_raw_kwargs" in values and values["_raw_kwargs"]:
            v += "  "
            for evaluation_parameter_count in range(len(values["_raw_kwargs"])):
                v += f" $eval_param__{evaluation_parameter_count},"
            v = v[:-1]
        return v

    @staticmethod
    def _choose_schema_type_for_value(
        schema_types: List[RendererSchemaType], value: Any
    ) -> RendererSchemaType:
        if isinstance(value, dict):
            return RendererSchemaType.STRING

        for schema_type in schema_types:
            try:
                renderer_param: Type[
                    BaseModel
                ] = RendererConfiguration._get_renderer_param_base_model_type(
                    name="try_param"
                )
                renderer_param(schema={"type": schema_type}, value=value)
                return schema_type
            except ValidationError:
                pass
        raise RendererConfigurationError(
            f"None of the schema_types: {schema_types} match the value: {value}"
        )

    def add_param(
        self,
        name: str,
        schema_type: Union[RendererSchemaType, List[RendererSchemaType]],
        value: Optional[Any] = None,
    ) -> None:
        """Adds a param that can be substituted into a template string during rendering.

        Attributes:
            name (str): A name for the attribute to be added to this RendererConfiguration instance.
            schema_type (list of, or single, RendererSchemaType or string): The possible types for the value being
                substituted. If more than one schema_type is passed, inference based on param value will be performed,
                and the first schema_type to match the value will be selected.
                    One of:
                     - array
                     - boolean
                     - date
                     - number
                     - string
            value (Optional[Any]): The value to be substituted into the template string. If no value is
                provided, a value lookup will be attempted in RendererConfiguration.kwargs using the
                provided name.

        Returns:
            None
        """
        renderer_param: Type[
            BaseModel
        ] = RendererConfiguration._get_renderer_param_base_model_type(name=name)
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

        if isinstance(schema_type, list) and value is not None:
            schema_type = RendererConfiguration._choose_schema_type_for_value(
                schema_types=schema_type, value=value
            )

        renderer_params_args: Dict[str, Optional[Any]]
        if value is None:
            renderer_params_args = {
                **self.params.dict(exclude_none=False),
                name: None,
            }
        else:
            renderer_params_args = {
                **self.params.dict(exclude_none=False),
                name: renderer_param(schema={"type": schema_type}, value=value),
            }

        self.params = cast(RendererParams, renderer_params(**renderer_params_args))
