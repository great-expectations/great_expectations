from __future__ import annotations

import datetime
import functools
import logging
import re
import warnings
from abc import ABC, abstractmethod
from collections import Counter
from copy import deepcopy
from inspect import isabstract
from numbers import Number
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    List,
    Literal,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import pandas as pd
from dateutil.parser import parse
from typing_extensions import ParamSpec, dataclass_transform

from great_expectations import __version__ as ge_version
from great_expectations._docs_decorators import public_api
from great_expectations.compatibility import pydantic
from great_expectations.compatibility.pydantic import Field, ModelMetaclass, StrictStr
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.metric_function_types import (
    SummarizationMetricNameSuffixes,
)
from great_expectations.core.result_format import ResultFormat
from great_expectations.core.suite_parameters import (
    get_suite_parameter_key,
    is_suite_parameter,
)
from great_expectations.exceptions import (
    GreatExpectationsError,
    InvalidExpectationConfigurationError,
    InvalidExpectationKwargsError,
)
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
    parse_result_format,
)
from great_expectations.expectations.model_field_descriptions import (
    COLUMN_A_DESCRIPTION,
    COLUMN_B_DESCRIPTION,
    COLUMN_DESCRIPTION,
    COLUMN_LIST_DESCRIPTION,
    MOSTLY_DESCRIPTION,
    WINDOWS_DESCRIPTION,
)
from great_expectations.expectations.model_field_types import (
    Mostly,
)
from great_expectations.expectations.registry import (
    get_metric_kwargs,
    register_expectation,
    register_renderer,
)
from great_expectations.expectations.sql_tokens_and_types import (
    valid_sql_tokens_and_types,
)
from great_expectations.expectations.window import Window
from great_expectations.render import (
    AtomicDiagnosticRendererType,
    AtomicPrescriptiveRendererType,
    CollapseContent,
    LegacyDiagnosticRendererType,
    LegacyRendererType,
    RenderedAtomicContent,
    RenderedStringTemplateContent,
    RenderedTableContent,
    renderedAtomicValueSchema,
)
from great_expectations.render.exceptions import RendererConfigurationError
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.renderer_configuration import (
    RendererConfiguration,
    RendererValueType,
)
from great_expectations.render.util import (
    build_count_and_index_table,
    build_count_table,
    num_to_str,
)
from great_expectations.util import camel_to_snake
from great_expectations.validator.computed_metric import MetricValue  # noqa: TCH001
from great_expectations.validator.metric_configuration import MetricConfiguration

if TYPE_CHECKING:
    from great_expectations.core.expectation_diagnostics.expectation_diagnostics import (
        ExpectationDiagnostics,
    )
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.execution_engine import (
        ExecutionEngine,
    )
    from great_expectations.render.renderer_configuration import MetaNotes
    from great_expectations.validator.validator import ValidationDependencies, Validator

logger = logging.getLogger(__name__)

P = ParamSpec("P")
T = TypeVar("T", List[RenderedStringTemplateContent], RenderedAtomicContent)


def render_suite_parameter_string(render_func: Callable[P, T]) -> Callable[P, T]:  # noqa: C901
    """Decorator for Expectation classes that renders suite parameters as strings.

    allows Expectations that use Suite Parameters to render the values
    of the Suite Parameters along with the rest of the output.

    Args:
        render_func: The render method of the Expectation class.

    Raises:
        GreatExpectationsError: If runtime_configuration with suite_parameters is not provided.
    """

    def inner_func(*args: P.args, **kwargs: P.kwargs) -> T:  # noqa: C901 - too complex
        rendered_string_template = render_func(*args, **kwargs)
        current_expectation_params: list = []
        app_template_str = "\n - $eval_param = $eval_param_value (at time of validation)."
        configuration: dict | None = kwargs.get("configuration")  # type: ignore[assignment] # could be object?
        if configuration:
            kwargs_dict: dict = configuration.get("kwargs", {})
            for value in kwargs_dict.values():
                if is_suite_parameter(value):
                    key = get_suite_parameter_key(value)
                    current_expectation_params.append(key)

        # if expectation configuration has no eval params, then don't look for the values in runtime_configuration  # noqa: E501
        # isinstance check should be removed upon implementation of RenderedAtomicContent suite parameter support  # noqa: E501
        if current_expectation_params and not isinstance(
            rendered_string_template, RenderedAtomicContent
        ):
            runtime_configuration: Optional[dict] = kwargs.get("runtime_configuration")  # type: ignore[assignment] # could be object?
            if runtime_configuration:
                eval_params = runtime_configuration.get("suite_parameters", {})
                styling = runtime_configuration.get("styling")
                for key, val in eval_params.items():
                    for param in current_expectation_params:
                        # "key in param" condition allows for eval param values to be rendered if arithmetic is present  # noqa: E501
                        if key == param or key in param:
                            app_params = {}
                            app_params["eval_param"] = key
                            app_params["eval_param_value"] = val
                            rendered_content = RenderedStringTemplateContent(
                                **{  # type: ignore[arg-type]
                                    "content_block_type": "string_template",
                                    "string_template": {
                                        "template": app_template_str,
                                        "params": app_params,
                                        "styling": styling,
                                    },
                                }
                            )
                            rendered_string_template.append(rendered_content)
            else:
                raise GreatExpectationsError(  # noqa: TRY003
                    f"""GX was not able to render the value of suite parameters.
                        Expectation {render_func} had suite parameters set, but they were not passed in."""  # noqa: E501
                )
        return rendered_string_template

    return inner_func


def param_method(param_name: str) -> Callable:
    """
    Decorator that wraps helper methods dealing with dynamic attributes on RendererConfiguration.params. Ensures a given
    param_name exists and is not None before executing the helper method. Params are unknown as they are defined by the
    renderer, and if a given param is None, no value was set/found that can be used in the helper method.

    If a helper method is decorated with @param_method(param_name="<param_name>") and the param attribute does not
    exist, the method will return either the input RendererConfiguration or None depending on the declared return type.
    """  # noqa: E501
    if not param_name:
        # If param_name was passed as an empty string
        raise RendererConfigurationError(  # noqa: TRY003
            "Method decorated with @param_method must be passed an existing param_name."
        )

    def _param_method(param_func: Callable) -> Callable:
        @functools.wraps(param_func)
        def wrapper(
            renderer_configuration: RendererConfiguration,
        ) -> Optional[Any]:
            try:
                return_type: Type = param_func.__annotations__["return"]
            except KeyError:
                method_name: str = getattr(param_func, "__name__", repr(param_func))
                raise RendererConfigurationError(  # noqa: TRY003
                    "Methods decorated with @param_method must have an annotated return "
                    f"type, but method {method_name} does not."
                )

            if hasattr(renderer_configuration.params, param_name):
                if getattr(renderer_configuration.params, param_name, None):
                    return_obj = param_func(renderer_configuration=renderer_configuration)
                else:  # noqa: PLR5501
                    if return_type is RendererConfiguration:
                        return_obj = renderer_configuration
                    else:
                        return_obj = None
            else:
                raise RendererConfigurationError(  # noqa: TRY003
                    f"RendererConfiguration.param does not have a param called {param_name}. "
                    f'Use RendererConfiguration.add_param() with name="{param_name}" to add it.'
                )
            return return_obj

        return wrapper

    return _param_method


# noinspection PyMethodParameters
@dataclass_transform(kw_only_default=True, field_specifiers=(Field,))
class MetaExpectation(ModelMetaclass):
    """MetaExpectation registers Expectations as they are defined, adding them to the Expectation registry.

    Any class inheriting from Expectation will be registered based on the value of the "expectation_type" class
    attribute, or, if that is not set, by snake-casing the name of the class.
    """  # noqa: E501

    def __new__(cls, clsname, bases, attrs):
        newclass = super().__new__(cls, clsname, bases, attrs)
        # noinspection PyUnresolvedReferences
        if not newclass.is_abstract():
            newclass.expectation_type = camel_to_snake(clsname)
            register_expectation(newclass)
        else:
            newclass.expectation_type = ""

        # noinspection PyUnresolvedReferences
        newclass._register_renderer_functions()
        return newclass


@public_api
class Expectation(pydantic.BaseModel, metaclass=MetaExpectation):
    """Base class for all Expectations.

    For a list of all available expectation types, see the `Expectation Gallery <https://greatexpectations.io/expectations/>`_.

    Expectation classes *must* have the following attributes set:
        1. `domain_keys`: a tuple of the *keys* used to determine the domain of the
           expectation
        2. `success_keys`: a tuple of the *keys* used to determine the success of
           the expectation.

    In some cases, subclasses of Expectation (such as BatchExpectation) can
    inherit these properties from their parent class.

    They *may* optionally override `runtime_keys`, and
    may optionally set an explicit value for expectation_type.
    runtime_keys lists the keys that can be used to control output but will
    not affect the actual success value of the expectation (such as result_format).

    Expectation classes *must* implement the following:
        1. `_validate`
        2. `get_validation_dependencies`

    In some cases, subclasses of Expectation, such as ColumnMapExpectation will already
    have correct implementations that may simply be inherited.

    Additionally, they *may* provide implementations of:
        1. `validate_configuration`, which should raise an error if the configuration
           will not be usable for the Expectation
        2. Data Docs rendering methods decorated with the @renderer decorator. See the
    """

    class Config:
        arbitrary_types_allowed = True
        smart_union = True
        extra = pydantic.Extra.forbid
        json_encoders = {RenderedAtomicContent: lambda data: data.to_json_dict()}

        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type[Expectation]) -> None:
            schema["properties"]["metadata"] = {
                "type": "object",
                "properties": {
                    "expectation_class": {
                        "title": "Expectation Class",
                        "type": "string",
                        "const": model.__name__,
                    },
                    "expectation_type": {
                        "title": "Expectation Type",
                        "type": "string",
                        "const": model.expectation_type,
                    },
                },
            }

    id: Union[str, None] = None
    meta: Union[dict, None] = None
    notes: Union[str, List[str], None] = None
    result_format: Union[ResultFormat, dict] = ResultFormat.BASIC
    description: Union[str, None] = pydantic.Field(
        default=None, description="A short description of your Expectation"
    )

    catch_exceptions: bool = False
    rendered_content: Optional[List[RenderedAtomicContent]] = None

    version: ClassVar[str] = ge_version
    domain_keys: ClassVar[Tuple[str, ...]] = ()
    success_keys: ClassVar[Tuple[str, ...]] = ()
    runtime_keys: ClassVar[Tuple[str, ...]] = (
        "catch_exceptions",
        "result_format",
    )
    args_keys: ClassVar[Tuple[str, ...]] = ()

    expectation_type: ClassVar[str]
    windows: Optional[List[Window]] = pydantic.Field(default=None, description=WINDOWS_DESCRIPTION)
    examples: ClassVar[List[dict]] = []

    _save_callback: Union[Callable[[Expectation], Expectation], None] = pydantic.PrivateAttr(
        default=None
    )

    @override
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Expectation):
            return False

        # rendered_content is derived from the rest of the expectation, and can/should
        # be excluded from equality checks
        exclude: set[str] = {"rendered_content"}

        self_dict = self.dict(exclude=exclude)
        other_dict = other.dict(exclude=exclude)

        # Simplify notes and meta equality - falsiness is equivalent
        for attr in ("notes", "meta"):
            self_val = self_dict.pop(attr, None) or None
            other_val = other_dict.pop(attr, None) or None
            if self_val != other_val:
                return False

        return self_dict == other_dict

    @pydantic.validator("result_format")
    def _validate_result_format(cls, result_format: ResultFormat | dict) -> ResultFormat | dict:
        if isinstance(result_format, dict) and "result_format" not in result_format:
            raise ValueError(  # noqa: TRY003
                "If configuring result format with a dictionary, the key 'result_format' must be present."  # noqa: E501
            )
        return result_format

    @classmethod
    def is_abstract(cls) -> bool:
        return isabstract(cls)

    def register_save_callback(self, save_callback: Callable[[Expectation], Expectation]) -> None:
        self._save_callback = save_callback

    @public_api
    def save(self):
        """Save the current state of this Expectation."""
        if not self._save_callback:
            raise RuntimeError(  # noqa: TRY003
                "Expectation must be added to ExpectationSuite before it can be saved."
            )
        if self._include_rendered_content:
            self.render()

        updated_self = self._save_callback(self)
        self.id = updated_self.id

    @classmethod
    def _register_renderer_functions(cls) -> None:
        expectation_type: str = camel_to_snake(cls.__name__)

        for candidate_renderer_fn_name in dir(cls):
            attr_obj: Callable | None = getattr(cls, candidate_renderer_fn_name, None)
            # attrs are not guaranteed to exist https://docs.python.org/3.10/library/functions.html#dir
            if attr_obj is None:
                continue
            if not hasattr(attr_obj, "_renderer_type"):
                continue
            register_renderer(object_name=expectation_type, parent_class=cls, renderer_fn=attr_obj)

    def render(self) -> None:
        """
        Renders content using the atomic prescriptive renderer for each expectation configuration associated with
           this ExpectationSuite to ExpectationConfiguration.rendered_content.
        """  # noqa: E501
        from great_expectations.render.renderer.inline_renderer import InlineRenderer

        inline_renderer = InlineRenderer(render_object=self.configuration)
        self.rendered_content = inline_renderer.get_rendered_content()

    @abstractmethod
    def _validate(
        self,
        metrics: dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ) -> Union[ExpectationValidationResult, dict]:
        raise NotImplementedError

    @classmethod
    @renderer(renderer_type=AtomicPrescriptiveRendererType.FAILED)
    def _prescriptive_failed(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> RenderedAtomicContent:
        """
        Default rendering function that is utilized by GX Cloud Front-end if an implemented atomic renderer fails
        """  # noqa: E501
        renderer_configuration: RendererConfiguration = RendererConfiguration(
            configuration=configuration,
            result=result,
            runtime_configuration=runtime_configuration,
        )

        template_str = "Rendering failed for Expectation: "

        if renderer_configuration.expectation_type and renderer_configuration.kwargs:
            template_str += "$expectation_type(**$kwargs)."
        elif renderer_configuration.expectation_type:
            template_str += "$expectation_type."
        else:
            template_str = f"{template_str[:-2]}."

        renderer_configuration.add_param(
            name="expectation_type",
            param_type=RendererValueType.STRING,
            value=renderer_configuration.expectation_type,
        )
        renderer_configuration.add_param(
            name="kwargs",
            param_type=RendererValueType.STRING,
            value=renderer_configuration.kwargs,
        )

        value_obj = renderedAtomicValueSchema.load(
            {
                "template": template_str,
                "params": renderer_configuration.params.dict(),
                "meta_notes": renderer_configuration.meta_notes,
                "schema": {"type": "com.superconductive.rendered.string"},
            }
        )
        rendered = RenderedAtomicContent(
            name=AtomicPrescriptiveRendererType.FAILED,
            value=value_obj,
            value_type="StringValueType",
        )
        return rendered

    @classmethod
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        if renderer_configuration.expectation_type and renderer_configuration.kwargs:
            template_str = "$expectation_type(**$kwargs)"
        elif renderer_configuration.expectation_type:
            template_str = "$expectation_type"
        else:
            raise ValueError("RendererConfiguration does not contain an expectation_type.")  # noqa: TRY003

        add_param_args = (
            (
                "expectation_type",
                RendererValueType.STRING,
                renderer_configuration.expectation_type,
            ),
            ("kwargs", RendererValueType.STRING, renderer_configuration.kwargs),
        )
        for name, param_type, value in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type, value=value)

        renderer_configuration.template_str = template_str
        return renderer_configuration

    @classmethod
    def _atomic_prescriptive_template(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> Tuple[Optional[str], dict, MetaNotes, Optional[dict]]:
        """
        Template function that contains the logic that is shared by AtomicPrescriptiveRendererType.SUMMARY and
        LegacyRendererType.PRESCRIPTIVE.
        """  # noqa: E501
        # deprecated-v0.15.43
        warnings.warn(
            "The method _atomic_prescriptive_template is deprecated as of v0.15.43 and will be removed in v0.18. "  # noqa: E501
            "Please refer to Expectation method _prescriptive_template for the latest renderer template pattern.",  # noqa: E501
            DeprecationWarning,
        )
        renderer_configuration: RendererConfiguration = RendererConfiguration(
            configuration=configuration,
            result=result,
            runtime_configuration=runtime_configuration,
        )
        renderer_configuration = cls._prescriptive_template(
            renderer_configuration=renderer_configuration,
        )
        styling = runtime_configuration.get("styling", {}) if runtime_configuration else {}
        return (
            renderer_configuration.template_str,
            renderer_configuration.params.dict(),
            renderer_configuration.meta_notes,
            styling,
        )

    @classmethod
    @renderer(renderer_type=AtomicPrescriptiveRendererType.SUMMARY)
    @render_suite_parameter_string
    def _prescriptive_summary(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> RenderedAtomicContent:
        renderer_configuration: RendererConfiguration = RendererConfiguration(
            configuration=configuration,
            result=result,
            runtime_configuration=runtime_configuration,
        )
        renderer_configuration = cls._prescriptive_template(
            renderer_configuration=renderer_configuration,
        )
        value_obj = renderedAtomicValueSchema.load(
            {
                "template": renderer_configuration.template_str,
                "params": renderer_configuration.params.dict(),
                "code_block": renderer_configuration.code_block or None,
                "meta_notes": renderer_configuration.meta_notes,
                "schema": {"type": "com.superconductive.rendered.string"},
            }
        )
        rendered = RenderedAtomicContent(
            name=AtomicPrescriptiveRendererType.SUMMARY,
            value=value_obj,
            value_type="StringValueType",
        )
        return rendered

    @classmethod
    @renderer(renderer_type=LegacyRendererType.PRESCRIPTIVE)
    def _prescriptive_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> List[RenderedStringTemplateContent]:
        renderer_configuration: RendererConfiguration = RendererConfiguration(
            configuration=configuration,
            result=result,
            runtime_configuration=runtime_configuration,
        )
        return [
            RenderedStringTemplateContent(
                **{  # type: ignore[arg-type]
                    "content_block_type": "string_template",
                    "styling": {"parent": {"classes": ["alert", "alert-warning"]}},
                    "string_template": {
                        "template": "$expectation_type(**$kwargs)",
                        "params": {
                            "expectation_type": renderer_configuration.expectation_type,
                            "kwargs": renderer_configuration.kwargs,
                        },
                        "styling": {
                            "params": {
                                "expectation_type": {
                                    "classes": ["badge", "badge-warning"],
                                }
                            }
                        },
                    },
                }
            )
        ]

    @classmethod
    @renderer(renderer_type=LegacyDiagnosticRendererType.META_PROPERTIES)
    def _diagnostic_meta_properties_renderer(  # noqa: C901
        cls,
        result: Optional[ExpectationValidationResult] = None,
    ) -> Union[list, List[str], List[list]]:
        """
            Render function used to add custom meta to Data Docs
            It gets a column set in the `properties_to_render` dictionary within `meta` and adds columns in Data Docs with the values that were set.
            example:
            meta = {
                "properties_to_render": {
                "Custom Column Header": "custom.value"
            },
                "custom": {
                "value": "1"
                }
            }
        data docs:
        ----------------------------------------------------------------
        | status|  Expectation                          | Observed value | Custom Column Header |
        ----------------------------------------------------------------
        |       | must be exactly 4 columns             |         4       |          1            |

        Here the custom column will be added in data docs.
        """  # noqa: E501

        if not result:
            return []
        custom_property_values = []
        meta_properties_to_render: Optional[dict] = None
        if result and result.expectation_config:
            meta_properties_to_render = result.expectation_config.kwargs.get(
                "meta_properties_to_render"
            )
        if meta_properties_to_render:
            for key in sorted(meta_properties_to_render.keys()):
                meta_property = meta_properties_to_render[key]
                if meta_property:
                    try:
                        # Allow complex structure with . usage
                        assert isinstance(result.expectation_config, ExpectationConfiguration)
                        obj = result.expectation_config.meta["attributes"]
                        keys = meta_property.split(".")
                        for i in range(0, len(keys)):
                            # Allow for keys with a . in the string like {"item.key": "1"}
                            remaining_key = "".join(keys[i:])
                            if remaining_key in obj:
                                obj = obj[remaining_key]
                                break
                            else:
                                obj = obj[keys[i]]

                        custom_property_values.append([obj])
                    except KeyError:
                        custom_property_values.append(["N/A"])
        return custom_property_values

    @classmethod
    @renderer(renderer_type=LegacyDiagnosticRendererType.STATUS_ICON)
    def _diagnostic_status_icon_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> RenderedStringTemplateContent:
        assert result, "Must provide a result object."
        raised_exception: bool = False
        if "raised_exception" in result.exception_info:
            raised_exception = result.exception_info["raised_exception"]
        else:
            for k, v in result.exception_info.items():
                # TODO JT: This accounts for a dictionary of type {"metric_id": ExceptionInfo} path defined in  # noqa: E501
                #  validator._resolve_suite_level_graph_and_process_metric_evaluation_errors
                raised_exception = v["raised_exception"]
        if raised_exception:
            return RenderedStringTemplateContent(
                **{  # type: ignore[arg-type]
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "$icon",
                        "params": {"icon": "", "markdown_status_icon": "❗"},
                        "styling": {
                            "params": {
                                "icon": {
                                    "classes": [
                                        "fas",
                                        "fa-exclamation-triangle",
                                        "text-warning",
                                    ],
                                    "tag": "i",
                                }
                            }
                        },
                    },
                }
            )

        if result.success:
            return RenderedStringTemplateContent(
                **{  # type: ignore[arg-type]
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "$icon",
                        "params": {"icon": "", "markdown_status_icon": "✅"},
                        "styling": {
                            "params": {
                                "icon": {
                                    "classes": [
                                        "fas",
                                        "fa-check-circle",
                                        "text-success",
                                    ],
                                    "tag": "i",
                                }
                            }
                        },
                    },
                    "styling": {"parent": {"classes": ["hide-succeeded-validation-target-child"]}},
                }
            )
        else:
            return RenderedStringTemplateContent(
                **{  # type: ignore[arg-type]
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "$icon",
                        "params": {"icon": "", "markdown_status_icon": "❌"},
                        "styling": {
                            "params": {
                                "icon": {
                                    "tag": "i",
                                    "classes": ["fas", "fa-times", "text-danger"],
                                }
                            }
                        },
                    },
                }
            )

    @classmethod
    @renderer(renderer_type=LegacyDiagnosticRendererType.UNEXPECTED_STATEMENT)
    def _diagnostic_unexpected_statement_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> List[Union[RenderedStringTemplateContent, CollapseContent]]:
        assert result, "Must provide a result object."
        success: Optional[bool] = result.success
        result_dict: dict = result.result
        exception = {
            "raised_exception": False,
            "exception_message": "",
            "exception_traceback": "",
        }
        if "raised_exception" in result.exception_info:
            exception["raised_exception"] = result.exception_info["raised_exception"]
            exception["exception_message"] = result.exception_info["exception_message"]
            exception["exception_traceback"] = result.exception_info["exception_traceback"]
        else:
            for k, v in result.exception_info.items():
                # TODO JT: This accounts for a dictionary of type {"metric_id": ExceptionInfo} path defined in  # noqa: E501
                #  validator._resolve_suite_level_graph_and_process_metric_evaluation_errors
                exception["raised_exception"] = v["raised_exception"]
                exception["exception_message"] = v["exception_message"]
                exception["exception_traceback"] = v["exception_traceback"]
                # This only pulls the first exception message and traceback from a list of exceptions to render in the data docs.  # noqa: E501
                break

        if exception["raised_exception"]:
            exception_message_template_str = (
                "\n\n$expectation_type raised an exception:\n$exception_message"
            )

            if result.expectation_config is not None:
                expectation_type = result.expectation_config.type
            else:
                expectation_type = None

            exception_message = RenderedStringTemplateContent(
                **{  # type: ignore[arg-type]
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": exception_message_template_str,
                        "params": {
                            "expectation_type": expectation_type,
                            "exception_message": exception["exception_message"],
                        },
                        "tag": "strong",
                        "styling": {
                            "classes": ["text-danger"],
                            "params": {
                                "exception_message": {"tag": "code"},
                                "expectation_type": {"classes": ["badge", "badge-danger", "mb-2"]},
                            },
                        },
                    },
                }
            )

            exception_traceback_collapse = CollapseContent(
                **{  # type: ignore[arg-type]
                    "collapse_toggle_link": "Show exception traceback...",
                    "collapse": [
                        RenderedStringTemplateContent(
                            **{  # type: ignore[arg-type]
                                "content_block_type": "string_template",
                                "string_template": {
                                    "template": exception["exception_traceback"],
                                    "tag": "code",
                                },
                            }
                        )
                    ],
                }
            )

            return [exception_message, exception_traceback_collapse]

        if success or not result_dict.get("unexpected_count"):
            return []
        else:
            unexpected_count = num_to_str(
                result_dict["unexpected_count"], use_locale=True, precision=20
            )
            unexpected_percent = f"{num_to_str(result_dict['unexpected_percent'], precision=4)}%"
            element_count = num_to_str(result_dict["element_count"], use_locale=True, precision=20)

            template_str = (
                "\n\n$unexpected_count unexpected values found. "
                "$unexpected_percent of $element_count total rows."
            )

            return [
                RenderedStringTemplateContent(
                    **{  # type: ignore[arg-type]
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": template_str,
                            "params": {
                                "unexpected_count": unexpected_count,
                                "unexpected_percent": unexpected_percent,
                                "element_count": element_count,
                            },
                            "tag": "strong",
                            "styling": {"classes": ["text-danger"]},
                        },
                    }
                )
            ]

    @classmethod
    @renderer(renderer_type=LegacyDiagnosticRendererType.UNEXPECTED_TABLE)
    def _diagnostic_unexpected_table_renderer(  # noqa: C901, PLR0912
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> Optional[List[Union[RenderedTableContent, CollapseContent]]]:
        if result is None:
            return None

        result_dict: Optional[dict] = result.result

        if result_dict is None:
            return None

        if not result_dict.get("partial_unexpected_list") and not result_dict.get(
            "partial_unexpected_counts"
        ):
            return None
        table_rows: List[Any] = []

        partial_unexpected_counts: Optional[List[dict]] = result_dict.get(
            "partial_unexpected_counts"
        )
        # this means the result_format is COMPLETE and we have the full set of unexpected indices
        unexpected_index_list: Optional[List[dict]] = result_dict.get("unexpected_index_list")
        unexpected_count: int = result_dict["unexpected_count"]
        if partial_unexpected_counts:
            # We will check to see whether we have *all* of the unexpected values
            # accounted for in our count, and include counts if we do. If we do not,
            # we will use this as simply a better (non-repeating) source of
            # "sampled" unexpected values
            unexpected_list: Optional[List[dict]] = result_dict.get("unexpected_list")
            unexpected_index_column_names: Optional[List[str]] = result_dict.get(
                "unexpected_index_column_names"
            )
            if unexpected_index_list:
                header_row, table_rows = build_count_and_index_table(
                    partial_unexpected_counts=partial_unexpected_counts,
                    unexpected_index_list=unexpected_index_list,
                    unexpected_count=unexpected_count,
                    unexpected_list=unexpected_list,
                    unexpected_index_column_names=unexpected_index_column_names,
                )
            else:
                header_row, table_rows = build_count_table(
                    partial_unexpected_counts=partial_unexpected_counts,
                    unexpected_count=unexpected_count,
                )

        else:
            header_row = ["Sampled Unexpected Values"]
            sampled_values_set = set()
            partial_unexpected_list: Optional[List[Any]] = result_dict.get(
                "partial_unexpected_list"
            )
            if partial_unexpected_list:
                for unexpected_value in partial_unexpected_list:
                    if unexpected_value:
                        string_unexpected_value = str(unexpected_value)
                    elif unexpected_value == "":
                        string_unexpected_value = "EMPTY"
                    else:
                        string_unexpected_value = "null"
                    if string_unexpected_value not in sampled_values_set:
                        table_rows.append([unexpected_value])
                        sampled_values_set.add(string_unexpected_value)

        unexpected_table_content_block = RenderedTableContent(
            **{  # type: ignore[arg-type]
                "content_block_type": "table",
                "table": table_rows,
                "header_row": header_row,
                "styling": {"body": {"classes": ["table-bordered", "table-sm", "mt-3"]}},
            }
        )
        if result_dict.get("unexpected_index_query"):
            query = result_dict.get("unexpected_index_query")
            # in Pandas case, this is a list
            if not isinstance(query, str):
                query = str(query)
            query_info = CollapseContent(
                **{  # type: ignore[arg-type]
                    "collapse_toggle_link": "To retrieve all unexpected values...",
                    "collapse": [
                        RenderedStringTemplateContent(
                            **{  # type: ignore[arg-type]
                                "content_block_type": "string_template",
                                "string_template": {
                                    "template": query,
                                    "tag": "code",
                                },
                            }
                        )
                    ],
                }
            )
            return [unexpected_table_content_block, query_info]
        return [unexpected_table_content_block]

    @classmethod
    def _get_observed_value_from_evr(self, result: Optional[ExpectationValidationResult]) -> str:
        result_dict: Optional[dict] = None
        if result:
            result_dict = result.result
        if result_dict is None:
            return "--"

        observed_value: Any = result_dict.get("observed_value")
        unexpected_percent: Optional[float] = result_dict.get("unexpected_percent")
        if observed_value is not None:
            if isinstance(observed_value, (int, float)) and not isinstance(observed_value, bool):
                return num_to_str(observed_value, precision=10, use_locale=True)
            return str(observed_value)
        elif unexpected_percent is not None:
            return num_to_str(unexpected_percent, precision=5) + "% unexpected"
        else:
            return "--"

    @classmethod
    @renderer(renderer_type=AtomicDiagnosticRendererType.FAILED)
    def _diagnostic_failed(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> RenderedAtomicContent:
        renderer_configuration: RendererConfiguration = RendererConfiguration(
            configuration=configuration,
            result=result,
            runtime_configuration=runtime_configuration,
        )

        template_str = "Rendering failed for Expectation: "

        if renderer_configuration.expectation_type and renderer_configuration.kwargs:
            template_str += "$expectation_type(**$kwargs)."
        elif renderer_configuration.expectation_type:
            template_str += "$expectation_type."
        else:
            template_str = f"{template_str[:-2]}."

        renderer_configuration.add_param(
            name="expectation_type",
            param_type=RendererValueType.STRING,
            value=renderer_configuration.expectation_type,
        )
        renderer_configuration.add_param(
            name="kwargs",
            param_type=RendererValueType.STRING,
            value=renderer_configuration.kwargs,
        )

        value_obj = renderedAtomicValueSchema.load(
            {
                "template": template_str,
                "params": renderer_configuration.params.dict(),
                "schema": {"type": "com.superconductive.rendered.string"},
            }
        )
        rendered = RenderedAtomicContent(
            name=AtomicDiagnosticRendererType.FAILED,
            value=value_obj,
            value_type="StringValueType",
        )
        return rendered

    @classmethod
    @renderer(renderer_type=AtomicDiagnosticRendererType.OBSERVED_VALUE)
    def _atomic_diagnostic_observed_value(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> RenderedAtomicContent:
        observed_value: str = cls._get_observed_value_from_evr(result=result)
        value_obj = renderedAtomicValueSchema.load(
            {
                "template": observed_value,
                "params": {},
                "schema": {"type": "com.superconductive.rendered.string"},
            }
        )
        rendered = RenderedAtomicContent(
            name=AtomicDiagnosticRendererType.OBSERVED_VALUE,
            value=value_obj,
            value_type="StringValueType",
        )
        return rendered

    @classmethod
    @renderer(renderer_type=LegacyDiagnosticRendererType.OBSERVED_VALUE)
    def _diagnostic_observed_value_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> str:
        return cls._get_observed_value_from_evr(result=result)

    @classmethod
    def get_allowed_config_keys(cls) -> Union[Tuple[str, ...], Tuple[str]]:
        key_list: Union[list, List[str]] = []
        if len(cls.domain_keys) > 0:
            key_list.extend(list(cls.domain_keys))
        if len(cls.success_keys) > 0:
            key_list.extend(list(cls.success_keys))
        if len(cls.runtime_keys) > 0:
            key_list.extend(list(cls.runtime_keys))
        return tuple(str(key) for key in key_list)

    # noinspection PyUnusedLocal
    def metrics_validate(
        self,
        metrics: dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        **kwargs: dict,
    ) -> ExpectationValidationResult:
        if runtime_configuration is None:
            runtime_configuration = {}

        validation_dependencies: ValidationDependencies = self.get_validation_dependencies(
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )
        runtime_configuration["result_format"] = validation_dependencies.result_format

        validation_dependencies_metric_configurations: List[MetricConfiguration] = (
            validation_dependencies.get_metric_configurations()
        )

        _validate_dependencies_against_available_metrics(
            validation_dependencies=validation_dependencies_metric_configurations,
            metrics=metrics,
        )

        metric_name: str
        metric_configuration: MetricConfiguration
        provided_metrics: Dict[str, MetricValue] = {
            metric_name: metrics[metric_configuration.id]
            for metric_name, metric_configuration in validation_dependencies.metric_configurations.items()  # noqa: E501
        }

        expectation_validation_result: Union[ExpectationValidationResult, dict] = self._validate(
            metrics=provided_metrics,
            runtime_configuration=runtime_configuration,
            execution_engine=execution_engine,
        )

        result_format = parse_result_format(runtime_configuration.get("result_format", {}))
        if result_format.get("result_format") == ResultFormat.BOOLEAN_ONLY:
            if isinstance(expectation_validation_result, ExpectationValidationResult):
                expectation_validation_result.result = {}
            else:
                expectation_validation_result["result"] = {}

        evr: ExpectationValidationResult = self._build_evr(
            raw_response=expectation_validation_result,
        )
        return evr

    def _build_evr(
        self,
        raw_response: Union[ExpectationValidationResult, dict],
        **kwargs: dict,
    ) -> ExpectationValidationResult:
        """_build_evr is a lightweight convenience wrapper handling cases where an Expectation implementor
        fails to return an EVR but returns the necessary components in a dictionary."""  # noqa: E501
        configuration = self.configuration

        evr: ExpectationValidationResult
        if not isinstance(raw_response, ExpectationValidationResult):
            if isinstance(raw_response, dict):
                evr = ExpectationValidationResult(**raw_response)
                evr.expectation_config = configuration
            else:
                raise GreatExpectationsError("Unable to build EVR")  # noqa: TRY003
        else:
            raw_response_dict: dict = raw_response.to_json_dict()
            evr = ExpectationValidationResult(**raw_response_dict)
            evr.expectation_config = configuration
        return evr

    def get_validation_dependencies(
        self,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> ValidationDependencies:
        """Returns the result format and metrics required to validate this Expectation using the provided result format."""  # noqa: E501
        from great_expectations.validator.validator import ValidationDependencies

        runtime_configuration = self._get_runtime_kwargs(
            runtime_configuration=runtime_configuration,
        )
        result_format: dict = runtime_configuration["result_format"]
        result_format = parse_result_format(result_format=result_format)
        return ValidationDependencies(metric_configurations={}, result_format=result_format)

    def _get_default_value(self, key: str) -> Any:
        field = self.__fields__.get(key)

        if field is not None:
            return field.default if not field.required else None
        else:
            logger.info(f'_get_default_value called with key "{key}", but it is not a known field')
            return None

    def _get_domain_kwargs(self) -> Dict[str, Optional[str]]:
        domain_kwargs: Dict[str, Optional[str]] = {
            key: self.configuration.kwargs.get(key, self._get_default_value(key))
            for key in self.domain_keys
        }
        missing_kwargs: Union[set, Set[str]] = set(self.domain_keys) - set(domain_kwargs.keys())
        if missing_kwargs:
            raise InvalidExpectationKwargsError(f"Missing domain kwargs: {list(missing_kwargs)}")  # noqa: TRY003
        return domain_kwargs

    def _get_success_kwargs(self) -> Dict[str, Any]:
        domain_kwargs: Dict[str, Optional[str]] = self._get_domain_kwargs()
        success_kwargs: Dict[str, Any] = {
            key: self.configuration.kwargs.get(key, self._get_default_value(key))
            for key in self.success_keys
        }
        success_kwargs.update(domain_kwargs)
        return success_kwargs

    def _get_runtime_kwargs(
        self,
        runtime_configuration: Optional[dict] = None,
    ) -> dict:
        configuration = deepcopy(self.configuration)

        if runtime_configuration:
            configuration.kwargs.update(runtime_configuration)

        success_kwargs = self._get_success_kwargs()
        runtime_kwargs = {
            key: configuration.kwargs.get(key, self._get_default_value(key))
            for key in self.runtime_keys
        }
        runtime_kwargs.update(success_kwargs)

        runtime_kwargs["result_format"] = parse_result_format(runtime_kwargs["result_format"])

        return runtime_kwargs

    def _get_result_format(
        self,
        runtime_configuration: Optional[dict] = None,
    ) -> Union[Dict[str, Union[str, int, bool, List[str], None]], str]:
        default_result_format: Optional[Any] = self._get_default_value("result_format")
        configuration_result_format: Union[
            Dict[str, Union[str, int, bool, List[str], None]], str
        ] = self.configuration.kwargs.get("result_format", default_result_format)
        result_format: Union[Dict[str, Union[str, int, bool, List[str], None]], str]
        if runtime_configuration:
            result_format = runtime_configuration.get(
                "result_format",
                configuration_result_format,
            )
        else:
            result_format = configuration_result_format
        return result_format

    @public_api
    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        pass  # no-op

    # Renamed from validate due to collision with Pydantic method of the same name
    def validate_(
        self,
        validator: Validator,
        suite_parameters: Optional[dict] = None,
        interactive_evaluation: bool = True,
        data_context: Optional[AbstractDataContext] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> ExpectationValidationResult:
        """Validates the expectation against the provided data.

        Args:
            validator: A Validator object that can be used to create Expectations, validate Expectations,
                and get Metrics for Expectations.
            suite_parameters: Dictionary of dynamic values used during Validation of an Expectation.
            interactive_evaluation: Setting the interactive_evaluation flag on a DataAsset
                make it possible to declare expectations and store expectations without
                immediately evaluating them.
            data_context: An instance of a GX DataContext.
            runtime_configuration: The runtime configuration for the Expectation.
        Returns:
            An ExpectationValidationResult object
        """  # noqa: E501
        configuration = deepcopy(self.configuration)

        # issue warnings if necessary
        self._warn_if_result_format_config_in_runtime_configuration(
            runtime_configuration=runtime_configuration,
        )
        self._warn_if_result_format_config_in_expectation_configuration(configuration=configuration)

        configuration.process_suite_parameters(
            suite_parameters, interactive_evaluation, data_context
        )
        expectation_validation_result_list: list[ExpectationValidationResult] = (
            validator.graph_validate(
                configurations=[configuration],
                runtime_configuration=runtime_configuration,
            )
        )
        return expectation_validation_result_list[0]

    @property
    def suite_parameter_options(self) -> tuple[str, ...]:
        """SuiteParameter options for this Expectation.

        Returns:
            tuple[str, ...]: The keys of the suite parameters used in this Expectation at runtime.
        """
        output: set[str] = set()
        as_dict = self.dict(exclude_defaults=True)
        for value in as_dict.values():
            if is_suite_parameter(value):
                output.add(get_suite_parameter_key(value))
        return tuple(sorted(output))

    @property
    def configuration(self) -> ExpectationConfiguration:
        kwargs = self.dict(exclude_defaults=True)
        meta = kwargs.pop("meta", None)
        notes = kwargs.pop("notes", None)
        description = kwargs.pop("description", None)
        id = kwargs.pop("id", None)
        rendered_content = kwargs.pop("rendered_content", None)
        return ExpectationConfiguration(
            type=camel_to_snake(self.__class__.__name__),
            kwargs=kwargs,
            meta=meta,
            notes=notes,
            description=description,
            id=id,
            rendered_content=rendered_content,
        )

    @property
    def _include_rendered_content(self) -> bool:
        from great_expectations.data_context.data_context.context_factory import project_manager

        return project_manager.is_using_cloud()

    def __copy__(self):
        return self.copy(update={"id": None}, deep=True)

    @public_api
    def run_diagnostics(  # noqa: PLR0913
        self,
        raise_exceptions_for_backends: bool = False,
        ignore_suppress: bool = False,
        ignore_only_for: bool = False,
        for_gallery: bool = False,
        debug_logger: Optional[logging.Logger] = None,
        only_consider_these_backends: Optional[List[str]] = None,
        context: Optional[AbstractDataContext] = None,
    ) -> ExpectationDiagnostics:
        """Produce a diagnostic report about this Expectation.

        The current uses for this method's output are
        using the JSON structure to populate the Public Expectation Gallery
        and enabling a fast dev loop for developing new Expectations where the
        contributors can quickly check the completeness of their expectations.

        The contents of the report are captured in the ExpectationDiagnostics dataclass.
        You can see some examples in test_expectation_diagnostics.py

        Some components (e.g. description, examples, library_metadata) of the diagnostic report can be introspected directly from the Exepctation class.
        Other components (e.g. metrics, renderers, executions) are at least partly dependent on instantiating, validating, and/or executing the Expectation class.
        For these kinds of components, at least one test case with include_in_gallery=True must be present in the examples to
        produce the metrics, renderers and execution engines parts of the report. This is due to
        a get_validation_dependencies requiring expectation_config as an argument.

        If errors are encountered in the process of running the diagnostics, they are assumed to be due to
        incompleteness of the Expectation's implementation (e.g., declaring a dependency on Metrics
        that do not exist). These errors are added under "errors" key in the report.

        Args:
            raise_exceptions_for_backends: Bool object that when True will raise an Exception if a backend fails to connect.
            ignore_suppress:  Bool object that when True will ignore the suppress_test_for list on Expectation sample tests.
            ignore_only_for:  Bool object that when True will ignore the only_for list on Expectation sample tests.
            for_gallery:  Bool object that when True will create empty arrays to use as examples for the Expectation Diagnostics.
            debug_logger (optional[logging.Logger]):  Logger object to use for sending debug messages to.
            only_consider_these_backends (optional[List[str]])  List of backends to consider.
            context (optional[AbstractDataContext]): Instance of any child of "AbstractDataContext" class.

        Returns:
            An Expectation Diagnostics report object
        """  # noqa: E501
        from great_expectations.core.expectation_diagnostics.expectation_doctor import (
            ExpectationDoctor,
        )

        return ExpectationDoctor(self).run_diagnostics(
            raise_exceptions_for_backends=raise_exceptions_for_backends,
            ignore_suppress=ignore_suppress,
            ignore_only_for=ignore_only_for,
            for_gallery=for_gallery,
            debug_logger=debug_logger,
            only_consider_these_backends=only_consider_these_backends,
            context=context,
        )

    @public_api
    def print_diagnostic_checklist(
        self,
        diagnostics: Optional[ExpectationDiagnostics] = None,
        show_failed_tests: bool = False,
        backends: Optional[List[str]] = None,
        show_debug_messages: bool = False,
    ) -> str:
        """Runs self.run_diagnostics and generates a diagnostic checklist.

        This output from this method is a thin wrapper for ExpectationDiagnostics.generate_checklist()
        This method is experimental.

        Args:
            diagnostics (optional[ExpectationDiagnostics]): If diagnostics are not provided, diagnostics will be ran on self.
            show_failed_tests (bool): If true, failing tests will be printed.
            backends: list of backends to pass to run_diagnostics
            show_debug_messages (bool): If true, create a logger and pass to run_diagnostics
        """  # noqa: E501
        from great_expectations.core.expectation_diagnostics.expectation_doctor import (
            ExpectationDoctor,
        )

        return ExpectationDoctor(self).print_diagnostic_checklist(
            diagnostics=diagnostics,
            show_failed_tests=show_failed_tests,
            backends=backends,
            show_debug_messages=show_debug_messages,
        )

    def _warn_if_result_format_config_in_runtime_configuration(
        self, runtime_configuration: Union[dict, None] = None
    ) -> None:
        """
        Issues warning if result_format is in runtime_configuration for Validator
        """
        if runtime_configuration and runtime_configuration.get("result_format"):
            warnings.warn(
                "`result_format` configured at the Validator-level will not be persisted. Please add the configuration to your Checkpoint config or checkpoint_run() method instead.",  # noqa: E501
                UserWarning,
            )

    def _warn_if_result_format_config_in_expectation_configuration(
        self, configuration: ExpectationConfiguration
    ) -> None:
        """
        Issues warning if result_format is in ExpectationConfiguration
        """

        if configuration.kwargs.get("result_format"):
            warnings.warn(
                "`result_format` configured at the Expectation-level will not be persisted. Please add the configuration to your Checkpoint config or checkpoint_run() method instead.",  # noqa: E501
                UserWarning,
            )

    @staticmethod
    def _add_array_params(
        array_param_name: str,
        param_prefix: str,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        if not param_prefix:
            raise RendererConfigurationError("Array param_prefix must be a non-empty string.")  # noqa: TRY003

        @param_method(param_name=array_param_name)
        def _add_params(
            renderer_configuration: RendererConfiguration,
        ) -> RendererConfiguration:
            array: Sequence[Optional[Any]] = getattr(
                renderer_configuration.params, array_param_name
            ).value
            if array:
                for idx, value in enumerate(array):
                    if isinstance(value, Number):
                        param_type = RendererValueType.NUMBER
                    else:
                        param_type = RendererValueType.STRING
                    renderer_configuration.add_param(
                        name=f"{param_prefix}{idx!s}",
                        param_type=param_type,
                        value=value,
                    )
            return renderer_configuration

        return _add_params(renderer_configuration=renderer_configuration)

    @staticmethod
    def _get_array_string(
        array_param_name: str,
        param_prefix: str,
        renderer_configuration: RendererConfiguration,
    ) -> str:
        if not param_prefix:
            raise RendererConfigurationError("Array param_prefix must be a non-empty string.")  # noqa: TRY003

        @param_method(param_name=array_param_name)
        def _get_string(renderer_configuration: RendererConfiguration) -> str:
            array: Sequence[Optional[Any]] = getattr(
                renderer_configuration.params, array_param_name
            ).value
            if array:
                array_string = " ".join([f"${param_prefix}{idx!s}" for idx in range(len(array))])
            else:
                array_string = "[ ]"
            return array_string

        return _get_string(renderer_configuration=renderer_configuration)

    @staticmethod
    @param_method(param_name="mostly")
    def _add_mostly_pct_param(
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        mostly_pct_value: str = num_to_str(
            renderer_configuration.params.mostly.value * 100,
            no_scientific=True,
        )
        renderer_configuration.add_param(
            name="mostly_pct",
            param_type=RendererValueType.STRING,
            value=mostly_pct_value,
        )
        return renderer_configuration

    @staticmethod
    @param_method(param_name="strict_min")
    def _get_strict_min_string(renderer_configuration: RendererConfiguration) -> str:
        return (
            "greater than"
            if renderer_configuration.params.strict_min.value is True
            else "greater than or equal to"
        )

    @staticmethod
    @param_method(param_name="strict_max")
    def _get_strict_max_string(renderer_configuration: RendererConfiguration) -> str:
        return (
            "less than"
            if renderer_configuration.params.strict_max.value is True
            else "less than or equal to"
        )


class BatchExpectation(Expectation, ABC):
    """Base class for BatchExpectations.

    BatchExpectations answer a semantic question about a Batch of data.

    For example, `expect_table_column_count_to_equal` and `expect_table_row_count_to_equal` answer
    how many columns and rows are in your table.

    BatchExpectations must implement a `_validate(...)` method containing logic
    for determining whether the Expectation is successfully validated.

    BatchExpectations may optionally provide implementations of `validate_configuration`,
    which should raise an error if the configuration will not be usable for the Expectation.

    Raises:
        InvalidExpectationConfigurationError: The configuration does not contain the values required by the Expectation.

    Args:
        domain_keys (tuple): A tuple of the keys used to determine the domain of the
            expectation.
    """  # noqa: E501

    batch_id: Union[str, None] = None
    row_condition: Union[str, None] = None
    condition_parser: Union[str, None] = None

    domain_keys: ClassVar[Tuple[str, ...]] = (
        "batch_id",
        "table",
        "row_condition",
        "condition_parser",
    )
    metric_dependencies: ClassVar[Tuple[str, ...]] = ()
    domain_type: ClassVar[MetricDomainTypes] = MetricDomainTypes.TABLE
    args_keys: ClassVar[Tuple[str, ...]] = ()

    class Config:
        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type[BatchExpectation]) -> None:
            Expectation.Config.schema_extra(schema, model)
            schema["properties"]["metadata"]["properties"].update(
                {
                    "domain_type": {
                        "title": "Domain Type",
                        "type": "string",
                        "const": model.domain_type,
                        "description": "Batch",
                    }
                }
            )

    @override
    def get_validation_dependencies(
        self,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> ValidationDependencies:
        validation_dependencies: ValidationDependencies = super().get_validation_dependencies(
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )

        metric_name: str
        for metric_name in self.metric_dependencies:
            metric_kwargs = get_metric_kwargs(
                metric_name=metric_name,
                configuration=self.configuration,
                runtime_configuration=runtime_configuration,
            )
            validation_dependencies.set_metric_configuration(
                metric_name=metric_name,
                metric_configuration=MetricConfiguration(
                    metric_name=metric_name,
                    metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                    metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
                ),
            )

        return validation_dependencies

    def _validate_metric_value_between(  # noqa: C901, PLR0912
        self,
        metric_name,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ) -> Dict[str, Union[bool, Dict[str, Any]]]:
        metric_value: Optional[Any] = metrics.get(metric_name)

        if metric_value is None:
            return {"success": False, "result": {"observed_value": metric_value}}

        # Obtaining components needed for validation
        success_kwargs = self._get_success_kwargs()
        min_value: Optional[Any] = success_kwargs.get("min_value")
        strict_min: Optional[bool] = success_kwargs.get("strict_min")
        max_value: Optional[Any] = success_kwargs.get("max_value")
        strict_max: Optional[bool] = success_kwargs.get("strict_max")

        if not isinstance(metric_value, datetime.datetime) and pd.isnull(metric_value):
            return {"success": False, "result": {"observed_value": None}}

        if isinstance(metric_value, datetime.datetime):
            if isinstance(min_value, str):
                try:
                    min_value = parse(min_value)
                except TypeError:
                    raise ValueError(  # noqa: TRY003
                        f"""Could not parse "min_value" of {min_value} (of type "{type(min_value)!s}) into datetime \
representation."""  # noqa: E501
                    )

            if isinstance(max_value, str):
                try:
                    max_value = parse(max_value)
                except TypeError:
                    raise ValueError(  # noqa: TRY003
                        f"""Could not parse "max_value" of {max_value} (of type "{type(max_value)!s}) into datetime \
representation."""  # noqa: E501
                    )

        if isinstance(min_value, datetime.datetime) or isinstance(max_value, datetime.datetime):
            if not isinstance(metric_value, datetime.datetime):
                try:
                    metric_value = parse(metric_value)
                except TypeError:
                    raise ValueError(  # noqa: TRY003
                        f"""Could not parse "metric_value" of {metric_value} (of type "{type(metric_value)!s}) into datetime \
representation."""  # noqa: E501
                    )

        # Checking if mean lies between thresholds
        if min_value is not None:
            if strict_min:
                above_min = metric_value > min_value
            else:
                above_min = metric_value >= min_value
        else:
            above_min = True

        if max_value is not None:
            if strict_max:
                below_max = metric_value < max_value
            else:
                below_max = metric_value <= max_value
        else:
            below_max = True

        success = bool(above_min and below_max)

        return {"success": success, "result": {"observed_value": metric_value}}


class QueryExpectation(BatchExpectation, ABC):
    """Base class for QueryExpectations.

    QueryExpectations facilitate the execution of SQL or Spark-SQL queries as the core logic for an Expectation.

    QueryExpectations must implement a `_validate(...)` method containing logic for determining whether data returned by the executed query is successfully validated.

    Query Expectations may optionally provide implementations of:

    1. `validate_configuration`, which should raise an error if the configuration will not be usable for the Expectation.

    2. Data Docs rendering methods decorated with the @renderer decorator.

    QueryExpectations may optionally define a `query` attribute

    Doing so precludes the need to pass a query into the Expectation. This default will be overridden if a query is passed in.

    Args:
        domain_keys (tuple): A tuple of the keys used to determine the domain of the
            expectation.
        success_keys (tuple): A tuple of the keys used to determine the success of
            the expectation.
        runtime_keys (optional[tuple]): Optional. A tuple of the keys that can be used to control output but will
            not affect the actual success value of the expectation (such as result_format).
        query (optional[str]): Optional. A SQL or Spark-SQL query to be executed. If not provided, a query must be passed
            into the QueryExpectation.

    --Documentation--
        - https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_query_expectations
    """  # noqa: E501

    domain_keys: ClassVar[Tuple] = (
        "batch_id",
        "row_condition",
        "condition_parser",
    )

    @override
    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        """Raises an exception if the configuration is not viable for an expectation.

        Args:
              configuration: An ExpectationConfiguration

        Raises:
              InvalidExpectationConfigurationError: If no `query` is specified
              UserWarning: If query is not parameterized, and/or row_condition is passed.
        """
        super().validate_configuration(configuration=configuration)
        if not configuration:
            configuration = self.configuration

        query: Optional[Any] = configuration.kwargs.get("query") or self._get_default_value("query")
        row_condition: Optional[Any] = configuration.kwargs.get(
            "row_condition"
        ) or self._get_default_value("row_condition")

        try:
            assert (
                "query" in configuration.kwargs or query
            ), "'query' parameter is required for Query Expectations."
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))
        try:
            if not isinstance(query, str):
                raise TypeError(f"'query' must be a string, but your query is type: {type(query)}")  # noqa: TRY003, TRY301
            parsed_query: Set[str] = {
                x
                for x in re.split(", |\\(|\n|\\)| |/", query)
                if x.upper() and x.upper() not in valid_sql_tokens_and_types
            }
            assert "{batch}" in parsed_query, (
                "Your query appears to not be parameterized for a data asset. "
                "By not parameterizing your query with `{batch}`, "
                "you may not be validating against your intended data asset, or the expectation may fail."  # noqa: E501
            )
            assert all(re.match("{.*?}", x) for x in parsed_query), (
                "Your query appears to have hard-coded references to your data. "
                "By not parameterizing your query with `{batch}`, {col}, etc., "
                "you may not be validating against your intended data asset, or the expectation may fail."  # noqa: E501
            )
        except (TypeError, AssertionError) as e:
            warnings.warn(str(e), UserWarning)
        try:
            assert row_condition is None, (
                "`row_condition` is an experimental feature. "
                "Combining this functionality with QueryExpectations may result in unexpected behavior."  # noqa: E501
            )
        except AssertionError as e:
            warnings.warn(str(e), UserWarning)


class ColumnAggregateExpectation(BatchExpectation, ABC):
    """Base class for column aggregate Expectations.

    These types of Expectation produce an aggregate metric for a column, such as the mean, standard deviation,
    number of unique values, column type, etc.

    --Documentation--
        - https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_aggregate_expectations/

    Args:
     domain_keys (tuple): A tuple of the keys used to determine the domain of the
         expectation.
     success_keys (tuple): A tuple of the keys used to determine the success of
         the expectation.

         - A  "column" key is required for column expectations.

    Raises:
        InvalidExpectationConfigurationError: If no `column` is specified
    """  # noqa: E501

    column: StrictStr = Field(min_length=1, description=COLUMN_DESCRIPTION)

    domain_keys: ClassVar[Tuple[str, ...]] = (
        "batch_id",
        "table",
        "column",
        "row_condition",
        "condition_parser",
    )
    domain_type: ClassVar[MetricDomainTypes] = MetricDomainTypes.COLUMN

    class Config:
        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type[ColumnAggregateExpectation]) -> None:
            BatchExpectation.Config.schema_extra(schema, model)
            schema["properties"]["metadata"]["properties"].update(
                {
                    "domain_type": {
                        "title": "Domain Type",
                        "type": "string",
                        "const": model.domain_type,
                        "description": "Column Aggregate",
                    }
                }
            )


class ColumnMapExpectation(BatchExpectation, ABC):
    """Base class for ColumnMapExpectations.

    ColumnMapExpectations are evaluated for a column and ask a yes/no question about every row in the column.
    Based on the result, they then calculate the percentage of rows that gave a positive answer.
    If the percentage is high enough, the Expectation considers that data valid.

    ColumnMapExpectations must implement a `_validate(...)` method containing logic
    for determining whether the Expectation is successfully validated.

    ColumnMapExpectations may optionally provide implementations of `validate_configuration`,
    which should raise an error if the configuration will not be usable for the Expectation. By default,
    the `validate_configuration` method will return an error if `column` is missing from the configuration.

    Raises:
        InvalidExpectationConfigurationError: If `column` is missing from configuration.
    Args:
        domain_keys (tuple): A tuple of the keys used to determine the domain of the
            expectation.
        success_keys (tuple): A tuple of the keys used to determine the success of
            the expectation.
    """  # noqa: E501

    column: StrictStr = Field(min_length=1, description=COLUMN_DESCRIPTION)
    mostly: Mostly = 1.0  # type: ignore[assignment] # TODO: Fix in CORE-412

    catch_exceptions: bool = True

    map_metric: ClassVar[Optional[str]] = None
    domain_keys: ClassVar[Tuple[str, ...]] = (
        "batch_id",
        "table",
        "column",
        "row_condition",
        "condition_parser",
    )
    domain_type: ClassVar[MetricDomainTypes] = MetricDomainTypes.COLUMN
    success_keys: ClassVar[Tuple[str, ...]] = ("mostly",)

    class Config:
        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type[ColumnMapExpectation]) -> None:
            BatchExpectation.Config.schema_extra(schema, model)
            schema["properties"]["metadata"]["properties"].update(
                {
                    "domain_type": {
                        "title": "Domain Type",
                        "type": "string",
                        "const": model.domain_type,
                        "description": "Column Map",
                    }
                }
            )

    @classmethod
    @override
    def is_abstract(cls) -> bool:
        return not cls.map_metric or super().is_abstract()

    @override
    def get_validation_dependencies(
        self,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs: dict,
    ) -> ValidationDependencies:
        validation_dependencies: ValidationDependencies = super().get_validation_dependencies(
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )
        assert isinstance(
            self.map_metric, str
        ), "ColumnMapExpectation must override get_validation_dependencies or declare exactly one map_metric"  # noqa: E501
        assert (
            self.metric_dependencies == tuple()
        ), "ColumnMapExpectation must be configured using map_metric, and cannot have metric_dependencies declared."  # noqa: E501

        metric_kwargs: dict

        metric_kwargs = get_metric_kwargs(
            metric_name=f"column_values.nonnull.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
            configuration=self.configuration,
            runtime_configuration=runtime_configuration,
        )
        validation_dependencies.set_metric_configuration(
            metric_name=f"column_values.nonnull.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
            metric_configuration=MetricConfiguration(
                metric_name=f"column_values.nonnull.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            ),
        )

        metric_kwargs = get_metric_kwargs(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
            configuration=self.configuration,
            runtime_configuration=runtime_configuration,
        )
        validation_dependencies.set_metric_configuration(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
            metric_configuration=MetricConfiguration(
                metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            ),
        )

        metric_kwargs = get_metric_kwargs(
            metric_name="table.row_count",
            configuration=self.configuration,
            runtime_configuration=runtime_configuration,
        )
        validation_dependencies.set_metric_configuration(
            metric_name="table.row_count",
            metric_configuration=MetricConfiguration(
                metric_name="table.row_count",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            ),
        )

        result_format_str: Optional[str] = validation_dependencies.result_format.get(
            "result_format"
        )
        include_unexpected_rows: Optional[bool] = validation_dependencies.result_format.get(
            "include_unexpected_rows"
        )

        if result_format_str == ResultFormat.BOOLEAN_ONLY:
            return validation_dependencies

        metric_kwargs = get_metric_kwargs(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
            configuration=self.configuration,
            runtime_configuration=runtime_configuration,
        )
        validation_dependencies.set_metric_configuration(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
            metric_configuration=MetricConfiguration(
                metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            ),
        )

        if include_unexpected_rows:
            metric_kwargs = get_metric_kwargs(
                metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}",
                configuration=self.configuration,
                runtime_configuration=runtime_configuration,
            )
            validation_dependencies.set_metric_configuration(
                metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}",
                metric_configuration=MetricConfiguration(
                    metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}",
                    metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                    metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
                ),
            )

        if result_format_str == ResultFormat.BASIC:
            return validation_dependencies

        metric_kwargs = get_metric_kwargs(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_LIST.value}",
            configuration=self.configuration,
            runtime_configuration=runtime_configuration,
        )
        validation_dependencies.set_metric_configuration(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_LIST.value}",
            metric_configuration=MetricConfiguration(
                metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_LIST.value}",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            ),
        )
        metric_kwargs = get_metric_kwargs(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_QUERY.value}",
            configuration=self.configuration,
            runtime_configuration=runtime_configuration,
        )
        validation_dependencies.set_metric_configuration(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_QUERY.value}",
            metric_configuration=MetricConfiguration(
                metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_QUERY.value}",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            ),
        )

        return validation_dependencies

    @override
    def _validate(
        self,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        result_format: str | dict[str, Any] = self._get_result_format(
            runtime_configuration=runtime_configuration
        )

        include_unexpected_rows: bool
        unexpected_index_column_names: int | str | list[str] | None
        if isinstance(result_format, dict):
            include_unexpected_rows = result_format.get("include_unexpected_rows", False)
            unexpected_index_column_names = result_format.get("unexpected_index_column_names", None)
        else:
            include_unexpected_rows = False
            unexpected_index_column_names = None

        total_count: Optional[int] = metrics.get("table.row_count")
        null_count: Optional[int] = metrics.get(
            f"column_values.nonnull.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
        )
        unexpected_count: Optional[int] = metrics.get(
            f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
        )
        unexpected_values: Optional[List[Any]] = metrics.get(
            f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}"
        )
        unexpected_index_list: Optional[List[int]] = metrics.get(
            f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_LIST.value}"
        )
        unexpected_index_query: Optional[str] = metrics.get(
            f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_QUERY.value}"
        )

        unexpected_rows: pd.DataFrame | None = None
        if include_unexpected_rows:
            unexpected_rows = metrics.get(
                f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}"
            )

        if total_count is None or null_count is None:
            total_count = nonnull_count = 0
        else:
            nonnull_count = total_count - null_count

        if unexpected_count is None or total_count == 0 or nonnull_count == 0:
            # Vacuously true
            success = True
        else:
            success = _mostly_success(
                nonnull_count,
                unexpected_count,
                self._get_success_kwargs()["mostly"],
            )

        return _format_map_output(
            result_format=parse_result_format(result_format),
            success=success,
            element_count=total_count,
            nonnull_count=nonnull_count,
            unexpected_count=unexpected_count,
            unexpected_list=unexpected_values,
            unexpected_index_list=unexpected_index_list,
            unexpected_rows=unexpected_rows,
            unexpected_index_query=unexpected_index_query,
            unexpected_index_column_names=unexpected_index_column_names,
        )


class ColumnPairMapExpectation(BatchExpectation, ABC):
    """Base class for ColumnPairMapExpectations.

    ColumnPairMapExpectations are evaluated for a pair of columns and ask a yes/no question about the row-wise
    relationship between those two columns. Based on the result, they then calculate the percentage of rows
    that gave a positive answer. If the percentage is high enough, the Expectation considers that data valid.

    ColumnPairMapExpectations must implement a `_validate(...)` method containing logic
    for determining whether the Expectation is successfully validated.

    ColumnPairMapExpectations may optionally provide implementations of `validate_configuration`,
    which should raise an error if the configuration will not be usable for the Expectation. By default,
    the `validate_configuration` method will return an error if `column_A` and `column_B` are missing from the configuration.

    Raises:
        InvalidExpectationConfigurationError:  If `column_A` and `column_B` parameters are missing from the configuration.

    Args:
        domain_keys (tuple): A tuple of the keys used to determine the domain of the
            expectation.
        success_keys (tuple): A tuple of the keys used to determine the success of
            the expectation.
    """  # noqa: E501

    column_A: StrictStr = Field(min_length=1, description=COLUMN_A_DESCRIPTION)
    column_B: StrictStr = Field(min_length=1, description=COLUMN_B_DESCRIPTION)
    mostly: Mostly = 1.0  # type: ignore[assignment] # TODO: Fix in CORE-412

    catch_exceptions: bool = True

    map_metric: ClassVar[Optional[str]] = None
    domain_keys = (
        "batch_id",
        "table",
        "column_A",
        "column_B",
        "row_condition",
        "condition_parser",
    )
    domain_type: ClassVar[MetricDomainTypes] = MetricDomainTypes.COLUMN_PAIR
    success_keys: ClassVar[Tuple[str, ...]] = ("mostly",)

    class Config:
        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type[ColumnPairMapExpectation]) -> None:
            BatchExpectation.Config.schema_extra(schema, model)
            schema["properties"]["metadata"]["properties"].update(
                {
                    "domain_type": {
                        "title": "Domain Type",
                        "type": "string",
                        "const": model.domain_type,
                        "description": "Column Pair Map",
                    }
                }
            )

    @classmethod
    @override
    def is_abstract(cls) -> bool:
        return cls.map_metric is None or super().is_abstract()

    @override
    def get_validation_dependencies(
        self,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> ValidationDependencies:
        validation_dependencies: ValidationDependencies = super().get_validation_dependencies(
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )
        assert isinstance(
            self.map_metric, str
        ), "ColumnPairMapExpectation must override get_validation_dependencies or declare exactly one map_metric"  # noqa: E501
        assert (
            self.metric_dependencies == tuple()
        ), "ColumnPairMapExpectation must be configured using map_metric, and cannot have metric_dependencies declared."  # noqa: E501
        metric_kwargs: dict

        configuration = self.configuration

        metric_kwargs = get_metric_kwargs(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        validation_dependencies.set_metric_configuration(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
            metric_configuration=MetricConfiguration(
                metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            ),
        )

        metric_kwargs = get_metric_kwargs(
            metric_name="table.row_count",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        validation_dependencies.set_metric_configuration(
            metric_name="table.row_count",
            metric_configuration=MetricConfiguration(
                metric_name="table.row_count",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            ),
        )

        metric_kwargs = get_metric_kwargs(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.FILTERED_ROW_COUNT.value}",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        validation_dependencies.set_metric_configuration(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.FILTERED_ROW_COUNT.value}",
            metric_configuration=MetricConfiguration(
                metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.FILTERED_ROW_COUNT.value}",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            ),
        )

        result_format_str: Optional[str] = validation_dependencies.result_format.get(
            "result_format"
        )
        include_unexpected_rows: Optional[bool] = validation_dependencies.result_format.get(
            "include_unexpected_rows"
        )

        if result_format_str == ResultFormat.BOOLEAN_ONLY:
            return validation_dependencies

        metric_kwargs = get_metric_kwargs(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        validation_dependencies.set_metric_configuration(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
            metric_configuration=MetricConfiguration(
                metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            ),
        )

        if include_unexpected_rows:
            metric_kwargs = get_metric_kwargs(
                metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}",
                configuration=configuration,
                runtime_configuration=runtime_configuration,
            )
            validation_dependencies.set_metric_configuration(
                metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}",
                metric_configuration=MetricConfiguration(
                    metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}",
                    metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                    metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
                ),
            )

        if result_format_str == ResultFormat.BASIC:
            return validation_dependencies

        metric_kwargs = get_metric_kwargs(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_LIST.value}",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        validation_dependencies.set_metric_configuration(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_LIST.value}",
            metric_configuration=MetricConfiguration(
                metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_LIST.value}",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            ),
        )
        metric_kwargs = get_metric_kwargs(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_QUERY.value}",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        validation_dependencies.set_metric_configuration(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_QUERY.value}",
            metric_configuration=MetricConfiguration(
                metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_QUERY.value}",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            ),
        )
        return validation_dependencies

    @override
    def _validate(
        self,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        result_format: Union[Dict[str, Union[int, str, bool, List[str], None]], str] = (
            self._get_result_format(runtime_configuration=runtime_configuration)
        )

        unexpected_index_column_names = None
        if isinstance(result_format, dict):
            unexpected_index_column_names = result_format.get("unexpected_index_column_names", None)
        total_count: Optional[int] = metrics.get("table.row_count")
        unexpected_count: Optional[int] = metrics.get(
            f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
        )
        unexpected_values: Optional[Any] = metrics.get(
            f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}"
        )
        unexpected_index_list: Optional[List[int]] = metrics.get(
            f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_LIST.value}"
        )
        unexpected_index_query: Optional[str] = metrics.get(
            f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_QUERY.value}"
        )
        filtered_row_count: Optional[int] = metrics.get(
            f"{self.map_metric}.{SummarizationMetricNameSuffixes.FILTERED_ROW_COUNT.value}"
        )

        if (
            total_count is None
            or unexpected_count is None
            or filtered_row_count is None
            or total_count == 0
            or filtered_row_count == 0
        ):
            # Vacuously true
            success = True
        else:
            success = _mostly_success(
                filtered_row_count,
                unexpected_count,
                self._get_success_kwargs()["mostly"],
            )

        return _format_map_output(
            result_format=parse_result_format(result_format),
            success=success,
            element_count=total_count,
            nonnull_count=filtered_row_count,
            unexpected_count=unexpected_count,
            unexpected_list=unexpected_values,
            unexpected_index_list=unexpected_index_list,
            unexpected_index_query=unexpected_index_query,
            unexpected_index_column_names=unexpected_index_column_names,
        )


class MulticolumnMapExpectation(BatchExpectation, ABC):
    """Base class for MulticolumnMapExpectations.

    MulticolumnMapExpectations are evaluated for a set of columns and ask a yes/no question about the
    row-wise relationship between those columns. Based on the result, they then calculate the
    percentage of rows that gave a positive answer. If the percentage is high enough,
    the Expectation considers that data valid.

    MulticolumnMapExpectations must implement a `_validate(...)` method containing logic
    for determining whether the Expectation is successfully validated.

    MulticolumnMapExpectations may optionally provide implementations of `validate_configuration`,
    which should raise an error if the configuration will not be usable for the Expectation. By default,
    the `validate_configuration` method will return an error if `column_list` is missing from the configuration.

    Raises:
        InvalidExpectationConfigurationError: If `column_list` is missing from configuration.

    Args:
        domain_keys (tuple): A tuple of the keys used to determine the domain of the
            expectation.
        success_keys (tuple): A tuple of the keys used to determine the success of
            the expectation.
    """  # noqa: E501

    column_list: List[StrictStr] = pydantic.Field(description=COLUMN_LIST_DESCRIPTION)
    mostly: Mostly = pydantic.Field(default=1.0, description=MOSTLY_DESCRIPTION)

    ignore_row_if: Literal["all_values_are_missing", "any_value_is_missing", "never"] = (
        "all_values_are_missing"
    )
    catch_exceptions: bool = True

    map_metric: ClassVar[Optional[str]] = None
    domain_keys = (
        "batch_id",
        "table",
        "column_list",
        "row_condition",
        "condition_parser",
        "ignore_row_if",
    )
    domain_type: ClassVar[MetricDomainTypes] = MetricDomainTypes.MULTICOLUMN
    success_keys = ("mostly",)

    class Config:
        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type[MulticolumnMapExpectation]) -> None:
            BatchExpectation.Config.schema_extra(schema, model)
            schema["properties"]["metadata"]["properties"].update(
                {
                    "domain_type": {
                        "title": "Domain Type",
                        "type": "string",
                        "const": model.domain_type,
                        "description": "Multicolumn Map",
                    }
                }
            )

    @classmethod
    @override
    def is_abstract(cls) -> bool:
        return cls.map_metric is None or super().is_abstract()

    @override
    def get_validation_dependencies(
        self,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> ValidationDependencies:
        validation_dependencies: ValidationDependencies = super().get_validation_dependencies(
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )
        assert isinstance(
            self.map_metric, str
        ), "MulticolumnMapExpectation must override get_validation_dependencies or declare exactly one map_metric"  # noqa: E501
        assert (
            self.metric_dependencies == tuple()
        ), "MulticolumnMapExpectation must be configured using map_metric, and cannot have metric_dependencies declared."  # noqa: E501
        # convenient name for updates

        configuration = self.configuration

        metric_kwargs = get_metric_kwargs(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        validation_dependencies.set_metric_configuration(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
            metric_configuration=MetricConfiguration(
                metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            ),
        )

        metric_kwargs = get_metric_kwargs(
            metric_name="table.row_count",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        validation_dependencies.set_metric_configuration(
            metric_name="table.row_count",
            metric_configuration=MetricConfiguration(
                metric_name="table.row_count",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            ),
        )

        metric_kwargs = get_metric_kwargs(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.FILTERED_ROW_COUNT.value}",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        validation_dependencies.set_metric_configuration(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.FILTERED_ROW_COUNT.value}",
            metric_configuration=MetricConfiguration(
                metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.FILTERED_ROW_COUNT.value}",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            ),
        )

        result_format_str: Optional[str] = validation_dependencies.result_format.get(
            "result_format"
        )
        include_unexpected_rows: Optional[bool] = validation_dependencies.result_format.get(
            "include_unexpected_rows"
        )

        if result_format_str == ResultFormat.BOOLEAN_ONLY:
            return validation_dependencies

        metric_kwargs = get_metric_kwargs(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        validation_dependencies.set_metric_configuration(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
            metric_configuration=MetricConfiguration(
                metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            ),
        )

        if result_format_str == ResultFormat.BASIC:
            return validation_dependencies

        if include_unexpected_rows:
            metric_kwargs = get_metric_kwargs(
                metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}",
                configuration=configuration,
                runtime_configuration=runtime_configuration,
            )
            validation_dependencies.set_metric_configuration(
                metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}",
                metric_configuration=MetricConfiguration(
                    metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}",
                    metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                    metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
                ),
            )

        from great_expectations.execution_engine import (
            SqlAlchemyExecutionEngine,
        )

        # ID/PK currently doesn't work for ExpectCompoundColumnsToBeUnique in SQL
        if self.map_metric == "compound_columns.unique" and isinstance(
            execution_engine, SqlAlchemyExecutionEngine
        ):
            return validation_dependencies

        metric_kwargs = get_metric_kwargs(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_LIST.value}",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        validation_dependencies.set_metric_configuration(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_LIST.value}",
            metric_configuration=MetricConfiguration(
                metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_LIST.value}",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            ),
        )
        metric_kwargs = get_metric_kwargs(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_QUERY.value}",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        validation_dependencies.set_metric_configuration(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_QUERY.value}",
            metric_configuration=MetricConfiguration(
                metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_QUERY.value}",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            ),
        )

        return validation_dependencies

    @override
    def _validate(
        self,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        result_format = self._get_result_format(runtime_configuration=runtime_configuration)
        unexpected_index_column_names = None
        if isinstance(result_format, dict):
            unexpected_index_column_names = result_format.get("unexpected_index_column_names", None)

        total_count: Optional[int] = metrics.get("table.row_count")
        unexpected_count: Optional[int] = metrics.get(
            f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
        )
        unexpected_values: Optional[Any] = metrics.get(
            f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}"
        )
        unexpected_index_list: Optional[List[int]] = metrics.get(
            f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_LIST.value}"
        )
        filtered_row_count: Optional[int] = metrics.get(
            f"{self.map_metric}.{SummarizationMetricNameSuffixes.FILTERED_ROW_COUNT.value}"
        )
        unexpected_index_query: Optional[str] = metrics.get(
            f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_QUERY.value}"
        )

        if (
            total_count is None
            or unexpected_count is None
            or filtered_row_count is None
            or total_count == 0
            or filtered_row_count == 0
        ):
            # Vacuously true
            success = True
        else:
            success = _mostly_success(
                filtered_row_count,
                unexpected_count,
                self._get_success_kwargs()["mostly"],
            )

        return _format_map_output(
            result_format=parse_result_format(result_format),
            success=success,
            element_count=total_count,
            nonnull_count=filtered_row_count,
            unexpected_count=unexpected_count,
            unexpected_list=unexpected_values,
            unexpected_index_list=unexpected_index_list,
            unexpected_index_query=unexpected_index_query,
            unexpected_index_column_names=unexpected_index_column_names,
        )


class UnexpectedRowsExpectation:
    unexpected_rows_query: str
    description: str | None = None

    def __new__(
        cls,
        unexpected_rows_query: str | None = None,
        description: str | None = None,
    ):
        # deprecated-v1.0.2
        warnings.warn(
            "Importing UnexpectedRowsExpectation from great_expectations.expectations.expectation "
            "is deprecated. Please import UnexpectedRowsExpectation from "
            "great_expectations.expectations instead.",
            category=DeprecationWarning,
        )
        from great_expectations.expectations import (
            UnexpectedRowsExpectation as CoreUnexpectedRowsExpectation,
        )

        return CoreUnexpectedRowsExpectation(
            unexpected_rows_query=unexpected_rows_query or cls.unexpected_rows_query,
            description=description or cls.description,
        )


def _format_map_output(  # noqa: C901, PLR0912, PLR0913, PLR0915
    result_format: dict,
    success: bool,
    element_count: Optional[int] = None,
    nonnull_count: Optional[int] = None,
    unexpected_count: Optional[int] = None,
    unexpected_list: Optional[List[Any]] = None,
    unexpected_index_list: Optional[List[int]] = None,
    unexpected_index_query: Optional[str] = None,
    # Actually Optional[List[str]], but this is necessary to keep the typechecker happy
    unexpected_index_column_names: Optional[Union[int, str, List[str]]] = None,
    unexpected_rows: pd.DataFrame | None = None,
) -> Dict:
    """Helper function to construct expectation result objects for map_expectations (such as column_map_expectation
    and file_lines_map_expectation).

    Expectations support four result_formats: BOOLEAN_ONLY, BASIC, SUMMARY, and COMPLETE.
    In each case, the object returned has a different set of populated fields.
    See :ref:`result_format` for more information.

    This function handles the logic for mapping those fields for column_map_expectations.
    """  # noqa: E501
    if element_count is None:
        element_count = 0

    # NB: unexpected_count parameter is explicit some implementing classes may limit the length of unexpected_list  # noqa: E501
    # Incrementally add to result and return when all values for the specified level are present
    return_obj: Dict[str, Any] = {"success": success}

    if result_format["result_format"] == ResultFormat.BOOLEAN_ONLY:
        return return_obj

    skip_missing = False
    missing_count: Optional[int] = None
    if nonnull_count is None:
        skip_missing = True
    else:
        missing_count = element_count - nonnull_count

    missing_percent: Optional[float] = None
    unexpected_percent_total: Optional[float] = None
    unexpected_percent_nonmissing: Optional[float] = None
    if unexpected_count is not None and element_count > 0:
        unexpected_percent_total = unexpected_count / element_count * 100

        if not skip_missing and missing_count is not None:
            missing_percent = missing_count / element_count * 100
            if nonnull_count is not None and nonnull_count > 0:
                unexpected_percent_nonmissing = unexpected_count / nonnull_count * 100
            else:
                unexpected_percent_nonmissing = None
        else:
            unexpected_percent_nonmissing = unexpected_percent_total

    return_obj["result"] = {
        "element_count": element_count,
        "unexpected_count": unexpected_count,
        "unexpected_percent": unexpected_percent_nonmissing,
    }

    exclude_unexpected_values = result_format.get("exclude_unexpected_values", False)
    if unexpected_list is not None and not exclude_unexpected_values:
        return_obj["result"]["partial_unexpected_list"] = unexpected_list[
            : result_format["partial_unexpected_count"]
        ]

    if unexpected_index_column_names is not None:
        return_obj["result"].update(
            {"unexpected_index_column_names": unexpected_index_column_names}
        )

    if not skip_missing:
        return_obj["result"]["missing_count"] = missing_count
        return_obj["result"]["missing_percent"] = missing_percent
        return_obj["result"]["unexpected_percent_total"] = unexpected_percent_total
        return_obj["result"]["unexpected_percent_nonmissing"] = unexpected_percent_nonmissing

    if result_format["include_unexpected_rows"]:
        if unexpected_rows is not None:
            if isinstance(unexpected_rows, pd.DataFrame):
                unexpected_rows = unexpected_rows.head(result_format["partial_unexpected_count"])
            elif isinstance(unexpected_rows, list):
                unexpected_rows = unexpected_rows[: result_format["partial_unexpected_count"]]
        return_obj["result"].update(
            {
                "unexpected_rows": unexpected_rows,
            }
        )

    if result_format["result_format"] == ResultFormat.BASIC:
        return return_obj

    if unexpected_list is not None and not exclude_unexpected_values:
        if len(unexpected_list) and isinstance(unexpected_list[0], dict):
            # in the case of multicolumn map expectations `unexpected_list` contains dicts,
            # which will throw an exception when we hash it to count unique members.
            # As a workaround, we flatten the values out to tuples.
            immutable_unexpected_list = [
                tuple([val for val in item.values()]) for item in unexpected_list
            ]
        else:
            immutable_unexpected_list = unexpected_list

    # Try to return the most common values, if possible.
    partial_unexpected_count: Optional[int] = result_format.get("partial_unexpected_count")
    partial_unexpected_counts: Optional[List[Dict[str, Any]]] = None
    if partial_unexpected_count is not None and partial_unexpected_count > 0:
        try:
            if not exclude_unexpected_values:
                partial_unexpected_counts = [
                    {"value": key, "count": value}
                    for key, value in sorted(
                        Counter(immutable_unexpected_list).most_common(  # type: ignore[possibly-undefined] # FIXME
                            result_format["partial_unexpected_count"]
                        ),
                        key=lambda x: (-x[1], x[0]),
                    )
                ]
                return_obj["result"].update(
                    {"partial_unexpected_counts": partial_unexpected_counts}
                )
        except TypeError:
            partial_unexpected_counts = [
                {"error": "partial_exception_counts requires a hashable type"}
            ]
        finally:
            if unexpected_index_list is not None:
                return_obj["result"].update(
                    {
                        "partial_unexpected_index_list": unexpected_index_list[
                            : result_format["partial_unexpected_count"]
                        ],
                    }
                )

    if result_format["result_format"] == ResultFormat.SUMMARY:
        return return_obj

    if unexpected_list is not None and not exclude_unexpected_values:
        return_obj["result"].update({"unexpected_list": unexpected_list})
    if unexpected_index_list is not None:
        return_obj["result"].update({"unexpected_index_list": unexpected_index_list})
    if unexpected_index_query is not None:
        return_obj["result"].update({"unexpected_index_query": unexpected_index_query})
    if result_format["result_format"] == ResultFormat.COMPLETE:
        return return_obj

    raise ValueError(f"Unknown result_format {result_format['result_format']}.")  # noqa: TRY003


def _validate_dependencies_against_available_metrics(
    validation_dependencies: List[MetricConfiguration],
    metrics: dict,
) -> None:
    """Check that validation_dependencies for current Expectations are available as Metrics.

    Args:
        validation_dependencies_as_metric_configurations: dependencies calculated for current Expectation.
        metrics: dict of metrics available to current Expectation.

    Raises:
        InvalidExpectationConfigurationError: If a validation dependency is not available as a Metric.
    """  # noqa: E501
    for metric_config in validation_dependencies:
        if metric_config.id not in metrics:
            raise InvalidExpectationConfigurationError(  # noqa: TRY003
                f"Metric {metric_config.id} is not available for validation of configuration. Please check your configuration."  # noqa: E501
            )


def _mostly_success(
    rows_considered_cnt: int,
    unexpected_cnt: int,
    mostly: float,
) -> bool:
    rows_considered_cnt_as_float: float = float(rows_considered_cnt)
    unexpected_cnt_as_float: float = float(unexpected_cnt)
    success_ratio: float = (
        rows_considered_cnt_as_float - unexpected_cnt_as_float
    ) / rows_considered_cnt_as_float
    return success_ratio >= mostly


def add_values_with_json_schema_from_list_in_params(
    params: dict,
    params_with_json_schema: dict,
    param_key_with_list: str,
    list_values_type: str = "string",
) -> dict:
    """
    Utility function used in _atomic_prescriptive_template() to take list values from a given params dict key,
    convert each value to a dict with JSON schema type info, then add it to params_with_json_schema (dict).
    """  # noqa: E501
    # deprecated-v0.15.43
    warnings.warn(
        "The method add_values_with_json_schema_from_list_in_params is deprecated as of v0.15.43 and will be removed in "  # noqa: E501
        "v0.18. Please refer to Expectation method _prescriptive_template for the latest renderer template pattern.",  # noqa: E501
        DeprecationWarning,
    )
    target_list = params.get(param_key_with_list)
    if target_list is not None and len(target_list) > 0:
        for i, v in enumerate(target_list):
            params_with_json_schema[f"v__{i!s}"] = {
                "schema": {"type": list_values_type},
                "value": v,
            }
    return params_with_json_schema
