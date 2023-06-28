from __future__ import annotations

import datetime
import functools
import glob
import json
import logging
import os
import re
import sys
import time
import traceback
import warnings
from abc import ABC, ABCMeta, abstractmethod
from collections import Counter, defaultdict
from copy import deepcopy
from inspect import isabstract
from numbers import Number
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
)

import pandas as pd
from dateutil.parser import parse

from great_expectations import __version__ as ge_version
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.expectation_configuration import (
    ExpectationConfiguration,
    parse_result_format,
)
from great_expectations.core.expectation_diagnostics.expectation_diagnostics import (
    ExpectationDiagnostics,
)
from great_expectations.core.expectation_diagnostics.expectation_test_data_cases import (
    ExpectationLegacyTestCaseAdapter,
    ExpectationTestCase,
    ExpectationTestDataCases,
    TestBackend,
    TestData,
)
from great_expectations.core.expectation_diagnostics.supporting_types import (
    AugmentedLibraryMetadata,
    ExpectationBackendTestResultCounts,
    ExpectationDescriptionDiagnostics,
    ExpectationDiagnosticMaturityMessages,
    ExpectationErrorDiagnostics,
    ExpectationExecutionEngineDiagnostics,
    ExpectationMetricDiagnostics,
    ExpectationRendererDiagnostics,
    ExpectationTestDiagnostics,
    Maturity,
    RendererTestDiagnostics,
)
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.metric_function_types import (
    SummarizationMetricNameSuffixes,
)
from great_expectations.core.util import nested_update
from great_expectations.exceptions import (
    ExpectationNotFoundError,
    GreatExpectationsError,
    InvalidExpectationConfigurationError,
    InvalidExpectationKwargsError,
)
from great_expectations.execution_engine import (
    ExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.registry import (
    _registered_metrics,
    _registered_renderers,
    get_expectation_impl,
    get_metric_kwargs,
    register_expectation,
    register_renderer,
)
from great_expectations.expectations.sql_tokens_and_types import (
    valid_sql_tokens_and_types,
)
from great_expectations.render import (
    AtomicDiagnosticRendererType,
    AtomicPrescriptiveRendererType,
    CollapseContent,
    LegacyDiagnosticRendererType,
    LegacyRendererType,
    RenderedAtomicContent,
    RenderedContentBlockContainer,
    RenderedGraphContent,
    RenderedStringTemplateContent,
    RenderedTableContent,
    ValueListContent,
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
from great_expectations.self_check.util import (
    evaluate_json_test_v3_api,
    generate_dataset_name_from_expectation_name,
    generate_expectation_tests,
)
from great_expectations.util import camel_to_snake, is_parseable_date
from great_expectations.validator.computed_metric import MetricValue  # noqa: TCH001
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.validator import ValidationDependencies, Validator
from great_expectations.warnings import warn_deprecated_parse_strings_as_datetimes

if TYPE_CHECKING:
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.render.renderer_configuration import MetaNotes
    from great_expectations.rule_based_profiler.config import RuleBasedProfilerConfig

logger = logging.getLogger(__name__)


_TEST_DEFS_DIR = os.path.join(  # noqa: PTH118
    os.path.dirname(__file__),  # noqa: PTH120
    "..",
    "..",
    "tests",
    "test_definitions",
)


@public_api
def render_evaluation_parameter_string(render_func) -> Callable:
    """Decorator for Expectation classes that renders evaluation parameters as strings.

    allows Expectations that use Evaluation Parameters to render the values
    of the Evaluation Parameters along with the rest of the output.

    Args:
        render_func: The render method of the Expectation class.

    Raises:
        GreatExpectationsError: If runtime_configuration with evaluation_parameters is not provided.
    """

    def inner_func(
        *args: Tuple[MetaExpectation], **kwargs: dict
    ) -> Union[List[RenderedStringTemplateContent], RenderedAtomicContent]:
        rendered_string_template: Union[
            List[RenderedStringTemplateContent], RenderedAtomicContent
        ] = render_func(*args, **kwargs)
        current_expectation_params = list()
        app_template_str = (
            "\n - $eval_param = $eval_param_value (at time of validation)."
        )
        configuration: Optional[dict] = kwargs.get("configuration")
        if configuration:
            kwargs_dict: dict = configuration.get("kwargs", {})
            for key, value in kwargs_dict.items():
                if isinstance(value, dict) and "$PARAMETER" in value.keys():
                    current_expectation_params.append(value["$PARAMETER"])

        # if expectation configuration has no eval params, then don't look for the values in runtime_configuration
        # isinstance check should be removed upon implementation of RenderedAtomicContent evaluation parameter support
        if current_expectation_params and not isinstance(
            rendered_string_template, RenderedAtomicContent
        ):
            runtime_configuration: Optional[dict] = kwargs.get("runtime_configuration")
            if runtime_configuration:
                eval_params = runtime_configuration.get("evaluation_parameters", {})
                styling = runtime_configuration.get("styling")
                for key, val in eval_params.items():
                    for param in current_expectation_params:
                        # "key in param" condition allows for eval param values to be rendered if arithmetic is present
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
                raise GreatExpectationsError(
                    f"""GX was not able to render the value of evaluation parameters.
                        Expectation {render_func} had evaluation parameters set, but they were not passed in."""
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
    """
    if not param_name:
        # If param_name was passed as an empty string
        raise RendererConfigurationError(
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
                raise RendererConfigurationError(
                    "Methods decorated with @param_method must have an annotated return "
                    f"type, but method {method_name} does not."
                )

            if hasattr(renderer_configuration.params, param_name):
                if getattr(renderer_configuration.params, param_name, None):
                    return_obj = param_func(
                        renderer_configuration=renderer_configuration
                    )
                else:
                    if return_type is RendererConfiguration:  # noqa: PLR5501
                        return_obj = renderer_configuration
                    else:
                        return_obj = None
            else:
                raise RendererConfigurationError(
                    f"RendererConfiguration.param does not have a param called {param_name}. "
                    f'Use RendererConfiguration.add_param() with name="{param_name}" to add it.'
                )
            return return_obj

        return wrapper

    return _param_method


# noinspection PyMethodParameters
class MetaExpectation(ABCMeta):
    """MetaExpectation registers Expectations as they are defined, adding them to the Expectation registry.

    Any class inheriting from Expectation will be registered based on the value of the "expectation_type" class
    attribute, or, if that is not set, by snake-casing the name of the class.
    """

    default_kwarg_values: Dict[str, object] = {}

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
        default_kwarg_values = {}
        for base in reversed(bases):
            default_kwargs = getattr(base, "default_kwarg_values", {})
            default_kwarg_values = nested_update(default_kwarg_values, default_kwargs)

        newclass.default_kwarg_values = nested_update(
            default_kwarg_values, attrs.get("default_kwarg_values", {})
        )
        return newclass


@public_api
class Expectation(metaclass=MetaExpectation):
    """Base class for all Expectations.

    Expectation classes *must* have the following attributes set:
        1. `domain_keys`: a tuple of the *keys* used to determine the domain of the
           expectation
        2. `success_keys`: a tuple of the *keys* used to determine the success of
           the expectation.

    In some cases, subclasses of Expectation (such as BatchExpectation) can
    inherit these properties from their parent class.

    They *may* optionally override `runtime_keys` and `default_kwarg_values`, and
    may optionally set an explicit value for expectation_type.
        1. runtime_keys lists the keys that can be used to control output but will
           not affect the actual success value of the expectation (such as result_format).
        2. default_kwarg_values is a dictionary that will be used to fill unspecified
           kwargs from the Expectation Configuration.

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

    version = ge_version
    domain_keys: Tuple[str, ...] = ()
    success_keys: Tuple[str, ...] = ()
    runtime_keys: Tuple[str, ...] = (
        "include_config",
        "catch_exceptions",
        "result_format",
    )
    default_kwarg_values: dict[
        str, bool | str | float | RuleBasedProfilerConfig | None
    ] = {
        "include_config": True,
        "catch_exceptions": False,
        "result_format": "BASIC",
    }
    args_keys: Tuple[str, ...] = ()

    expectation_type: str
    examples: List[dict] = []

    def __init__(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        if configuration:
            self.validate_configuration(configuration=configuration)

        self._configuration = configuration

    @classmethod
    def is_abstract(cls) -> bool:
        return isabstract(cls)

    @classmethod
    def _register_renderer_functions(cls) -> None:
        expectation_type: str = camel_to_snake(cls.__name__)

        for candidate_renderer_fn_name in dir(cls):
            attr_obj: Callable = getattr(cls, candidate_renderer_fn_name)
            if not hasattr(attr_obj, "_renderer_type"):
                continue
            register_renderer(
                object_name=expectation_type, parent_class=cls, renderer_fn=attr_obj
            )

    @abstractmethod
    def _validate(
        self,
        configuration: ExpectationConfiguration,
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
        """
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
            raise ValueError(
                "RendererConfiguration does not contain an expectation_type."
            )

        add_param_args = (
            (
                "expectation_type",
                RendererValueType.STRING,
                renderer_configuration.expectation_type,
            ),
            ("kwargs", RendererValueType.STRING, renderer_configuration.kwargs),
        )
        for name, param_type, value in add_param_args:
            renderer_configuration.add_param(
                name=name, param_type=param_type, value=value
            )

        renderer_configuration.template_str = template_str
        return renderer_configuration

    @classmethod
    def _atomic_prescriptive_template(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> Tuple[str, dict, MetaNotes, Optional[dict]]:
        """
        Template function that contains the logic that is shared by AtomicPrescriptiveRendererType.SUMMARY and
        LegacyRendererType.PRESCRIPTIVE.
        """
        # deprecated-v0.15.43
        warnings.warn(
            "The method _atomic_prescriptive_template is deprecated as of v0.15.43 and will be removed in v0.18. "
            "Please refer to Expectation method _prescriptive_template for the latest renderer template pattern.",
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
        styling = (
            runtime_configuration.get("styling", {}) if runtime_configuration else {}
        )
        return (
            renderer_configuration.template_str,
            renderer_configuration.params.dict(),
            renderer_configuration.meta_notes,
            styling,
        )

    @classmethod
    @renderer(renderer_type=AtomicPrescriptiveRendererType.SUMMARY)
    @render_evaluation_parameter_string
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
    def _diagnostic_meta_properties_renderer(
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
        """

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
                        assert isinstance(
                            result.expectation_config, ExpectationConfiguration
                        )
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
        if result.exception_info["raised_exception"]:
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
                    "styling": {
                        "parent": {
                            "classes": ["hide-succeeded-validation-target-child"]
                        }
                    },
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

        if result.exception_info["raised_exception"]:
            exception_message_template_str = (
                "\n\n$expectation_type raised an exception:\n$exception_message"
            )

            if result.expectation_config is not None:
                expectation_type = result.expectation_config.expectation_type
            else:
                expectation_type = None

            exception_message = RenderedStringTemplateContent(
                **{  # type: ignore[arg-type]
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": exception_message_template_str,
                        "params": {
                            "expectation_type": expectation_type,
                            "exception_message": result.exception_info[
                                "exception_message"
                            ],
                        },
                        "tag": "strong",
                        "styling": {
                            "classes": ["text-danger"],
                            "params": {
                                "exception_message": {"tag": "code"},
                                "expectation_type": {
                                    "classes": ["badge", "badge-danger", "mb-2"]
                                },
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
                                    "template": result.exception_info[
                                        "exception_traceback"
                                    ],
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
            unexpected_percent = (
                f"{num_to_str(result_dict['unexpected_percent'], precision=4)}%"
            )
            element_count = num_to_str(
                result_dict["element_count"], use_locale=True, precision=20
            )

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
    def _diagnostic_unexpected_table_renderer(  # noqa: PLR0912
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
        unexpected_index_list: Optional[List[dict]] = result_dict.get(
            "unexpected_index_list"
        )
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
                    elif unexpected_value == "":  # noqa: PLC1901
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
                "styling": {
                    "body": {"classes": ["table-bordered", "table-sm", "mt-3"]}
                },
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
    def _get_observed_value_from_evr(
        self, result: Optional[ExpectationValidationResult]
    ) -> str:
        result_dict: Optional[dict] = None
        if result:
            result_dict = result.result
        if result_dict is None:
            return "--"

        observed_value: Any = result_dict.get("observed_value")
        unexpected_percent: Optional[float] = result_dict.get("unexpected_percent")
        if observed_value is not None:
            if isinstance(observed_value, (int, float)) and not isinstance(
                observed_value, bool
            ):
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
        configuration: Optional[ExpectationConfiguration] = None,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        **kwargs: dict,
    ) -> ExpectationValidationResult:
        if not configuration:
            configuration = self.configuration

        if runtime_configuration is None:
            runtime_configuration = {}

        validation_dependencies: ValidationDependencies = (
            self.get_validation_dependencies(
                configuration=configuration,
                execution_engine=execution_engine,
                runtime_configuration=runtime_configuration,
            )
        )
        runtime_configuration["result_format"] = validation_dependencies.result_format

        metric_name: str
        metric_configuration: MetricConfiguration
        provided_metrics: Dict[str, MetricValue] = {
            metric_name: metrics[metric_configuration.id]
            for metric_name, metric_configuration in validation_dependencies.metric_configurations.items()
        }

        expectation_validation_result: Union[
            ExpectationValidationResult, dict
        ] = self._validate(
            configuration=configuration,
            metrics=provided_metrics,
            runtime_configuration=runtime_configuration,
            execution_engine=execution_engine,
        )

        result_format = parse_result_format(
            runtime_configuration.get("result_format", {})
        )
        if result_format.get("result_format") == "BOOLEAN_ONLY":
            if isinstance(expectation_validation_result, ExpectationValidationResult):
                expectation_validation_result.result = {}
            else:
                expectation_validation_result["result"] = {}

        evr: ExpectationValidationResult = self._build_evr(
            raw_response=expectation_validation_result,
            configuration=configuration,
        )
        return evr

    # noinspection PyUnusedLocal
    @staticmethod
    def _build_evr(
        raw_response: Union[ExpectationValidationResult, dict],
        configuration: ExpectationConfiguration,
        **kwargs: dict,
    ) -> ExpectationValidationResult:
        """_build_evr is a lightweight convenience wrapper handling cases where an Expectation implementor
        fails to return an EVR but returns the necessary components in a dictionary."""
        evr: ExpectationValidationResult
        if not isinstance(raw_response, ExpectationValidationResult):
            if isinstance(raw_response, dict):
                evr = ExpectationValidationResult(**raw_response)
                evr.expectation_config = configuration
            else:
                raise GreatExpectationsError("Unable to build EVR")
        else:
            raw_response_dict: dict = raw_response.to_json_dict()
            evr = ExpectationValidationResult(**raw_response_dict)
            evr.expectation_config = configuration
        return evr

    def get_validation_dependencies(
        self,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> ValidationDependencies:
        """Returns the result format and metrics required to validate this Expectation using the provided result format."""
        runtime_configuration = self.get_runtime_kwargs(
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        result_format: dict = runtime_configuration["result_format"]
        result_format = parse_result_format(result_format=result_format)
        return ValidationDependencies(
            metric_configurations={}, result_format=result_format
        )

    def get_domain_kwargs(
        self, configuration: ExpectationConfiguration
    ) -> Dict[str, Optional[str]]:
        domain_kwargs: Dict[str, Optional[str]] = {
            key: configuration.kwargs.get(key, self.default_kwarg_values.get(key))
            for key in self.domain_keys
        }
        missing_kwargs: Union[set, Set[str]] = set(self.domain_keys) - set(
            domain_kwargs.keys()
        )
        if missing_kwargs:
            raise InvalidExpectationKwargsError(
                f"Missing domain kwargs: {list(missing_kwargs)}"
            )
        return domain_kwargs

    @public_api
    def get_success_kwargs(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> Dict[str, Any]:
        """Retrieve the success kwargs.

        Args:
            configuration: The `ExpectationConfiguration` that contains the kwargs. If no configuration arg is provided,
                the success kwargs from the configuration attribute of the Expectation instance will be returned.
        """
        if not configuration:
            configuration = self.configuration

        domain_kwargs: Dict[str, Optional[str]] = self.get_domain_kwargs(
            configuration=configuration
        )
        success_kwargs: Dict[str, Any] = {
            key: configuration.kwargs.get(key, self.default_kwarg_values.get(key))
            for key in self.success_keys
        }
        success_kwargs.update(domain_kwargs)
        return success_kwargs

    def get_runtime_kwargs(
        self,
        configuration: Optional[ExpectationConfiguration] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> dict:
        if not configuration:
            configuration = self.configuration

        configuration = deepcopy(configuration)

        if runtime_configuration:
            configuration.kwargs.update(runtime_configuration)

        success_kwargs = self.get_success_kwargs(configuration=configuration)
        runtime_kwargs = {
            key: configuration.kwargs.get(key, self.default_kwarg_values.get(key))
            for key in self.runtime_keys
        }
        runtime_kwargs.update(success_kwargs)

        runtime_kwargs["result_format"] = parse_result_format(
            runtime_kwargs["result_format"]
        )

        return runtime_kwargs

    def get_result_format(
        self,
        configuration: ExpectationConfiguration,
        runtime_configuration: Optional[dict] = None,
    ) -> Union[Dict[str, Union[str, int, bool, List[str], None]], str]:
        default_result_format: Optional[Any] = self.default_kwarg_values.get(
            "result_format"
        )
        configuration_result_format: Union[
            Dict[str, Union[str, int, bool, List[str], None]], str
        ] = configuration.kwargs.get("result_format", default_result_format)
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
        """Validates the configuration for the Expectation.

        For all expectations, the configuration's `expectation_type` needs to match the type of the expectation being
        configured. This method is meant to be overridden by specific expectations to provide additional validation
        checks as required. Overriding methods should call `super().validate_configuration(configuration)`.

        Raises:
            InvalidExpectationConfigurationError: The configuration does not contain the values required
                by the Expectation.
        """
        if not configuration:
            configuration = self.configuration
        try:
            assert (
                configuration.expectation_type == self.expectation_type
            ), f"expectation configuration type {configuration.expectation_type} does not match expectation type {self.expectation_type}"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    @public_api
    def validate(  # noqa: PLR0913
        self,
        validator: Validator,
        configuration: Optional[ExpectationConfiguration] = None,
        evaluation_parameters: Optional[dict] = None,
        interactive_evaluation: bool = True,
        data_context: Optional[AbstractDataContext] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> ExpectationValidationResult:
        """Validates the expectation against the provided data.

        Args:
            validator: A Validator object that can be used to create Expectations, validate Expectations,
                and get Metrics for Expectations.
            configuration: Defines the parameters and name of a specific expectation.
            evaluation_parameters: Dictionary of dynamic values used during Validation of an Expectation.
            interactive_evaluation: Setting the interactive_evaluation flag on a DataAsset
                make it possible to declare expectations and store expectations without
                immediately evaluating them.
            data_context: An instance of a GX DataContext.
            runtime_configuration: The runtime configuration for the Expectation.
        Returns:
            An ExpectationValidationResult object
        """
        if not configuration:
            configuration = deepcopy(self.configuration)

        # issue warnings if necessary
        self._warn_if_result_format_config_in_runtime_configuration(
            runtime_configuration=runtime_configuration,
        )
        self._warn_if_result_format_config_in_expectation_configuration(
            configuration=configuration
        )

        configuration.process_evaluation_parameters(
            evaluation_parameters, interactive_evaluation, data_context
        )
        expectation_validation_result_list: list[
            ExpectationValidationResult
        ] = validator.graph_validate(
            configurations=[configuration],
            runtime_configuration=runtime_configuration,
        )
        return expectation_validation_result_list[0]

    @property
    def configuration(self) -> ExpectationConfiguration:
        if self._configuration is None:
            raise InvalidExpectationConfigurationError(
                "cannot access configuration: expectation has not yet been configured"
            )
        return self._configuration

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
        """

        if debug_logger is not None:
            _debug = lambda x: debug_logger.debug(  # noqa: E731
                f"(run_diagnostics) {x}"
            )
            _error = lambda x: debug_logger.error(  # noqa: E731
                f"(run_diagnostics) {x}"
            )
        else:
            _debug = lambda x: x  # noqa: E731
            _error = lambda x: x  # noqa: E731

        library_metadata: AugmentedLibraryMetadata = (
            self._get_augmented_library_metadata()
        )
        examples: List[ExpectationTestDataCases] = self._get_examples(
            return_only_gallery_examples=False
        )
        gallery_examples: List[ExpectationTestDataCases] = []
        for example in examples:
            _tests_to_include = [
                test for test in example.tests if test.include_in_gallery
            ]
            example = deepcopy(example)  # noqa: PLW2901
            if _tests_to_include:
                example.tests = _tests_to_include
                gallery_examples.append(example)

        description_diagnostics: ExpectationDescriptionDiagnostics = (
            self._get_description_diagnostics()
        )

        _expectation_config: Optional[
            ExpectationConfiguration
        ] = self._get_expectation_configuration_from_examples(examples)
        if not _expectation_config:
            _error(
                f"Was NOT able to get Expectation configuration for {self.expectation_type}. "
                "Is there at least one sample test where 'success' is True?"
            )
        metric_diagnostics_list: List[
            ExpectationMetricDiagnostics
        ] = self._get_metric_diagnostics_list(
            expectation_config=_expectation_config,
        )

        introspected_execution_engines: ExpectationExecutionEngineDiagnostics = (
            self._get_execution_engine_diagnostics(
                metric_diagnostics_list=metric_diagnostics_list,
                registered_metrics=_registered_metrics,
            )
        )
        engines_implemented = [
            e.replace("ExecutionEngine", "")
            for e, i in introspected_execution_engines.items()
            if i is True
        ]
        _debug(
            f"Implemented engines for {self.expectation_type}: {', '.join(engines_implemented)}"
        )

        _debug("Getting test results")
        test_results: List[ExpectationTestDiagnostics] = self._get_test_results(
            expectation_type=description_diagnostics.snake_name,
            test_data_cases=examples,
            execution_engine_diagnostics=introspected_execution_engines,
            raise_exceptions_for_backends=raise_exceptions_for_backends,
            ignore_suppress=ignore_suppress,
            ignore_only_for=ignore_only_for,
            debug_logger=debug_logger,
            only_consider_these_backends=only_consider_these_backends,
            context=context,
        )

        backend_test_result_counts: List[
            ExpectationBackendTestResultCounts
        ] = ExpectationDiagnostics._get_backends_from_test_results(test_results)

        renderers: List[
            ExpectationRendererDiagnostics
        ] = self._get_renderer_diagnostics(
            expectation_type=description_diagnostics.snake_name,
            test_diagnostics=test_results,
            registered_renderers=_registered_renderers,  # type: ignore[arg-type]
        )

        maturity_checklist: ExpectationDiagnosticMaturityMessages = (
            self._get_maturity_checklist(
                library_metadata=library_metadata,
                description=description_diagnostics,
                examples=examples,
                tests=test_results,
                backend_test_result_counts=backend_test_result_counts,
            )
        )

        coverage_score: float = Expectation._get_coverage_score(
            backend_test_result_counts=backend_test_result_counts,
            execution_engines=introspected_execution_engines,
        )

        _debug(f"coverage_score: {coverage_score} for {self.expectation_type}")

        # Set final maturity level based on status of all checks
        library_metadata.maturity = Expectation._get_final_maturity_level(
            maturity_checklist=maturity_checklist
        )

        # Set the errors found when running tests
        errors = [
            test_result.error_diagnostics
            for test_result in test_results
            if test_result.error_diagnostics
        ]

        # If run for the gallery, don't include a bunch of stuff
        #   - Don't set examples and test_results to empty lists here since these
        #     returned attributes will be needed to re-calculate the maturity
        #     checklist later (after merging results from different runs of the
        #     build_gallery.py script per backend)
        if for_gallery:
            gallery_examples = []
            renderers = []
            errors = []

        return ExpectationDiagnostics(
            library_metadata=library_metadata,
            examples=examples,
            gallery_examples=gallery_examples,
            description=description_diagnostics,
            renderers=renderers,
            metrics=metric_diagnostics_list,
            execution_engines=introspected_execution_engines,
            tests=test_results,
            backend_test_result_counts=backend_test_result_counts,
            maturity_checklist=maturity_checklist,
            errors=errors,
            coverage_score=coverage_score,
        )

    def _warn_if_result_format_config_in_runtime_configuration(
        self, runtime_configuration: Union[dict, None] = None
    ) -> None:
        """
        Issues warning if result_format is in runtime_configuration for Validator
        """
        if runtime_configuration and runtime_configuration.get("result_format"):
            warnings.warn(
                "`result_format` configured at the Validator-level will not be persisted. Please add the configuration to your Checkpoint config or checkpoint_run() method instead.",
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
                "`result_format` configured at the Expectation-level will not be persisted. Please add the configuration to your Checkpoint config or checkpoint_run() method instead.",
                UserWarning,
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
        """

        if diagnostics is None:
            debug_logger = None
            if show_debug_messages:
                debug_logger = logging.getLogger()
                chandler = logging.StreamHandler(stream=sys.stdout)
                chandler.setLevel(logging.DEBUG)
                chandler.setFormatter(
                    logging.Formatter(
                        "%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%dT%H:%M:%S"
                    )
                )
                debug_logger.addHandler(chandler)
                debug_logger.setLevel(logging.DEBUG)

            diagnostics = self.run_diagnostics(
                debug_logger=debug_logger, only_consider_these_backends=backends
            )
        if show_failed_tests:
            for test in diagnostics.tests:
                if test.test_passed is False:
                    print(f"=== {test.test_title} ({test.backend}) ===\n")
                    print(f"{80 * '='}\n")

        checklist: str = diagnostics.generate_checklist()
        print(checklist)

        return checklist

    def _get_examples_from_json(self):
        """Only meant to be called by self._get_examples"""
        results = []
        found = glob.glob(
            os.path.join(  # noqa: PTH118
                _TEST_DEFS_DIR, "**", f"{self.expectation_type}.json"
            ),
            recursive=True,
        )
        if found:
            with open(found[0]) as fp:
                data = json.load(fp)
            results = data["datasets"]
        return results

    def _get_examples(
        self, return_only_gallery_examples: bool = True
    ) -> List[ExpectationTestDataCases]:
        """
        Get a list of examples from the object's `examples` member variable.

        For core expectations, the examples are found in tests/test_definitions/

        :param return_only_gallery_examples: if True, include only test examples where `include_in_gallery` is true
        :return: list of examples or [], if no examples exist
        """
        # Currently, only community contrib expectations have an examples attribute
        all_examples: List[dict] = self.examples or self._get_examples_from_json()

        included_examples = []
        for i, example in enumerate(all_examples, 1):
            included_test_cases = []
            # As of commit 7766bb5caa4e0 on 1/28/22, only_for does not need to be applied to individual tests
            # See:
            #   - https://github.com/great-expectations/great_expectations/blob/7766bb5caa4e0e5b22fa3b3a5e1f2ac18922fdeb/tests/test_definitions/column_map_expectations/expect_column_values_to_be_unique.json#L174
            #   - https://github.com/great-expectations/great_expectations/pull/4073
            top_level_only_for = example.get("only_for")
            top_level_suppress_test_for = example.get("suppress_test_for")
            for test in example["tests"]:
                if (
                    test.get("include_in_gallery") == True  # noqa: E712
                    or return_only_gallery_examples == False  # noqa: E712
                ):
                    copied_test = deepcopy(test)
                    if top_level_only_for:
                        if "only_for" not in copied_test:
                            copied_test["only_for"] = top_level_only_for
                        else:
                            copied_test["only_for"].extend(top_level_only_for)
                    if top_level_suppress_test_for:
                        if "suppress_test_for" not in copied_test:
                            copied_test[
                                "suppress_test_for"
                            ] = top_level_suppress_test_for
                        else:
                            copied_test["suppress_test_for"].extend(
                                top_level_suppress_test_for
                            )
                    included_test_cases.append(
                        ExpectationLegacyTestCaseAdapter(**copied_test)
                    )

            # If at least one ExpectationTestCase from the ExpectationTestDataCases was selected,
            # then keep a copy of the ExpectationTestDataCases including data and the selected ExpectationTestCases.
            if len(included_test_cases) > 0:
                copied_example = deepcopy(example)
                copied_example["tests"] = included_test_cases
                copied_example.pop("_notes", None)
                copied_example.pop("only_for", None)
                copied_example.pop("suppress_test_for", None)
                if "test_backends" in copied_example:
                    copied_example["test_backends"] = [
                        TestBackend(**tb) for tb in copied_example["test_backends"]
                    ]

                if "dataset_name" not in copied_example:
                    dataset_name = generate_dataset_name_from_expectation_name(
                        dataset=copied_example,
                        expectation_type=self.expectation_type,
                        index=i,
                    )
                    copied_example["dataset_name"] = dataset_name

                included_examples.append(ExpectationTestDataCases(**copied_example))

        return included_examples

    def _get_docstring_and_short_description(self) -> Tuple[str, str]:
        """Conveninence method to get the Exepctation's docstring and first line"""

        if self.__doc__ is not None:
            docstring = self.__doc__
            short_description = next(line for line in self.__doc__.split("\n") if line)
        else:
            docstring = ""
            short_description = ""

        return docstring, short_description

    def _get_description_diagnostics(self) -> ExpectationDescriptionDiagnostics:
        """Introspect the Expectation and create its ExpectationDescriptionDiagnostics object"""

        camel_name = self.__class__.__name__
        snake_name = camel_to_snake(self.__class__.__name__)
        docstring, short_description = self._get_docstring_and_short_description()

        return ExpectationDescriptionDiagnostics(
            **{
                "camel_name": camel_name,
                "snake_name": snake_name,
                "short_description": short_description,
                "docstring": docstring,
            }
        )

    def _get_expectation_configuration_from_examples(
        self,
        examples: List[ExpectationTestDataCases],
    ) -> Optional[ExpectationConfiguration]:
        """Return an ExpectationConfiguration instance using test input expected to succeed"""
        if examples:
            for example in examples:
                tests = example.tests
                if tests:
                    for test in tests:
                        if test.output.get("success"):
                            return ExpectationConfiguration(
                                expectation_type=self.expectation_type,
                                kwargs=test.input,
                            )

            # There is no sample test where `success` is True, or there are no tests
            for example in examples:
                tests = example.tests
                if tests:
                    for test in tests:
                        if test.input:
                            return ExpectationConfiguration(
                                expectation_type=self.expectation_type,
                                kwargs=test.input,
                            )
        return None

    @staticmethod
    def is_expectation_self_initializing(name: str) -> bool:
        """
        Given the name of an Expectation, returns a boolean that represents whether an Expectation can be auto-intialized.

        Args:
            name (str): name of Expectation

        Returns:
            boolean that represents whether an Expectation can be auto-initialized. Information also outputted to logger.
        """

        expectation_impl: MetaExpectation = get_expectation_impl(name)
        if not expectation_impl:
            raise ExpectationNotFoundError(
                f"Expectation {name} was not found in the list of registered Expectations. "
                f"Please check your configuration and try again"
            )
        if "auto" in expectation_impl.default_kwarg_values:
            print(
                f"The Expectation {name} is able to be self-initialized. Please run by using the auto=True parameter."
            )
            return True
        else:
            print(f"The Expectation {name} is not able to be self-initialized.")
            return False

    @staticmethod
    def _add_array_params(
        array_param_name: str,
        param_prefix: str,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        if not param_prefix:
            raise RendererConfigurationError(
                "Array param_prefix must be a non-empty string."
            )

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
                        name=f"{param_prefix}{str(idx)}",
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
            raise RendererConfigurationError(
                "Array param_prefix must be a non-empty string."
            )

        @param_method(param_name=array_param_name)
        def _get_string(renderer_configuration: RendererConfiguration) -> str:
            array: Sequence[Optional[Any]] = getattr(
                renderer_configuration.params, array_param_name
            ).value
            if array:
                array_string = " ".join(
                    [f"${param_prefix}{str(idx)}" for idx in range(len(array))]
                )
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
            precision=15,
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

    @staticmethod
    def _choose_example(
        examples: List[ExpectationTestDataCases],
    ) -> Tuple[TestData, ExpectationTestCase]:
        """Choose examples to use for run_diagnostics.

        This implementation of this method is very naive---it just takes the first one.
        """
        example = examples[0]

        example_test_data = example["data"]
        example_test_case = example["tests"][0]

        return example_test_data, example_test_case

    @staticmethod
    def _get_registered_renderers(
        expectation_type: str,
        registered_renderers: dict,
    ) -> List[str]:
        """Get a list of supported renderers for this Expectation, in sorted order."""
        supported_renderers = list(registered_renderers[expectation_type].keys())
        supported_renderers.sort()
        return supported_renderers

    @classmethod
    def _get_test_results(  # noqa: PLR0913
        cls,
        expectation_type: str,
        test_data_cases: List[ExpectationTestDataCases],
        execution_engine_diagnostics: ExpectationExecutionEngineDiagnostics,
        raise_exceptions_for_backends: bool = False,
        ignore_suppress: bool = False,
        ignore_only_for: bool = False,
        debug_logger: Optional[logging.Logger] = None,
        only_consider_these_backends: Optional[List[str]] = None,
        context: Optional[AbstractDataContext] = None,
    ) -> List[ExpectationTestDiagnostics]:
        """Generate test results. This is an internal method for run_diagnostics."""

        if debug_logger is not None:
            _debug = lambda x: debug_logger.debug(  # noqa: E731
                f"(_get_test_results) {x}"
            )
            _error = lambda x: debug_logger.error(  # noqa: E731
                f"(_get_test_results) {x}"
            )
        else:
            _debug = lambda x: x  # noqa: E731
            _error = lambda x: x  # noqa: E731
        _debug("Starting")

        test_results = []

        exp_tests = generate_expectation_tests(
            expectation_type=expectation_type,
            test_data_cases=test_data_cases,
            execution_engine_diagnostics=execution_engine_diagnostics,
            raise_exceptions_for_backends=raise_exceptions_for_backends,
            ignore_suppress=ignore_suppress,
            ignore_only_for=ignore_only_for,
            debug_logger=debug_logger,
            only_consider_these_backends=only_consider_these_backends,
            context=context,
        )

        error_diagnostics: Optional[ExpectationErrorDiagnostics]
        backend_test_times = defaultdict(list)
        for exp_test in exp_tests:
            if exp_test["test"] is None:
                _debug(
                    f"validator_with_data failure for {exp_test['backend']}--{expectation_type}"
                )

                error_diagnostics = ExpectationErrorDiagnostics(
                    error_msg=exp_test["error"],
                    stack_trace="",
                    test_title="all",
                    test_backend=exp_test["backend"],
                )

                test_results.append(
                    ExpectationTestDiagnostics(
                        test_title="all",
                        backend=exp_test["backend"],
                        test_passed=False,
                        include_in_gallery=False,
                        validation_result=None,
                        error_diagnostics=error_diagnostics,
                    )
                )
                continue

            exp_combined_test_name = f"{exp_test['backend']}--{exp_test['test']['title']}--{expectation_type}"
            _debug(f"Starting {exp_combined_test_name}")
            _start = time.time()
            validation_result, error_message, stack_trace = evaluate_json_test_v3_api(
                validator=exp_test["validator_with_data"],
                expectation_type=exp_test["expectation_type"],
                test=exp_test["test"],
                raise_exception=False,
                debug_logger=debug_logger,
            )
            _end = time.time()
            _duration = _end - _start
            backend_test_times[exp_test["backend"]].append(_duration)
            _debug(
                f"Took {_duration} seconds to evaluate_json_test_v3_api for {exp_combined_test_name}"
            )
            if error_message is None:
                _debug(f"PASSED {exp_combined_test_name}")
                test_passed = True
                error_diagnostics = None
            else:
                _error(f"{repr(error_message)} for {exp_combined_test_name}")
                print(f"{stack_trace[0]}")
                error_diagnostics = ExpectationErrorDiagnostics(
                    error_msg=error_message,
                    stack_trace=stack_trace,
                    test_title=exp_test["test"]["title"],
                    test_backend=exp_test["backend"],
                )
                test_passed = False

            if validation_result:
                # The ExpectationTestDiagnostics instance will error when calling it's to_dict()
                # method (AttributeError: 'ExpectationConfiguration' object has no attribute 'raw_kwargs')
                validation_result.expectation_config.raw_kwargs = (
                    validation_result.expectation_config._raw_kwargs
                )

            test_results.append(
                ExpectationTestDiagnostics(
                    test_title=exp_test["test"]["title"],
                    backend=exp_test["backend"],
                    test_passed=test_passed,
                    include_in_gallery=exp_test["test"]["include_in_gallery"],
                    validation_result=validation_result,
                    error_diagnostics=error_diagnostics,
                )
            )

        for backend_name, test_times in sorted(backend_test_times.items()):
            _debug(
                f"Took {sum(test_times)} seconds to run {len(test_times)} tests {backend_name}--{expectation_type}"
            )

        return test_results

    def _get_rendered_result_as_string(  # noqa: C901, PLR0912
        self, rendered_result
    ) -> str:
        """Convenience method to get rendered results as strings."""

        result: str = ""

        if type(rendered_result) == str:
            result = rendered_result

        elif type(rendered_result) == list:
            sub_result_list = []
            for sub_result in rendered_result:
                res = self._get_rendered_result_as_string(sub_result)
                if res is not None:
                    sub_result_list.append(res)

            result = "\n".join(sub_result_list)

        elif isinstance(rendered_result, RenderedStringTemplateContent):
            result = rendered_result.__str__()

        elif isinstance(rendered_result, CollapseContent):
            result = rendered_result.__str__()

        elif isinstance(rendered_result, RenderedAtomicContent):
            result = f"(RenderedAtomicContent) {repr(rendered_result.to_json_dict())}"

        elif isinstance(rendered_result, RenderedContentBlockContainer):
            result = "(RenderedContentBlockContainer) " + repr(
                rendered_result.to_json_dict()
            )

        elif isinstance(rendered_result, RenderedTableContent):
            result = f"(RenderedTableContent) {repr(rendered_result.to_json_dict())}"

        elif isinstance(rendered_result, RenderedGraphContent):
            result = f"(RenderedGraphContent) {repr(rendered_result.to_json_dict())}"

        elif isinstance(rendered_result, ValueListContent):
            result = f"(ValueListContent) {repr(rendered_result.to_json_dict())}"

        elif isinstance(rendered_result, dict):
            result = f"(dict) {repr(rendered_result)}"

        elif isinstance(rendered_result, int):
            result = repr(rendered_result)

        elif rendered_result is None:
            result = ""

        else:
            raise TypeError(
                f"Expectation._get_rendered_result_as_string can't render type {type(rendered_result)} as a string."
            )

        if "inf" in result:
            result = ""
        return result

    def _get_renderer_diagnostics(
        self,
        expectation_type: str,
        test_diagnostics: List[ExpectationTestDiagnostics],
        registered_renderers: List[str],
        standard_renderers: Optional[
            List[Union[str, LegacyRendererType, LegacyDiagnosticRendererType]]
        ] = None,
    ) -> List[ExpectationRendererDiagnostics]:
        """Generate Renderer diagnostics for this Expectation, based primarily on a list of ExpectationTestDiagnostics."""

        if not standard_renderers:
            standard_renderers = [
                LegacyRendererType.ANSWER,
                LegacyDiagnosticRendererType.UNEXPECTED_STATEMENT,
                LegacyDiagnosticRendererType.OBSERVED_VALUE,
                LegacyDiagnosticRendererType.STATUS_ICON,
                LegacyDiagnosticRendererType.UNEXPECTED_TABLE,
                LegacyRendererType.PRESCRIPTIVE,
                LegacyRendererType.QUESTION,
            ]

        supported_renderers = self._get_registered_renderers(
            expectation_type=expectation_type,
            registered_renderers=registered_renderers,  # type: ignore[arg-type]
        )

        renderer_diagnostic_list = []
        for renderer_name in set(standard_renderers).union(set(supported_renderers)):
            samples = []
            if renderer_name in supported_renderers:
                _, renderer = registered_renderers[expectation_type][renderer_name]  # type: ignore[call-overload]

                for test_diagnostic in test_diagnostics:
                    test_title = test_diagnostic["test_title"]

                    try:
                        rendered_result = renderer(
                            configuration=test_diagnostic["validation_result"][
                                "expectation_config"
                            ],
                            result=test_diagnostic["validation_result"],
                        )
                        rendered_result_str = self._get_rendered_result_as_string(
                            rendered_result
                        )

                    except Exception as e:
                        new_sample = RendererTestDiagnostics(
                            test_title=test_title,
                            renderered_str=None,
                            rendered_successfully=False,
                            error_message=str(e),
                            stack_trace=traceback.format_exc(),
                        )

                    else:
                        new_sample = RendererTestDiagnostics(
                            test_title=test_title,
                            renderered_str=rendered_result_str,
                            rendered_successfully=True,
                        )

                    finally:
                        samples.append(new_sample)

            new_renderer_diagnostics = ExpectationRendererDiagnostics(
                name=renderer_name,
                is_supported=renderer_name in supported_renderers,
                is_standard=renderer_name in standard_renderers,
                samples=samples,
            )
            renderer_diagnostic_list.append(new_renderer_diagnostics)

        # Sort to enforce consistency for testing
        renderer_diagnostic_list.sort(key=lambda x: x.name)

        return renderer_diagnostic_list

    @staticmethod
    def _get_execution_engine_diagnostics(
        metric_diagnostics_list: List[ExpectationMetricDiagnostics],
        registered_metrics: dict,
        execution_engine_names: Optional[List[str]] = None,
    ) -> ExpectationExecutionEngineDiagnostics:
        """Check to see which execution_engines are fully supported for this Expectation.

        In order for a given execution engine to count, *every* metric must have support on that execution engines.
        """
        if not execution_engine_names:
            execution_engine_names = [
                "PandasExecutionEngine",
                "SqlAlchemyExecutionEngine",
                "SparkDFExecutionEngine",
            ]

        execution_engines = {}
        for provider in execution_engine_names:
            all_true = True
            if not metric_diagnostics_list:
                all_true = False
            for metric_diagnostics in metric_diagnostics_list:
                try:
                    has_provider = (
                        provider
                        in registered_metrics[metric_diagnostics.name]["providers"]
                    )
                    if not has_provider:
                        all_true = False
                        break
                except KeyError:
                    # https://github.com/great-expectations/great_expectations/blob/abd8f68a162eaf9c33839d2c412d8ba84f5d725b/great_expectations/expectations/core/expect_table_row_count_to_equal_other_table.py#L174-L181
                    # expect_table_row_count_to_equal_other_table does tricky things and replaces
                    # registered metric "table.row_count" with "table.row_count.self" and "table.row_count.other"
                    if "table.row_count" in metric_diagnostics.name:
                        continue

            execution_engines[provider] = all_true

        return ExpectationExecutionEngineDiagnostics(**execution_engines)

    def _get_metric_diagnostics_list(
        self,
        expectation_config: Optional[ExpectationConfiguration],
    ) -> List[ExpectationMetricDiagnostics]:
        """Check to see which Metrics are upstream validation_dependencies for this Expectation."""

        # NOTE: Abe 20210102: Strictly speaking, identifying upstream metrics shouldn't need to rely on an expectation config.
        # There's probably some part of get_validation_dependencies that can be factored out to remove the dependency.

        if not expectation_config:
            return []

        validation_dependencies: ValidationDependencies = (
            self.get_validation_dependencies(configuration=expectation_config)
        )

        metric_name: str
        metric_diagnostics_list: List[ExpectationMetricDiagnostics] = [
            ExpectationMetricDiagnostics(
                name=metric_name,
                has_question_renderer=False,
            )
            for metric_name in validation_dependencies.get_metric_names()
        ]

        return metric_diagnostics_list

    def _get_augmented_library_metadata(self):
        """Introspect the Expectation's library_metadata object (if it exists), and augment it with additional information."""

        augmented_library_metadata = {
            "maturity": Maturity.CONCEPT_ONLY,
            "tags": [],
            "contributors": [],
            "requirements": [],
            "library_metadata_passed_checks": False,
            "has_full_test_suite": False,
            "manually_reviewed_code": False,
        }
        required_keys = {"contributors", "tags"}
        allowed_keys = {
            "contributors",
            "has_full_test_suite",
            "manually_reviewed_code",
            "maturity",
            "requirements",
            "tags",
        }
        problems = []

        if hasattr(self, "library_metadata"):
            augmented_library_metadata.update(self.library_metadata)
            keys = set(self.library_metadata.keys())
            missing_required_keys = required_keys - keys
            forbidden_keys = keys - allowed_keys

            if missing_required_keys:
                problems.append(
                    f"Missing required key(s): {sorted(missing_required_keys)}"
                )
            if forbidden_keys:
                problems.append(f"Extra key(s) found: {sorted(forbidden_keys)}")
            if type(augmented_library_metadata["requirements"]) != list:
                problems.append("library_metadata['requirements'] is not a list ")
            if not problems:
                augmented_library_metadata["library_metadata_passed_checks"] = True
        else:
            problems.append("No library_metadata attribute found")

        augmented_library_metadata["problems"] = problems
        return AugmentedLibraryMetadata.from_legacy_dict(augmented_library_metadata)

    def _get_maturity_checklist(  # noqa: PLR0913
        self,
        library_metadata: Union[
            AugmentedLibraryMetadata, ExpectationDescriptionDiagnostics
        ],
        description: ExpectationDescriptionDiagnostics,
        examples: List[ExpectationTestDataCases],
        tests: List[ExpectationTestDiagnostics],
        backend_test_result_counts: List[ExpectationBackendTestResultCounts],
    ) -> ExpectationDiagnosticMaturityMessages:
        """Generate maturity checklist messages"""
        experimental_checks = []
        beta_checks = []
        production_checks = []

        experimental_checks.append(
            ExpectationDiagnostics._check_library_metadata(library_metadata)
        )
        experimental_checks.append(ExpectationDiagnostics._check_docstring(description))
        experimental_checks.append(
            ExpectationDiagnostics._check_example_cases(examples, tests)
        )
        experimental_checks.append(
            ExpectationDiagnostics._check_core_logic_for_at_least_one_execution_engine(
                backend_test_result_counts
            )
        )
        beta_checks.append(
            ExpectationDiagnostics._check_input_validation(self, examples)
        )
        beta_checks.append(ExpectationDiagnostics._check_renderer_methods(self))
        beta_checks.append(
            ExpectationDiagnostics._check_core_logic_for_all_applicable_execution_engines(
                backend_test_result_counts
            )
        )

        production_checks.append(
            ExpectationDiagnostics._check_full_test_suite(library_metadata)
        )
        production_checks.append(
            ExpectationDiagnostics._check_manual_code_review(library_metadata)
        )

        return ExpectationDiagnosticMaturityMessages(
            experimental=experimental_checks,
            beta=beta_checks,
            production=production_checks,
        )

    @staticmethod
    def _get_coverage_score(
        backend_test_result_counts: List[ExpectationBackendTestResultCounts],
        execution_engines: ExpectationExecutionEngineDiagnostics,
    ) -> float:
        """Generate coverage score"""
        _total_passed = 0
        _total_failed = 0
        _num_backends = 0
        _num_engines = sum([x for x in execution_engines.values() if x])
        for result in backend_test_result_counts:
            _num_backends += 1
            _total_passed += result.num_passed
            _total_failed += result.num_failed

        coverage_score = (
            _num_backends + _num_engines + _total_passed - (1.5 * _total_failed)
        )

        return coverage_score

    @staticmethod
    def _get_final_maturity_level(
        maturity_checklist: ExpectationDiagnosticMaturityMessages,
    ) -> Maturity:
        """Get final maturity level based on status of all checks"""
        maturity = ""
        all_experimental = all(
            check.passed for check in maturity_checklist.experimental
        )
        all_beta = all(check.passed for check in maturity_checklist.beta)
        all_production = all(check.passed for check in maturity_checklist.production)
        if all_production and all_beta and all_experimental:
            maturity = Maturity.PRODUCTION
        elif all_beta and all_experimental:
            maturity = Maturity.BETA
        else:
            maturity = Maturity.EXPERIMENTAL

        return maturity


@public_api
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
    """

    domain_keys: Tuple[str, ...] = (
        "batch_id",
        "table",
        "row_condition",
        "condition_parser",
    )
    metric_dependencies: Tuple[str, ...] = ()
    domain_type = MetricDomainTypes.TABLE
    args_keys: Tuple[str, ...] = ()

    def get_validation_dependencies(
        self,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> ValidationDependencies:
        validation_dependencies: ValidationDependencies = (
            super().get_validation_dependencies(
                configuration=configuration,
                execution_engine=execution_engine,
                runtime_configuration=runtime_configuration,
            )
        )

        metric_name: str
        for metric_name in self.metric_dependencies:
            metric_kwargs = get_metric_kwargs(
                metric_name=metric_name,
                configuration=configuration,
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

    @staticmethod
    def validate_metric_value_between_configuration(
        configuration: Optional[ExpectationConfiguration] = None,
    ) -> bool:
        if not configuration:
            return True

        # Validating that Minimum and Maximum values are of the proper format and type
        min_val = configuration.kwargs.get("min_value")
        max_val = configuration.kwargs.get("max_value")

        try:
            assert (
                min_val is None
                or is_parseable_date(min_val)
                or isinstance(min_val, (float, int, dict, datetime.datetime))
            ), "Provided min threshold must be a datetime (for datetime columns) or number"
            if isinstance(min_val, dict):
                assert (
                    "$PARAMETER" in min_val
                ), 'Evaluation Parameter dict for min_value kwarg must have "$PARAMETER" key'

            assert (
                max_val is None
                or is_parseable_date(max_val)
                or isinstance(max_val, (float, int, dict, datetime.datetime))
            ), "Provided max threshold must be a datetime (for datetime columns) or number"
            if isinstance(max_val, dict):
                assert (
                    "$PARAMETER" in max_val
                ), 'Evaluation Parameter dict for max_value kwarg must have "$PARAMETER" key'

        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

        return True

    def _validate_metric_value_between(  # noqa: C901, PLR0912, PLR0913
        self,
        metric_name,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ) -> Dict[str, Union[bool, Dict[str, Any]]]:
        metric_value: Optional[Any] = metrics.get(metric_name)

        if metric_value is None:
            return {"success": False, "result": {"observed_value": metric_value}}

        # Obtaining components needed for validation
        min_value: Optional[Any] = self.get_success_kwargs(
            configuration=configuration
        ).get("min_value")
        strict_min: Optional[bool] = self.get_success_kwargs(
            configuration=configuration
        ).get("strict_min")
        max_value: Optional[Any] = self.get_success_kwargs(
            configuration=configuration
        ).get("max_value")
        strict_max: Optional[bool] = self.get_success_kwargs(
            configuration=configuration
        ).get("strict_max")

        parse_strings_as_datetimes: Optional[bool] = self.get_success_kwargs(
            configuration=configuration
        ).get("parse_strings_as_datetimes")

        if parse_strings_as_datetimes:
            warn_deprecated_parse_strings_as_datetimes()

            if min_value is not None:
                try:
                    min_value = parse(min_value)
                except TypeError:
                    pass

            if max_value is not None:
                try:
                    max_value = parse(max_value)
                except TypeError:
                    pass

        if not isinstance(metric_value, datetime.datetime) and pd.isnull(metric_value):
            return {"success": False, "result": {"observed_value": None}}

        if isinstance(metric_value, datetime.datetime):
            if isinstance(min_value, str):
                try:
                    min_value = parse(min_value)
                except TypeError:
                    raise ValueError(
                        f"""Could not parse "min_value" of {min_value} (of type "{str(type(min_value))}) into datetime \
representation."""
                    )

            if isinstance(max_value, str):
                try:
                    max_value = parse(max_value)
                except TypeError:
                    raise ValueError(
                        f"""Could not parse "max_value" of {max_value} (of type "{str(type(max_value))}) into datetime \
representation."""
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


@public_api
class TableExpectation(BatchExpectation, ABC):
    """Base class for TableExpectations.

    WARNING: TableExpectation will be deprecated in a future release. Please use BatchExpectation instead.

    TableExpectations answer a semantic question about the table itself.

    For example, `expect_table_column_count_to_equal` and `expect_table_row_count_to_equal` answer
    how many columns and rows are in your table.

    TableExpectations must implement a `_validate(...)` method containing logic
    for determining whether the Expectation is successfully validated.

    TableExpectations may optionally provide implementations of `validate_configuration`,
    which should raise an error if the configuration will not be usable for the Expectation.

    Raises:
        InvalidExpectationConfigurationError: The configuration does not contain the values required by the Expectation.

    Args:
        domain_keys (tuple): A tuple of the keys used to determine the domain of the
            expectation.
    """


@public_api
class QueryExpectation(BatchExpectation, ABC):
    """Base class for QueryExpectations.

    QueryExpectations facilitate the execution of SQL or Spark-SQL queries as the core logic for an Expectation.

    QueryExpectations must implement a `_validate(...)` method containing logic for determining whether data returned by the executed query is successfully validated.

    Query Expectations may optionally provide implementations of:

    1. `validate_configuration`, which should raise an error if the configuration will not be usable for the Expectation.

    2. Data Docs rendering methods decorated with the @renderer decorator.

    QueryExpectations may optionally define a `query` attribute, and specify that query as a default in `default_kwarg_values`.

    Doing so precludes the need to pass a query into the Expectation. This default will be overridden if a query is passed in.

    Args:
        domain_keys (tuple): A tuple of the keys used to determine the domain of the
            expectation.
        success_keys (tuple): A tuple of the keys used to determine the success of
            the expectation.
        runtime_keys (optional[tuple]): Optional. A tuple of the keys that can be used to control output but will
            not affect the actual success value of the expectation (such as result_format).
        default_kwarg_values (optional[dict]): Optional. A dictionary that will be used to fill unspecified
            kwargs from the Expectation Configuration.
        query (optional[str]): Optional. A SQL or Spark-SQL query to be executed. If not provided, a query must be passed
            into the QueryExpectation.

    --Documentation--
        - https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_query_expectations
    """

    default_kwarg_values: Dict = {
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
        "row_condition": None,
        "condition_parser": None,
    }

    domain_keys: Tuple = (
        "batch_id",
        "row_condition",
        "condition_parser",
    )

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

        query: Optional[Any] = configuration.kwargs.get(
            "query"
        ) or self.default_kwarg_values.get("query")
        row_condition: Optional[Any] = configuration.kwargs.get(
            "row_condition"
        ) or self.default_kwarg_values.get("row_condition")

        try:
            assert (
                "query" in configuration.kwargs or query
            ), "'query' parameter is required for Query Expectations."
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))
        try:
            if not isinstance(query, str):
                raise TypeError(
                    f"'query' must be a string, but your query is type: {type(query)}"
                )
            parsed_query: Set[str] = {
                x
                for x in re.split(", |\\(|\n|\\)| |/", query)
                if x.upper() and x.upper() not in valid_sql_tokens_and_types
            }
            assert "{active_batch}" in parsed_query, (
                "Your query appears to not be parameterized for a data asset. "
                "By not parameterizing your query with `{active_batch}`, "
                "you may not be validating against your intended data asset, or the expectation may fail."
            )
            assert all(re.match("{.*?}", x) for x in parsed_query), (
                "Your query appears to have hard-coded references to your data. "
                "By not parameterizing your query with `{active_batch}`, {col}, etc., "
                "you may not be validating against your intended data asset, or the expectation may fail."
            )
        except (TypeError, AssertionError) as e:
            warnings.warn(str(e), UserWarning)
        try:
            assert row_condition is None, (
                "`row_condition` is an experimental feature. "
                "Combining this functionality with QueryExpectations may result in unexpected behavior."
            )
        except AssertionError as e:
            warnings.warn(str(e), UserWarning)


@public_api
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
     default_kwarg_values (optional[dict]): Optional. A dictionary that will be used to fill unspecified
         kwargs from the Expectation Configuration.

         - A  "column" key is required for column expectations.

    Raises:
        InvalidExpectationConfigurationError: If no `column` is specified
    """

    domain_keys = ("batch_id", "table", "column", "row_condition", "condition_parser")
    domain_type = MetricDomainTypes.COLUMN

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        super().validate_configuration(configuration=configuration)
        if not configuration:
            configuration = self.configuration
        # Ensuring basic configuration parameters are properly set
        try:
            assert (
                "column" in configuration.kwargs
            ), "'column' parameter is required for column expectations"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))


@public_api
class ColumnExpectation(ColumnAggregateExpectation, ABC):
    """Base class for column aggregate Expectations.

    These types of Expectation produce an aggregate metric for a column, such as the mean, standard deviation,
    number of unique values, column type, etc.

    WARNING: This class will be deprecated in favor of ColumnAggregateExpectation, and removed in a future release.
    If you're using this class, please update your code to use ColumnAggregateExpectation instead.
    There is no change in functionality between the two classes; just a name change for clarity.

    --Documentation--
        - https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_aggregate_expectations/

    Args:
     domain_keys (tuple): A tuple of the keys used to determine the domain of the
         expectation.
     success_keys (tuple): A tuple of the keys used to determine the success of
         the expectation.
     default_kwarg_values (optional[dict]): Optional. A dictionary that will be used to fill unspecified
         kwargs from the Expectation Configuration.

         - A  "column" key is required for column expectations.

    Raises:
        InvalidExpectationConfigurationError: If no `column` is specified
    """


@public_api
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
        default_kwarg_values (optional[dict]): Optional. A dictionary that will be used to fill unspecified
            kwargs from the Expectation Configuration.
    """

    map_metric: Optional[str] = None
    domain_keys = ("batch_id", "table", "column", "row_condition", "condition_parser")
    domain_type = MetricDomainTypes.COLUMN
    success_keys: Tuple[str, ...] = ("mostly",)
    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "mostly": 1,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": True,
    }

    @classmethod
    def is_abstract(cls) -> bool:
        return not cls.map_metric or super().is_abstract()

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        super().validate_configuration(configuration=configuration)
        if not configuration:
            configuration = self.configuration
        try:
            assert (
                "column" in configuration.kwargs
            ), "'column' parameter is required for column map expectations"
            _validate_mostly_config(configuration)
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    def get_validation_dependencies(
        self,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs: dict,
    ) -> ValidationDependencies:
        validation_dependencies: ValidationDependencies = (
            super().get_validation_dependencies(
                configuration=configuration,
                execution_engine=execution_engine,
                runtime_configuration=runtime_configuration,
            )
        )
        assert isinstance(
            self.map_metric, str
        ), "ColumnMapExpectation must override get_validation_dependencies or declare exactly one map_metric"
        assert (
            self.metric_dependencies == tuple()
        ), "ColumnMapExpectation must be configured using map_metric, and cannot have metric_dependencies declared."

        metric_kwargs: dict

        metric_kwargs = get_metric_kwargs(
            metric_name=f"column_values.nonnull.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
            configuration=configuration,
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

        result_format_str: Optional[str] = validation_dependencies.result_format.get(
            "result_format"
        )
        include_unexpected_rows: Optional[
            bool
        ] = validation_dependencies.result_format.get("include_unexpected_rows")

        if result_format_str == "BOOLEAN_ONLY":
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

        if result_format_str in ["BASIC"]:
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

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        result_format: str | dict[str, Any] = self.get_result_format(
            configuration=configuration, runtime_configuration=runtime_configuration
        )

        include_unexpected_rows: bool
        unexpected_index_column_names: int | str | list[str] | None
        if isinstance(result_format, dict):
            include_unexpected_rows = result_format.get(
                "include_unexpected_rows", False
            )
            unexpected_index_column_names = result_format.get(
                "unexpected_index_column_names", None
            )
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
                self.get_success_kwargs().get(
                    "mostly", self.default_kwarg_values.get("mostly")
                ),
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


@public_api
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
        default_kwarg_values (optional[dict]): Optional. A dictionary that will be used to fill unspecified
            kwargs from the Expectation Configuration.
    """

    map_metric = None
    domain_keys = (
        "batch_id",
        "table",
        "column_A",
        "column_B",
        "row_condition",
        "condition_parser",
    )
    domain_type = MetricDomainTypes.COLUMN_PAIR
    success_keys = ("mostly",)
    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "mostly": 1,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": True,
    }

    @classmethod
    def is_abstract(cls) -> bool:
        return cls.map_metric is None or super().is_abstract()

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        super().validate_configuration(configuration=configuration)
        if not configuration:
            configuration = self.configuration
        try:
            assert (
                "column_A" in configuration.kwargs
            ), "'column_A' parameter is required for column pair map expectations"
            assert (
                "column_B" in configuration.kwargs
            ), "'column_B' parameter is required for column pair map expectations"
            _validate_mostly_config(configuration)
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    def get_validation_dependencies(
        self,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> ValidationDependencies:
        validation_dependencies: ValidationDependencies = (
            super().get_validation_dependencies(
                configuration=configuration,
                execution_engine=execution_engine,
                runtime_configuration=runtime_configuration,
            )
        )
        assert isinstance(
            self.map_metric, str
        ), "ColumnPairMapExpectation must override get_validation_dependencies or declare exactly one map_metric"
        assert (
            self.metric_dependencies == tuple()
        ), "ColumnPairMapExpectation must be configured using map_metric, and cannot have metric_dependencies declared."
        metric_kwargs: dict

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
        include_unexpected_rows: Optional[
            bool
        ] = validation_dependencies.result_format.get("include_unexpected_rows")

        if result_format_str == "BOOLEAN_ONLY":
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

        if result_format_str in ["BASIC"]:
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

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        result_format: Union[
            Dict[str, Union[int, str, bool, List[str], None]], str
        ] = self.get_result_format(
            configuration=configuration, runtime_configuration=runtime_configuration
        )

        unexpected_index_column_names = None
        if isinstance(result_format, dict):
            unexpected_index_column_names = result_format.get(
                "unexpected_index_column_names", None
            )
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
                self.get_success_kwargs().get(
                    "mostly", self.default_kwarg_values.get("mostly")
                ),
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


@public_api
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
        default_kwarg_values (optional[dict]): Optional. A dictionary that will be used to fill unspecified
            kwargs from the Expectation Configuration.
    """

    map_metric = None
    domain_keys = (
        "batch_id",
        "table",
        "column_list",
        "row_condition",
        "condition_parser",
        "ignore_row_if",
    )
    domain_type = MetricDomainTypes.MULTICOLUMN
    success_keys = ("mostly",)
    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "mostly": 1,
        "ignore_row_if": "all_values_are_missing",
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": True,
    }

    @classmethod
    def is_abstract(cls) -> bool:
        return cls.map_metric is None or super().is_abstract()

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        super().validate_configuration(configuration=configuration)
        if not configuration:
            configuration = self.configuration
        try:
            assert (
                "column_list" in configuration.kwargs
            ), "'column_list' parameter is required for multicolumn map expectations"
            _validate_mostly_config(configuration)
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    def get_validation_dependencies(
        self,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> ValidationDependencies:
        validation_dependencies: ValidationDependencies = (
            super().get_validation_dependencies(
                configuration=configuration,
                execution_engine=execution_engine,
                runtime_configuration=runtime_configuration,
            )
        )
        assert isinstance(
            self.map_metric, str
        ), "MulticolumnMapExpectation must override get_validation_dependencies or declare exactly one map_metric"
        assert (
            self.metric_dependencies == tuple()
        ), "MulticolumnMapExpectation must be configured using map_metric, and cannot have metric_dependencies declared."
        # convenient name for updates

        metric_kwargs: dict

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
        include_unexpected_rows: Optional[
            bool
        ] = validation_dependencies.result_format.get("include_unexpected_rows")

        if result_format_str == "BOOLEAN_ONLY":
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

        if result_format_str in ["BASIC"]:
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

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        result_format = self.get_result_format(
            configuration=configuration, runtime_configuration=runtime_configuration
        )
        unexpected_index_column_names = None
        if isinstance(result_format, dict):
            unexpected_index_column_names = result_format.get(
                "unexpected_index_column_names", None
            )

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
                self.get_success_kwargs().get(
                    "mostly", self.default_kwarg_values.get("mostly")
                ),
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
    """
    if element_count is None:
        element_count = 0

    # NB: unexpected_count parameter is explicit some implementing classes may limit the length of unexpected_list
    # Incrementally add to result and return when all values for the specified level are present
    return_obj: Dict[str, Any] = {"success": success}

    if result_format["result_format"] == "BOOLEAN_ONLY":
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

    if unexpected_list is not None:
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
        return_obj["result"][
            "unexpected_percent_nonmissing"
        ] = unexpected_percent_nonmissing

    if result_format["include_unexpected_rows"]:
        return_obj["result"].update(
            {
                "unexpected_rows": unexpected_rows,
            }
        )

    if result_format["result_format"] == "BASIC":
        return return_obj

    if unexpected_list is not None:
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
    partial_unexpected_count: Optional[int] = result_format.get(
        "partial_unexpected_count"
    )
    partial_unexpected_counts: Optional[List[Dict[str, Any]]] = None
    if partial_unexpected_count is not None and 0 < partial_unexpected_count:
        try:
            partial_unexpected_counts = [
                {"value": key, "count": value}
                for key, value in sorted(
                    Counter(immutable_unexpected_list).most_common(
                        result_format["partial_unexpected_count"]
                    ),
                    key=lambda x: (-x[1], x[0]),
                )
            ]
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
            return_obj["result"].update(
                {"partial_unexpected_counts": partial_unexpected_counts}
            )

    if result_format["result_format"] == "SUMMARY":
        return return_obj

    if unexpected_list is not None:
        return_obj["result"].update({"unexpected_list": unexpected_list})
    if unexpected_index_list is not None:
        return_obj["result"].update({"unexpected_index_list": unexpected_index_list})
    if unexpected_index_query is not None:
        return_obj["result"].update({"unexpected_index_query": unexpected_index_query})
    if result_format["result_format"] == "COMPLETE":
        return return_obj

    raise ValueError(f"Unknown result_format {result_format['result_format']}.")


def _validate_mostly_config(configuration: ExpectationConfiguration) -> None:
    """
    Validates "mostly" in ExpectationConfiguration is a number if it exists.

    Args:
        configuration: The ExpectationConfiguration to be validated

    Raises:
        AssertionError: An error is mostly exists in the configuration but is not between 0 and 1.
    """
    if "mostly" in configuration.kwargs:
        mostly = configuration.kwargs["mostly"]
        assert isinstance(
            mostly, (int, float)
        ), "'mostly' parameter must be an integer or float"
        assert 0 <= mostly <= 1, "'mostly' parameter must be between 0 and 1"


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
    """
    # deprecated-v0.15.43
    warnings.warn(
        "The method add_values_with_json_schema_from_list_in_params is deprecated as of v0.15.43 and will be removed in "
        "v0.18. Please refer to Expectation method _prescriptive_template for the latest renderer template pattern.",
        DeprecationWarning,
    )
    target_list = params.get(param_key_with_list)
    if target_list is not None and len(target_list) > 0:
        for i, v in enumerate(target_list):
            params_with_json_schema[f"v__{str(i)}"] = {
                "schema": {"type": list_values_type},
                "value": v,
            }
    return params_with_json_schema
