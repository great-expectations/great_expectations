import glob
import json
import logging
import os
import traceback
import warnings
from abc import ABC, ABCMeta, abstractmethod
from collections import Counter
from copy import deepcopy
from inspect import isabstract
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
from dateutil.parser import parse
from numpy import negative

from great_expectations import __version__ as ge_version
from great_expectations import execution_engine
from great_expectations.core import expectation_configuration
from great_expectations.core.batch import Batch
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
    ExpectationDescriptionDiagnostics,
    ExpectationDiagnosticMaturityMessages,
    ExpectationErrorDiagnostics,
    ExpectationExecutionEngineDiagnostics,
    ExpectationMetricDiagnostics,
    ExpectationRendererDiagnostics,
    ExpectationTestDiagnostics,
    RendererTestDiagnostics,
)
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.core.util import nested_update
from great_expectations.exceptions import (
    GreatExpectationsError,
    InvalidExpectationConfigurationError,
    InvalidExpectationKwargsError,
)
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.expectations.registry import (
    _registered_metrics,
    _registered_renderers,
    get_metric_kwargs,
    register_expectation,
    register_renderer,
)
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import (
    CollapseContent,
    RenderedAtomicContent,
    RenderedContentBlockContainer,
    RenderedGraphContent,
    RenderedStringTemplateContent,
    RenderedTableContent,
    ValueListContent,
    renderedAtomicValueSchema,
)
from great_expectations.render.util import num_to_str
from great_expectations.self_check.util import (
    evaluate_json_test_cfe,
    generate_expectation_tests,
)
from great_expectations.util import camel_to_snake, is_parseable_date
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.validator import Validator

logger = logging.getLogger(__name__)


_TEST_DEFS_DIR = os.path.join(
    os.path.dirname(__file__),
    "..",
    "..",
    "tests",
    "test_definitions",
)


class MetaExpectation(ABCMeta):
    """MetaExpectation registers Expectations as they are defined, adding them to the Expectation registry.

    Any class inheriting from Expectation will be registered based on the value of the "expectation_type" class
    attribute, or, if that is not set, by snake-casing the name of the class.
    """

    def __new__(cls, clsname, bases, attrs):
        newclass = super().__new__(cls, clsname, bases, attrs)
        if not newclass.is_abstract():
            newclass.expectation_type = camel_to_snake(clsname)
            register_expectation(newclass)
        newclass._register_renderer_functions()
        default_kwarg_values = {}
        for base in reversed(bases):
            default_kwargs = getattr(base, "default_kwarg_values", {})
            default_kwarg_values = nested_update(default_kwarg_values, default_kwargs)

        newclass.default_kwarg_values = nested_update(
            default_kwarg_values, attrs.get("default_kwarg_values", {})
        )
        return newclass


class Expectation(metaclass=MetaExpectation):
    """Base class for all Expectations.

    Expectation classes *must* have the following attributes set:
        1. `domain_keys`: a tuple of the *keys* used to determine the domain of the
           expectation
        2. `success_keys`: a tuple of the *keys* used to determine the success of
           the expectation.

    In some cases, subclasses of Expectation (such as TableExpectation) can
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
    domain_keys = tuple()
    success_keys = tuple()
    runtime_keys = (
        "include_config",
        "catch_exceptions",
        "result_format",
    )
    default_kwarg_values = {
        "include_config": True,
        "catch_exceptions": False,
        "result_format": "BASIC",
    }
    args_keys = None

    def __init__(self, configuration: Optional[ExpectationConfiguration] = None):
        if configuration is not None:
            self.validate_configuration(configuration)
        self._configuration = configuration

    @classmethod
    def is_abstract(cls):
        return isabstract(cls)

    @classmethod
    def _register_renderer_functions(cls):
        expectation_type = camel_to_snake(cls.__name__)

        for candidate_renderer_fn_name in dir(cls):
            attr_obj = getattr(cls, candidate_renderer_fn_name)
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
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        raise NotImplementedError

    @classmethod
    def _atomic_prescriptive_template(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        """
        Template function that contains the logic that is shared by atomic.prescriptive.summary (GE Cloud) and
        renderer.prescriptive (OSS GE)
        """
        if runtime_configuration is None:
            runtime_configuration = {}

        styling = runtime_configuration.get("styling")

        template_str = "$expectation_type(**$kwargs)"
        params = {
            "expectation_type": configuration.expectation_type,
            "kwargs": configuration.kwargs,
        }

        params_with_json_schema = {
            "expectation_type": {
                "schema": {"type": "string"},
                "value": configuration.expectation_type,
            },
            "kwargs": {"schema": {"type": "string"}, "value": configuration.kwargs},
        }
        return (template_str, params_with_json_schema, styling)

    @classmethod
    @renderer(renderer_type="atomic.prescriptive.summary")
    @render_evaluation_parameter_string
    def _prescriptive_summary(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        """
        Rendering function that is utilized by GE Cloud Front-end
        """
        (
            template_str,
            params_with_json_schema,
            styling,
        ) = cls._atomic_prescriptive_template(
            configuration, result, language, runtime_configuration, **kwargs
        )
        value_obj = renderedAtomicValueSchema.load(
            {
                "template": template_str,
                "params": params_with_json_schema,
                "schema": {"type": "com.superconductive.rendered.string"},
            }
        )
        rendered = RenderedAtomicContent(
            name="atomic.prescriptive.summary",
            value=value_obj,
            value_type="StringValueType",
        )
        return rendered

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    def _prescriptive_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "styling": {"parent": {"classes": ["alert", "alert-warning"]}},
                    "string_template": {
                        "template": "$expectation_type(**$kwargs)",
                        "params": {
                            "expectation_type": configuration.expectation_type,
                            "kwargs": configuration.kwargs,
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
    @renderer(renderer_type="renderer.diagnostic.meta_properties")
    def _diagnostic_meta_properties_renderer(cls, result=None, **kwargs):
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

        if result is None:
            return []
        custom_property_values = []
        meta_properties_to_render = result.expectation_config.kwargs.get(
            "meta_properties_to_render", None
        )
        if meta_properties_to_render is not None:
            for key in sorted(meta_properties_to_render.keys()):
                meta_property = meta_properties_to_render[key]
                if meta_property is not None:
                    try:
                        # Allow complex structure with . usage
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
                else:
                    custom_property_values.append(["N/A"])
        return custom_property_values

    @classmethod
    @renderer(renderer_type="renderer.diagnostic.status_icon")
    def _diagnostic_status_icon_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        assert result, "Must provide a result object."
        if result.exception_info["raised_exception"]:
            return RenderedStringTemplateContent(
                **{
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
                **{
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
                **{
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
    @renderer(renderer_type="renderer.diagnostic.unexpected_statement")
    def _diagnostic_unexpected_statement_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        assert result, "Must provide a result object."
        success = result.success
        result_dict = result.result

        if result.exception_info["raised_exception"]:
            exception_message_template_str = (
                "\n\n$expectation_type raised an exception:\n$exception_message"
            )

            exception_message = RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": exception_message_template_str,
                        "params": {
                            "expectation_type": result.expectation_config.expectation_type,
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
                **{
                    "collapse_toggle_link": "Show exception traceback...",
                    "collapse": [
                        RenderedStringTemplateContent(
                            **{
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
                    **{
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
    @renderer(renderer_type="renderer.diagnostic.unexpected_table")
    def _diagnostic_unexpected_table_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        try:
            result_dict = result.result
        except KeyError:
            return None

        if result_dict is None:
            return None

        if not result_dict.get("partial_unexpected_list") and not result_dict.get(
            "partial_unexpected_counts"
        ):
            return None

        table_rows = []

        if result_dict.get("partial_unexpected_counts"):
            # We will check to see whether we have *all* of the unexpected values
            # accounted for in our count, and include counts if we do. If we do not,
            # we will use this as simply a better (non-repeating) source of
            # "sampled" unexpected values
            total_count = 0
            for unexpected_count_dict in result_dict.get("partial_unexpected_counts"):
                value = unexpected_count_dict.get("value")
                count = unexpected_count_dict.get("count")
                total_count += count
                if value is not None and value != "":
                    table_rows.append([value, count])
                elif value == "":
                    table_rows.append(["EMPTY", count])
                else:
                    table_rows.append(["null", count])

            # Check to see if we have *all* of the unexpected values accounted for. If so,
            # we show counts. If not, we only show "sampled" unexpected values.
            if total_count == result_dict.get("unexpected_count"):
                header_row = ["Unexpected Value", "Count"]
            else:
                header_row = ["Sampled Unexpected Values"]
                table_rows = [[row[0]] for row in table_rows]
        else:
            header_row = ["Sampled Unexpected Values"]
            sampled_values_set = set()
            for unexpected_value in result_dict.get("partial_unexpected_list"):
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
            **{
                "content_block_type": "table",
                "table": table_rows,
                "header_row": header_row,
                "styling": {
                    "body": {"classes": ["table-bordered", "table-sm", "mt-3"]}
                },
            }
        )

        return unexpected_table_content_block

    @classmethod
    def _get_observed_value_from_evr(self, result: ExpectationValidationResult) -> str:
        result_dict = result.result
        if result_dict is None:
            return "--"

        if result_dict.get("observed_value") is not None:
            observed_value = result_dict.get("observed_value")
            if isinstance(observed_value, (int, float)) and not isinstance(
                observed_value, bool
            ):
                return num_to_str(observed_value, precision=10, use_locale=True)
            return str(observed_value)
        elif result_dict.get("unexpected_percent") is not None:
            return (
                num_to_str(result_dict.get("unexpected_percent"), precision=5)
                + "% unexpected"
            )
        else:
            return "--"

    @classmethod
    @renderer(renderer_type="atomic.diagnostic.observed_value")
    def _atomic_diagnostic_observed_value(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        """
        Rendering function that is utilized by GE Cloud Front-end
        """
        observed_value = cls._get_observed_value_from_evr(result=result)
        value_obj = renderedAtomicValueSchema.load(
            {
                "template": observed_value,
                "params": {},
                "schema": {"type": "com.superconductive.rendered.string"},
            }
        )
        rendered = RenderedAtomicContent(
            name="atomic.diagnostic.observed_value",
            value=value_obj,
            value_type="StringValueType",
        )
        return rendered

    @classmethod
    @renderer(renderer_type="renderer.diagnostic.observed_value")
    def _diagnostic_observed_value_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        return cls._get_observed_value_from_evr(result=result)

    @classmethod
    def get_allowed_config_keys(cls):
        return cls.domain_keys + cls.success_keys + cls.runtime_keys

    def metrics_validate(
        self,
        metrics: Dict,
        configuration: Optional[ExpectationConfiguration] = None,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ) -> ExpectationValidationResult:
        if configuration is None:
            configuration = self.configuration

        validation_dependencies: dict = self.get_validation_dependencies(
            configuration,
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )
        runtime_configuration["result_format"] = validation_dependencies[
            "result_format"
        ]
        requested_metrics = validation_dependencies["metrics"]

        provided_metrics = {}
        for name, metric_edge_key in requested_metrics.items():
            provided_metrics[name] = metrics[metric_edge_key.id]

        expectation_validation_result: Union[
            ExpectationValidationResult, dict
        ] = self._validate(
            configuration=configuration,
            metrics=provided_metrics,
            runtime_configuration=runtime_configuration,
            execution_engine=execution_engine,
        )
        evr: ExpectationValidationResult = self._build_evr(
            raw_response=expectation_validation_result, configuration=configuration
        )
        return evr

    @staticmethod
    def _build_evr(raw_response, configuration) -> ExpectationValidationResult:
        """_build_evr is a lightweight convenience wrapper handling cases where an Expectation implementor
        fails to return an EVR but returns the necessary components in a dictionary."""
        if not isinstance(raw_response, ExpectationValidationResult):
            if isinstance(raw_response, dict):
                evr = ExpectationValidationResult(**raw_response)
                evr.expectation_config = configuration
            else:
                raise GreatExpectationsError("Unable to build EVR")
        else:
            evr = raw_response
            evr.expectation_config = configuration
        return evr

    def get_validation_dependencies(
        self,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> dict:
        """Returns the result format and metrics required to validate this Expectation using the provided result format."""
        runtime_configuration = self.get_runtime_kwargs(
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        result_format: dict = runtime_configuration["result_format"]
        result_format = parse_result_format(result_format=result_format)
        return {
            "result_format": result_format,
            "metrics": {},
        }

    def get_domain_kwargs(
        self, configuration: Optional[ExpectationConfiguration] = None
    ):
        if not configuration:
            configuration = self.configuration

        domain_kwargs = {
            key: configuration.kwargs.get(key, self.default_kwarg_values.get(key))
            for key in self.domain_keys
        }
        # Process evaluation parameter dependencies

        missing_kwargs = set(self.domain_keys) - set(domain_kwargs.keys())
        if missing_kwargs:
            raise InvalidExpectationKwargsError(
                f"Missing domain kwargs: {list(missing_kwargs)}"
            )
        return domain_kwargs

    def get_success_kwargs(
        self, configuration: Optional[ExpectationConfiguration] = None
    ):
        if not configuration:
            configuration = self.configuration

        domain_kwargs = self.get_domain_kwargs(configuration)
        success_kwargs = {
            key: configuration.kwargs.get(key, self.default_kwarg_values.get(key))
            for key in self.success_keys
        }
        success_kwargs.update(domain_kwargs)
        return success_kwargs

    def get_runtime_kwargs(
        self,
        configuration: Optional[ExpectationConfiguration] = None,
        runtime_configuration: dict = None,
    ) -> dict:
        if not configuration:
            configuration = self.configuration

        configuration = deepcopy(configuration)

        if runtime_configuration:
            configuration.kwargs.update(runtime_configuration)

        success_kwargs = self.get_success_kwargs(configuration)
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
        runtime_configuration: dict = None,
    ) -> dict:
        default_result_format: Optional[
            Union[bool, str]
        ] = self.default_kwarg_values.get("result_format")
        configuration_result_format: dict = configuration.kwargs.get(
            "result_format", default_result_format
        )
        result_format: dict
        if runtime_configuration:
            result_format = runtime_configuration.get(
                "result_format",
                configuration_result_format,
            )
        else:
            result_format = configuration_result_format
        return result_format

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        if configuration is None:
            configuration = self.configuration
        try:
            assert (
                configuration.expectation_type == self.expectation_type
            ), f"expectation configuration type {configuration.expectation_type} does not match expectation type {self.expectation_type}"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    def validate(
        self,
        validator: Validator,
        configuration: Optional[ExpectationConfiguration] = None,
        evaluation_parameters=None,
        interactive_evaluation=True,
        data_context=None,
        runtime_configuration=None,
    ):
        if configuration is None:
            configuration = deepcopy(self.configuration)

        configuration.process_evaluation_parameters(
            evaluation_parameters, interactive_evaluation, data_context
        )
        evr = validator.graph_validate(
            configurations=[configuration],
            runtime_configuration=runtime_configuration,
        )[0]

        return evr

    @property
    def configuration(self):
        if self._configuration is None:
            raise InvalidExpectationConfigurationError(
                "cannot access configuration: expectation has not yet been configured"
            )
        return self._configuration

    @classmethod
    def build_configuration(cls, *args, **kwargs):
        # Combine all arguments into a single new "all_args" dictionary to name positional parameters
        all_args = dict(zip(cls.validation_kwargs, args))
        all_args.update(kwargs)

        # Unpack display parameters; remove them from all_args if appropriate
        if "include_config" in kwargs:
            include_config = kwargs["include_config"]
            del all_args["include_config"]
        else:
            include_config = cls.default_expectation_args["include_config"]

        if "catch_exceptions" in kwargs:
            catch_exceptions = kwargs["catch_exceptions"]
            del all_args["catch_exceptions"]
        else:
            catch_exceptions = cls.default_expectation_args["catch_exceptions"]

        if "result_format" in kwargs:
            result_format = kwargs["result_format"]
        else:
            result_format = cls.default_expectation_args["result_format"]

        # Extract the meta object for use as a top-level expectation_config holder
        if "meta" in kwargs:
            meta = kwargs["meta"]
            del all_args["meta"]
        else:
            meta = None

        # Construct the expectation_config object
        return ExpectationConfiguration(
            expectation_type=cls.expectation_type,
            kwargs=convert_to_json_serializable(deepcopy(all_args)),
            meta=meta,
        )

    def run_diagnostics(
        self,
        raise_exceptions_for_backends: bool = False,
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
        """

        # errors :List[ExpectationErrorDiagnostics] = []

        library_metadata: ExpectationDescriptionDiagnostics = (
            self._get_augmented_library_metadata()
        )
        examples: List[ExpectationTestDataCases] = self._get_examples(
            return_only_gallery_examples=False
        )
        gallery_examples: List[ExpectationTestDataCases] = []
        for example in examples:
            _tests_to_include = [
                test
                for test in example.tests
                if test.include_in_gallery
            ]
            example = deepcopy(example)
            if _tests_to_include:
                example.tests = _tests_to_include
                gallery_examples.append(example)

        description_diagnostics: ExpectationDescriptionDiagnostics = (
            self._get_description_diagnostics()
        )

        _expectation_config: ExpectationConfiguration = self._get_expectation_configuration_from_examples(
            examples
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

        test_results: List[ExpectationTestDiagnostics] = self._get_test_results(
            expectation_type=description_diagnostics.snake_name,
            test_data_cases=examples,
            execution_engine_diagnostics=introspected_execution_engines,
            raise_exceptions_for_backends=raise_exceptions_for_backends,
        )

        renderers: List[
            ExpectationRendererDiagnostics
        ] = self._get_renderer_diagnostics(
            expectation_type=description_diagnostics.snake_name,
            test_diagnostics=test_results,
            registered_renderers=_registered_renderers,
        )

        maturity_checklist: ExpectationDiagnosticMaturityMessages = (
            self._get_maturity_checklist(
                library_metadata=library_metadata,
                description=description_diagnostics,
                examples=examples,
                tests=test_results,
                execution_engines=introspected_execution_engines,
            )
        )

        return ExpectationDiagnostics(
            library_metadata=library_metadata,
            examples=examples,
            gallery_examples=gallery_examples,
            description=description_diagnostics,
            renderers=renderers,
            metrics=metric_diagnostics_list,
            execution_engines=introspected_execution_engines,
            tests=test_results,
            maturity_checklist=maturity_checklist,
            errors=[],  #!!!FIXME!!!
        )

    def print_diagnostic_checklist(
        self,
        diagnostics: Optional[ExpectationDiagnostics] = None,
        show_failed_tests: bool = False,
    ) -> str:
        """Runs self.run_diagnostics and generates a diagnostic checklist.

        This output from this method is a thin wrapper for ExpectationDiagnostics.generate_checklist()
        This method is experimental.
        """

        if diagnostics is None:
            diagnostics = self.run_diagnostics()

        if show_failed_tests:
            for test in diagnostics.tests:
                if test.test_passed is False:
                    print(f"=== {test.test_title} ({test.backend}) ===\n")
                    print(test.stack_trace)
                    print(f"{80 * '='}\n")

        checklist: str = diagnostics.generate_checklist()
        print(checklist)

        return checklist

    def _get_examples_from_json(self):
        """Only meant to be called by self._get_examples"""
        results = []
        found = glob.glob(
            os.path.join(_TEST_DEFS_DIR, "**", f"{self.expectation_type}.json"),
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
        try:
            # Currently, only community contrib expectations have an examples attribute
            all_examples = self.examples
        except AttributeError:
            all_examples = self._get_examples_from_json()
            if all_examples == []:
                return []

        included_examples = []
        for example in all_examples:

            included_test_cases = []
            # As of commit 7766bb5caa4e0 on 1/28/22, only_for does not need to be applied to individual tests
            # See:
            #   - https://github.com/great-expectations/great_expectations/blob/7766bb5caa4e0e5b22fa3b3a5e1f2ac18922fdeb/tests/test_definitions/column_map_expectations/expect_column_values_to_be_unique.json#L174
            #   - https://github.com/great-expectations/great_expectations/pull/4073
            top_level_only_for = example.get("only_for")
            for test in example["tests"]:
                if (
                    test.get("include_in_gallery") == True
                    or return_only_gallery_examples == False
                ):
                    copied_test = deepcopy(test)
                    if top_level_only_for and "only_for" not in copied_test:
                        copied_test["only_for"] = top_level_only_for
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
                if "test_backends" in copied_example:
                    copied_example["test_backends"] = [
                        TestBackend(**tb) for tb in copied_example["test_backends"]
                    ]
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
    ) -> ExpectationConfiguration:
        """Return an ExpectationConfiguration instance using test input expected to succeed"""
        if examples:
            for example in examples:
                tests = example.tests
                if tests:
                    for test in tests:
                        if test.output.get("success"):
                            return ExpectationConfiguration(
                                expectation_type=self.expectation_type,
                                kwargs=test.input
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
    def _get_test_results(
        cls,
        expectation_type: str,
        test_data_cases: List[ExpectationTestDataCases],
        execution_engine_diagnostics: ExpectationExecutionEngineDiagnostics,
        raise_exceptions_for_backends: bool = False,
    ) -> List[ExpectationTestDiagnostics]:
        """Generate test results. This is an internal method for run_diagnostics."""
        test_results = []

        exp_tests = generate_expectation_tests(
            expectation_type=expectation_type,
            test_data_cases=test_data_cases,
            execution_engine_diagnostics=execution_engine_diagnostics,
            raise_exceptions_for_backends=raise_exceptions_for_backends,
        )

        for exp_test in exp_tests:
            try:
                validation_result = evaluate_json_test_cfe(
                    validator=exp_test["validator_with_data"],
                    expectation_type=exp_test["expectation_type"],
                    test=exp_test["test"],
                )
                test_passed = True
                error_diagnostics = None

            except Exception as e:
                error_diagnostics = ExpectationErrorDiagnostics(
                    error_msg=str(e),
                    stack_trace=traceback.format_exc(),
                )
                test_passed = False
                validation_result = []
            else:
                # The ExpectationTestDiagnostics instance will error when calling it's to_dict()
                # method (AttributeError: 'ExpectationConfiguration' object has no attribute 'raw_kwargs')
                validation_result.expectation_config.raw_kwargs = validation_result.expectation_config._raw_kwargs

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

        return test_results

    def _get_rendered_result_as_string(self, rendered_result) -> str:
        """Convenience method to get rendered results as strings."""

        if type(rendered_result) == str:
            return rendered_result

        elif type(rendered_result) == list:
            sub_result_list = []
            for sub_result in rendered_result:
                res = self._get_rendered_result_as_string(sub_result)
                if res is not None:
                    sub_result_list.append(res)

            return "\n".join(sub_result_list)

        elif isinstance(rendered_result, RenderedStringTemplateContent):
            return rendered_result.__str__()

        elif isinstance(rendered_result, CollapseContent):
            return rendered_result.__str__()

        elif isinstance(rendered_result, RenderedAtomicContent):
            return f"(RenderedAtomicContent) {repr(rendered_result.to_json_dict())}"

        elif isinstance(rendered_result, RenderedContentBlockContainer):
            return "(RenderedContentBlockContainer) " + repr(
                rendered_result.to_json_dict()
            )

        elif isinstance(rendered_result, RenderedTableContent):
            return f"(RenderedTableContent) {repr(rendered_result.to_json_dict())}"

        elif isinstance(rendered_result, RenderedGraphContent):
            return f"(RenderedGraphContent) {repr(rendered_result.to_json_dict())}"

        elif isinstance(rendered_result, ValueListContent):
            return f"(ValueListContent) {repr(rendered_result.to_json_dict())}"

        elif isinstance(rendered_result, dict):
            return f"(dict) {repr(rendered_result)}"

        elif isinstance(rendered_result, int):
            return repr(rendered_result)

        elif rendered_result == None:
            return ""

        else:
            raise TypeError(
                f"Expectation._get_rendered_result_as_string can't render type {type(rendered_result)} as a string."
            )

    def _get_renderer_diagnostics(
        self,
        expectation_type: str,
        test_diagnostics: List[ExpectationTestDiagnostics],
        registered_renderers: List[str],
        standard_renderers: List[str] = [
            "renderer.answer",
            "renderer.diagnostic.unexpected_statement",
            "renderer.diagnostic.observed_value",
            "renderer.diagnostic.status_icon",
            "renderer.diagnostic.unexpected_table",
            "renderer.prescriptive",
            "renderer.question",
        ],
    ) -> List[ExpectationRendererDiagnostics]:
        """Generate Renderer diagnostics for this Expectation, based primarily on a list of ExpectationTestDiagnostics."""

        supported_renderers = self._get_registered_renderers(
            expectation_type=expectation_type,
            registered_renderers=registered_renderers,
        )

        renderer_diagnostic_list = []
        for renderer_name in set(standard_renderers).union(set(supported_renderers)):
            samples = []
            if renderer_name in supported_renderers:
                _, renderer = registered_renderers[expectation_type][renderer_name]

                for test_diagnostic in test_diagnostics:
                    test_title = test_diagnostic["test_title"]

                    try:
                        rendered_result = renderer(
                            configuration=test_diagnostic["validation_result"]["expectation_config"],
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
        execution_engine_names: List[str] = [
            "PandasExecutionEngine",
            "SqlAlchemyExecutionEngine",
            "SparkDFExecutionEngine",
        ],
    ) -> ExpectationExecutionEngineDiagnostics:
        """Check to see which execution_engines are fully supported for this Expectation.

        In order for a given execution engine to count, *every* metric must have support on that execution engines.
        """

        execution_engines = {}
        for provider in execution_engine_names:
            all_true = True
            for metric_diagnostics in metric_diagnostics_list:
                try:
                    has_provider = (
                        provider
                        in registered_metrics[metric_diagnostics.name]["providers"]
                    )
                    if not has_provider:
                        all_true = False
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
        expectation_config: ExpectationConfiguration,
    ) -> List[ExpectationMetricDiagnostics]:
        """Check to see which Metrics are upstream dependencies for this Expectation."""

        # NOTE: Abe 20210102: Strictly speaking, identifying upstream metrics shouldn't need to rely on an expectation config.
        # There's probably some part of get_validation_dependencies that can be factored out to remove the dependency.

        if not expectation_config:
            return []
        validation_dependencies = self.get_validation_dependencies(
            configuration=expectation_config
        )

        metric_diagnostics_list = []
        for metric in validation_dependencies["metrics"].keys():
            new_metric_diagnostics = ExpectationMetricDiagnostics(
                name=metric,
                has_question_renderer=False,
            )
            metric_diagnostics_list.append(new_metric_diagnostics)

        return metric_diagnostics_list

    def _get_augmented_library_metadata(self):
        """Introspect the Expectation's library_metadata object (if it exists), and augment it with additional information."""

        augmented_library_metadata = {
            "maturity": "CONCEPT_ONLY",
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
            if not problems:
                augmented_library_metadata["library_metadata_passed_checks"] = True
        else:
            problems.append("No library_metadata attribute found")

        augmented_library_metadata["problems"] = problems
        return AugmentedLibraryMetadata.from_legacy_dict(augmented_library_metadata)

    def _get_maturity_checklist(
        self,
        library_metadata: AugmentedLibraryMetadata,
        description: ExpectationDescriptionDiagnostics,
        examples: List[ExpectationTestDataCases],
        tests: List[ExpectationTestDiagnostics],
        execution_engines: ExpectationExecutionEngineDiagnostics,
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
                tests
            )
        )
        experimental_checks.append(ExpectationDiagnostics._check_linting(self))

        beta_checks.append(
            ExpectationDiagnostics._check_input_validation(self, examples)
        )
        beta_checks.append(ExpectationDiagnostics._check_renderer_methods(self))
        beta_checks.append(
            ExpectationDiagnostics._check_core_logic_for_all_applicable_execution_engines(
                tests
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


class TableExpectation(Expectation, ABC):
    domain_keys = (
        "batch_id",
        "table",
        "row_condition",
        "condition_parser",
    )
    metric_dependencies = tuple()
    domain_type = MetricDomainTypes.TABLE

    def get_validation_dependencies(
        self,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        dependencies = super().get_validation_dependencies(
            configuration, execution_engine, runtime_configuration
        )
        for metric_name in self.metric_dependencies:
            metric_kwargs = get_metric_kwargs(
                metric_name=metric_name,
                configuration=configuration,
                runtime_configuration=runtime_configuration,
            )
            dependencies["metrics"][metric_name] = MetricConfiguration(
                metric_name=metric_name,
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            )

        return dependencies

    @staticmethod
    def validate_metric_value_between_configuration(
        configuration: Optional[ExpectationConfiguration],
    ):
        # Validating that Minimum and Maximum values are of the proper format and type
        min_val = None
        max_val = None

        if "min_value" in configuration.kwargs:
            min_val = configuration.kwargs["min_value"]

        if "max_value" in configuration.kwargs:
            max_val = configuration.kwargs["max_value"]

        try:
            assert (
                min_val is None
                or is_parseable_date(min_val)
                or isinstance(min_val, (float, int, dict))
            ), "Provided min threshold must be a datetime (for datetime columns) or number"
            if isinstance(min_val, dict):
                assert (
                    "$PARAMETER" in min_val
                ), 'Evaluation Parameter dict for min_value kwarg must have "$PARAMETER" key'

            assert (
                max_val is None
                or is_parseable_date(max_val)
                or isinstance(max_val, (float, int, dict))
            ), "Provided max threshold must be a datetime (for datetime columns) or number"
            if isinstance(max_val, dict):
                assert (
                    "$PARAMETER" in max_val
                ), 'Evaluation Parameter dict for max_value kwarg must have "$PARAMETER" key'

        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

        if min_val is not None and max_val is not None and min_val > max_val:
            raise InvalidExpectationConfigurationError(
                "Minimum Threshold cannot be larger than Maximum Threshold"
            )

        return True

    def _validate_metric_value_between(
        self,
        metric_name,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        metric_value = metrics.get(metric_name)

        if metric_value is None:
            return {"success": False, "result": {"observed_value": metric_value}}

        # Obtaining components needed for validation
        min_value = self.get_success_kwargs(configuration).get("min_value")
        strict_min = self.get_success_kwargs(configuration).get("strict_min")
        max_value = self.get_success_kwargs(configuration).get("max_value")
        strict_max = self.get_success_kwargs(configuration).get("strict_max")

        parse_strings_as_datetimes = self.get_success_kwargs(configuration).get(
            "parse_strings_as_datetimes"
        )

        if parse_strings_as_datetimes:
            # deprecated-v0.13.41
            warnings.warn(
                """The parameter "parse_strings_as_datetimes" is deprecated as of v0.13.41 in \
v0.16. As part of the V3 API transition, we've moved away from input transformation. For more information, \
please see: https://greatexpectations.io/blog/why_we_dont_do_transformations_for_expectations/
""",
                DeprecationWarning,
            )

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

        success = above_min and below_max

        return {"success": success, "result": {"observed_value": metric_value}}


class ColumnExpectation(TableExpectation, ABC):
    domain_keys = ("batch_id", "table", "column", "row_condition", "condition_parser")
    domain_type = MetricDomainTypes.COLUMN

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        # Ensuring basic configuration parameters are properly set
        try:
            assert (
                "column" in configuration.kwargs
            ), "'column' parameter is required for column expectations"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))


class ColumnMapExpectation(TableExpectation, ABC):
    map_metric = None
    domain_keys = ("batch_id", "table", "column", "row_condition", "condition_parser")
    domain_type = MetricDomainTypes.COLUMN
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
    def is_abstract(cls):
        return cls.map_metric is None or super().is_abstract()

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        super().validate_configuration(configuration)
        try:
            assert (
                "column" in configuration.kwargs
            ), "'column' parameter is required for column map expectations"
            if "mostly" in configuration.kwargs:
                mostly = configuration.kwargs["mostly"]
                assert isinstance(
                    mostly, (int, float)
                ), "'mostly' parameter must be an integer or float"
                assert 0 <= mostly <= 1, "'mostly' parameter must be between 0 and 1"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    def get_validation_dependencies(
        self,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        dependencies = super().get_validation_dependencies(
            configuration, execution_engine, runtime_configuration
        )
        assert isinstance(
            self.map_metric, str
        ), "ColumnMapExpectation must override get_validation_dependencies or declare exactly one map_metric"
        assert (
            self.metric_dependencies == tuple()
        ), "ColumnMapExpectation must be configured using map_metric, and cannot have metric_dependencies declared."
        # convenient name for updates

        metric_dependencies = dependencies["metrics"]

        metric_kwargs = get_metric_kwargs(
            metric_name="column_values.nonnull.unexpected_count",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        metric_dependencies[
            "column_values.nonnull.unexpected_count"
        ] = MetricConfiguration(
            "column_values.nonnull.unexpected_count",
            metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
            metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
        )
        metric_kwargs = get_metric_kwargs(
            metric_name=f"{self.map_metric}.unexpected_count",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        metric_dependencies[
            f"{self.map_metric}.unexpected_count"
        ] = MetricConfiguration(
            f"{self.map_metric}.unexpected_count",
            metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
            metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
        )

        metric_kwargs = get_metric_kwargs(
            metric_name="table.row_count",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        metric_dependencies["table.row_count"] = MetricConfiguration(
            metric_name="table.row_count",
            metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
            metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
        )

        result_format_str = dependencies["result_format"].get("result_format")
        include_unexpected_rows = dependencies["result_format"].get(
            "include_unexpected_rows"
        )

        if result_format_str == "BOOLEAN_ONLY":
            return dependencies

        metric_kwargs = get_metric_kwargs(
            f"{self.map_metric}.unexpected_values",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        metric_dependencies[
            f"{self.map_metric}.unexpected_values"
        ] = MetricConfiguration(
            metric_name=f"{self.map_metric}.unexpected_values",
            metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
            metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
        )

        if include_unexpected_rows:
            metric_kwargs = get_metric_kwargs(
                f"{self.map_metric}.unexpected_rows",
                configuration=configuration,
                runtime_configuration=runtime_configuration,
            )
            metric_dependencies[
                f"{self.map_metric}.unexpected_rows"
            ] = MetricConfiguration(
                metric_name=f"{self.map_metric}.unexpected_rows",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            )

        if result_format_str in ["BASIC", "SUMMARY"]:
            return dependencies

        if include_unexpected_rows:
            metric_kwargs = get_metric_kwargs(
                f"{self.map_metric}.unexpected_rows",
                configuration=configuration,
                runtime_configuration=runtime_configuration,
            )
            metric_dependencies[
                f"{self.map_metric}.unexpected_rows"
            ] = MetricConfiguration(
                metric_name=f"{self.map_metric}.unexpected_rows",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            )

        if isinstance(execution_engine, PandasExecutionEngine):
            metric_kwargs = get_metric_kwargs(
                f"{self.map_metric}.unexpected_index_list",
                configuration=configuration,
                runtime_configuration=runtime_configuration,
            )
            metric_dependencies[
                f"{self.map_metric}.unexpected_index_list"
            ] = MetricConfiguration(
                metric_name=f"{self.map_metric}.unexpected_index_list",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            )

        return dependencies

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        result_format = self.get_result_format(
            configuration=configuration, runtime_configuration=runtime_configuration
        )
        include_unexpected_rows = result_format.get("include_unexpected_rows")

        mostly = self.get_success_kwargs().get(
            "mostly", self.default_kwarg_values.get("mostly")
        )
        total_count = metrics.get("table.row_count")
        null_count = metrics.get("column_values.nonnull.unexpected_count")
        unexpected_count = metrics.get(f"{self.map_metric}.unexpected_count")
        unexpected_values = metrics.get(f"{self.map_metric}.unexpected_values")
        unexpected_index_list = metrics.get(f"{self.map_metric}.unexpected_index_list")
        unexpected_rows = None
        if include_unexpected_rows:
            unexpected_rows = metrics.get(f"{self.map_metric}.unexpected_rows")

        if total_count is None or null_count is None:
            total_count = nonnull_count = 0
        else:
            nonnull_count = total_count - null_count

        success = None
        if total_count == 0 or nonnull_count == 0:
            # Vacuously true
            success = True
        elif nonnull_count > 0:
            success_ratio = float(nonnull_count - unexpected_count) / nonnull_count
            success = success_ratio >= mostly

        return _format_map_output(
            result_format=parse_result_format(result_format),
            success=success,
            element_count=total_count,
            nonnull_count=nonnull_count,
            unexpected_count=unexpected_count,
            unexpected_list=unexpected_values,
            unexpected_index_list=unexpected_index_list,
            unexpected_rows=unexpected_rows,
        )


class ColumnPairMapExpectation(TableExpectation, ABC):
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
    def is_abstract(cls):
        return cls.map_metric is None or super().is_abstract()

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        super().validate_configuration(configuration)
        try:
            assert (
                "column_A" in configuration.kwargs
            ), "'column_A' parameter is required for column pair map expectations"
            assert (
                "column_B" in configuration.kwargs
            ), "'column_B' parameter is required for column pair map expectations"
            if "mostly" in configuration.kwargs:
                mostly = configuration.kwargs["mostly"]
                assert isinstance(
                    mostly, (int, float)
                ), "'mostly' parameter must be an integer or float"
                assert 0 <= mostly <= 1, "'mostly' parameter must be between 0 and 1"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    def get_validation_dependencies(
        self,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        dependencies = super().get_validation_dependencies(
            configuration, execution_engine, runtime_configuration
        )
        assert isinstance(
            self.map_metric, str
        ), "ColumnPairMapExpectation must override get_validation_dependencies or declare exactly one map_metric"
        assert (
            self.metric_dependencies == tuple()
        ), "ColumnPairMapExpectation must be configured using map_metric, and cannot have metric_dependencies declared."
        # convenient name for updates

        metric_dependencies = dependencies["metrics"]

        metric_kwargs = get_metric_kwargs(
            metric_name=f"{self.map_metric}.unexpected_count",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        metric_dependencies[
            f"{self.map_metric}.unexpected_count"
        ] = MetricConfiguration(
            f"{self.map_metric}.unexpected_count",
            metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
            metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
        )

        metric_kwargs = get_metric_kwargs(
            metric_name="table.row_count",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        metric_dependencies["table.row_count"] = MetricConfiguration(
            metric_name="table.row_count",
            metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
            metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
        )

        metric_kwargs = get_metric_kwargs(
            f"{self.map_metric}.filtered_row_count",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        metric_dependencies[
            f"{self.map_metric}.filtered_row_count"
        ] = MetricConfiguration(
            metric_name=f"{self.map_metric}.filtered_row_count",
            metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
            metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
        )

        result_format_str = dependencies["result_format"].get("result_format")
        include_unexpected_rows = dependencies["result_format"].get(
            "include_unexpected_rows"
        )

        if result_format_str == "BOOLEAN_ONLY":
            return dependencies

        metric_kwargs = get_metric_kwargs(
            f"{self.map_metric}.unexpected_values",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        metric_dependencies[
            f"{self.map_metric}.unexpected_values"
        ] = MetricConfiguration(
            metric_name=f"{self.map_metric}.unexpected_values",
            metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
            metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
        )

        if result_format_str in ["BASIC", "SUMMARY"]:
            return dependencies

        if include_unexpected_rows:
            metric_kwargs = get_metric_kwargs(
                f"{self.map_metric}.unexpected_rows",
                configuration=configuration,
                runtime_configuration=runtime_configuration,
            )
            metric_dependencies[
                f"{self.map_metric}.unexpected_rows"
            ] = MetricConfiguration(
                metric_name=f"{self.map_metric}.unexpected_rows",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            )

        if isinstance(execution_engine, PandasExecutionEngine):
            metric_kwargs = get_metric_kwargs(
                f"{self.map_metric}.unexpected_index_list",
                configuration=configuration,
                runtime_configuration=runtime_configuration,
            )
            metric_dependencies[
                f"{self.map_metric}.unexpected_index_list"
            ] = MetricConfiguration(
                metric_name=f"{self.map_metric}.unexpected_index_list",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            )

        return dependencies

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        result_format = self.get_result_format(
            configuration=configuration, runtime_configuration=runtime_configuration
        )
        mostly = self.get_success_kwargs().get(
            "mostly", self.default_kwarg_values.get("mostly")
        )
        total_count = metrics.get("table.row_count")
        unexpected_count = metrics.get(f"{self.map_metric}.unexpected_count")
        unexpected_values = metrics.get(f"{self.map_metric}.unexpected_values")
        unexpected_index_list = metrics.get(f"{self.map_metric}.unexpected_index_list")
        filtered_row_count = metrics.get(f"{self.map_metric}.filtered_row_count")

        if (
            total_count is None
            or filtered_row_count is None
            or total_count == 0
            or filtered_row_count == 0
        ):
            # Vacuously true
            success = True
        else:
            success_ratio = (
                float(filtered_row_count - unexpected_count) / filtered_row_count
            )
            success = success_ratio >= mostly

        return _format_map_output(
            result_format=parse_result_format(result_format),
            success=success,
            element_count=total_count,
            nonnull_count=filtered_row_count,
            unexpected_count=unexpected_count,
            unexpected_list=unexpected_values,
            unexpected_index_list=unexpected_index_list,
        )


class MulticolumnMapExpectation(TableExpectation, ABC):
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
    success_keys = tuple()
    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "ignore_row_if": "all_value_are_missing",
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": True,
    }

    @classmethod
    def is_abstract(cls):
        return cls.map_metric is None or super().is_abstract()

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        super().validate_configuration(configuration)
        try:
            assert (
                "column_list" in configuration.kwargs
            ), "'column_list' parameter is required for multicolumn map expectations"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    def get_validation_dependencies(
        self,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        dependencies = super().get_validation_dependencies(
            configuration, execution_engine, runtime_configuration
        )
        assert isinstance(
            self.map_metric, str
        ), "MulticolumnMapExpectation must override get_validation_dependencies or declare exactly one map_metric"
        assert (
            self.metric_dependencies == tuple()
        ), "MulticolumnMapExpectation must be configured using map_metric, and cannot have metric_dependencies declared."
        # convenient name for updates

        metric_dependencies = dependencies["metrics"]

        metric_kwargs = get_metric_kwargs(
            metric_name=f"{self.map_metric}.unexpected_count",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        metric_dependencies[
            f"{self.map_metric}.unexpected_count"
        ] = MetricConfiguration(
            f"{self.map_metric}.unexpected_count",
            metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
            metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
        )

        metric_kwargs = get_metric_kwargs(
            metric_name="table.row_count",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        metric_dependencies["table.row_count"] = MetricConfiguration(
            metric_name="table.row_count",
            metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
            metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
        )

        metric_kwargs = get_metric_kwargs(
            f"{self.map_metric}.filtered_row_count",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        metric_dependencies[
            f"{self.map_metric}.filtered_row_count"
        ] = MetricConfiguration(
            metric_name=f"{self.map_metric}.filtered_row_count",
            metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
            metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
        )

        result_format_str = dependencies["result_format"].get("result_format")
        include_unexpected_rows = dependencies["result_format"].get(
            "include_unexpected_rows"
        )

        if result_format_str == "BOOLEAN_ONLY":
            return dependencies

        metric_kwargs = get_metric_kwargs(
            f"{self.map_metric}.unexpected_values",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        metric_dependencies[
            f"{self.map_metric}.unexpected_values"
        ] = MetricConfiguration(
            metric_name=f"{self.map_metric}.unexpected_values",
            metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
            metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
        )

        if result_format_str in ["BASIC", "SUMMARY"]:
            return dependencies

        if include_unexpected_rows:
            metric_kwargs = get_metric_kwargs(
                f"{self.map_metric}.unexpected_rows",
                configuration=configuration,
                runtime_configuration=runtime_configuration,
            )
            metric_dependencies[
                f"{self.map_metric}.unexpected_rows"
            ] = MetricConfiguration(
                metric_name=f"{self.map_metric}.unexpected_rows",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            )

        if isinstance(execution_engine, PandasExecutionEngine):
            metric_kwargs = get_metric_kwargs(
                f"{self.map_metric}.unexpected_index_list",
                configuration=configuration,
                runtime_configuration=runtime_configuration,
            )
            metric_dependencies[
                f"{self.map_metric}.unexpected_index_list"
            ] = MetricConfiguration(
                metric_name=f"{self.map_metric}.unexpected_index_list",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            )

        return dependencies

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        result_format = self.get_result_format(
            configuration=configuration, runtime_configuration=runtime_configuration
        )
        total_count = metrics.get("table.row_count")
        unexpected_count = metrics.get(f"{self.map_metric}.unexpected_count")
        unexpected_values = metrics.get(f"{self.map_metric}.unexpected_values")
        unexpected_index_list = metrics.get(f"{self.map_metric}.unexpected_index_list")
        filtered_row_count = metrics.get(f"{self.map_metric}.filtered_row_count")

        if (
            total_count is None
            or filtered_row_count is None
            or total_count == 0
            or filtered_row_count == 0
        ):
            # Vacuously true
            success = True
        else:
            success = unexpected_count == 0

        return _format_map_output(
            result_format=parse_result_format(result_format),
            success=success,
            element_count=total_count,
            nonnull_count=filtered_row_count,
            unexpected_count=unexpected_count,
            unexpected_list=unexpected_values,
            unexpected_index_list=unexpected_index_list,
        )


def _format_map_output(
    result_format,
    success,
    element_count,
    nonnull_count,
    unexpected_count,
    unexpected_list,
    unexpected_index_list,
    unexpected_rows=None,
):
    """Helper function to construct expectation result objects for map_expectations (such as column_map_expectation
    and file_lines_map_expectation).

    Expectations support four result_formats: BOOLEAN_ONLY, BASIC, SUMMARY, and COMPLETE.
    In each case, the object returned has a different set of populated fields.
    See :ref:`result_format` for more information.

    This function handles the logic for mapping those fields for column_map_expectations.
    """
    # NB: unexpected_count parameter is explicit some implementing classes may limit the length of unexpected_list
    # Incrementally add to result and return when all values for the specified level are present
    return_obj = {"success": success}

    if result_format["result_format"] == "BOOLEAN_ONLY":
        return return_obj

    skip_missing = False

    if nonnull_count is None:
        missing_count = None
        skip_missing: bool = True
    else:
        missing_count = element_count - nonnull_count

    if element_count > 0:
        unexpected_percent_total = unexpected_count / element_count * 100

        if not skip_missing:
            missing_percent = missing_count / element_count * 100
            if nonnull_count > 0:
                unexpected_percent_nonmissing = unexpected_count / nonnull_count * 100
            else:
                unexpected_percent_nonmissing = None
        else:
            unexpected_percent_nonmissing = unexpected_percent_total

    else:
        missing_percent = None
        unexpected_percent_total = None
        unexpected_percent_nonmissing = None

    return_obj["result"] = {
        "element_count": element_count,
        "unexpected_count": unexpected_count,
        "unexpected_percent": unexpected_percent_nonmissing,
        "partial_unexpected_list": unexpected_list[
            : result_format["partial_unexpected_count"]
        ],
    }

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
    partial_unexpected_counts = None
    if 0 < result_format.get("partial_unexpected_count"):
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
            return_obj["result"].update(
                {
                    "partial_unexpected_index_list": unexpected_index_list[
                        : result_format["partial_unexpected_count"]
                    ]
                    if unexpected_index_list is not None
                    else None,
                    "partial_unexpected_counts": partial_unexpected_counts,
                }
            )

    if result_format["result_format"] == "SUMMARY":
        return return_obj

    return_obj["result"].update(
        {
            "unexpected_list": unexpected_list,
            "unexpected_index_list": unexpected_index_list,
        }
    )

    if result_format["result_format"] == "COMPLETE":
        return return_obj

    raise ValueError(f"Unknown result_format {result_format['result_format']}.")
