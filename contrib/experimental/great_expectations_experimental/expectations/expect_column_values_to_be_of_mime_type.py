import json
import re
from typing import Optional

import magic

#!!! This giant block of imports should be something simpler, such as:
# from great_exepectations.helpers.expectation_creation import *
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    Expectation,
    ExpectationConfiguration,
)
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
    column_function_partial,
)
from great_expectations.expectations.registry import (
    _registered_expectations,
    _registered_metrics,
    _registered_renderers,
)
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import num_to_str, substitute_none_for_missing
from great_expectations.validator.validation_graph import MetricConfiguration
from great_expectations.validator.validator import Validator


class ColumnValuesToBeOfMimeType(ColumnMapMetricProvider):
    """
    MetricProvider tests whether binary data's mime type matches the provided string.
    Uses the python-magic package which is an interface to libmagic.
    """

    condition_metric_name = "column_values.mime_type.is_type"
    condition_value_keys = ("mime_type",)

    default_kwargs_values = {"mime_type": None}

    function_metric_name = "column_values.mime_type"
    function_value_keys = tuple()

    @column_function_partial(engine=PandasExecutionEngine)
    def _pandas_function(self, column, **kwargs):
        try:
            return column.apply(lambda x: magic.from_buffer(x, mime=True))
        except TypeError:
            raise (
                TypeError(
                    "Cannot complete Z-score calculations on a non-numerical column."
                )
            )

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas_condition(cls, column, _metrics, mime_type, **kwargs):
        types, _, _ = _metrics["column_values.mime_type.map"]
        return types == mime_type

    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        """Returns a dictionary of given metric names and their corresponding configuration, specifying the metric
        types and their respective domains"""
        dependencies: dict = super()._get_evaluation_dependencies(
            metric=metric,
            configuration=configuration,
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )

        if metric.metric_name == "column_values.mime_type.is_type.condition":
            dependencies["column_values.mime_type.map"] = MetricConfiguration(
                metric_name="column_values.mime_type.map",
                metric_domain_kwargs=metric.metric_domain_kwargs,
            )

        return dependencies


class ExpectColumnValuesToBeOfMimeType(ColumnMapExpectation):
    """Expect column entries to be binary values encoding a file of a given mime type,\
        which is represented as as string.
    expect_column_values_to_be_of_mime_type is a \
    :func:`column_map_expectation <great_expectations.execution_engine.execution_engine.MetaExecutionEngine
    .column_map_expectation>`.
    Args:
       mime_type (str): expected mime type 
    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Return `"success": True` if at least mostly fraction of values match the expectation. \
            For more detail, see :ref:`mostly`.
    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see :ref:`meta`.
    Returns:
        An ExpectationSuiteValidationResult
        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.
    """

    library_metadata = {
        "maturity": "experimental",
        "tags": ["experimental", "column map expectation", "binary"],
        "package": "experimental_expectations",
        "contributors": ["@zdata-inc", "@wilbry"],
        "requirements": ["python-magic"],
    }

    map_metric = "column_values.mime_type.is_type"
    success_keys = ("mostly", "mime_type")

    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,
        "mime_type": None,
        "mostly": 1,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }

    examples = [
        {
            "data": {
                "all_png": [  # One pixel PNGs
                    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x00\x00\x00\x00:~\x9bU\x00\x00\x00\nIDATx\x9cc\xd0\x00\x00\x00*\x00)k1Ts\x00\x00\x00\x00IEND\xaeB`\x82",
                    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x03\x08\x02\x00\x00\x00\xdd\xbf\xf2\xd5\x00\x00\x00\rIDATx\x9cc\xd0`\x80\x01k\x00\x01\xff\x00d\xda\x15Wd\x00\x00\x00\x00IEND\xaeB`\x82",
                    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x03\x08\x02\x00\x00\x00\xdd\xbf\xf2\xd5\x00\x00\x00\rIDATx\x9cc`a\x80\x01V\x00\x00=\x00\n{\xe1\xb5y\x00\x00\x00\x00IEND\xaeB`\x82",
                    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x03\x08\x02\x00\x00\x00\xdd\xbf\xf2\xd5\x00\x00\x00\rIDATx\x9cc`a\x80\x01V\x00\x00=\x00\n{\xe1\xb5y\x00\x00\x00\x00IEND\xaeB`\x82",
                ],
                "all_flac": [  # Mono
                    b'fLaC\x00\x00\x00"\x10\x00\x10\x00\x00\x00(\x00\x00(\n\xc4Ap\x00\x00\x00\n\x84?\xae\x81\xf8\xf4\xbd\xe6\x01\xe5n\xbf\x1a\x8b\xac\x13\x84\x00\x00( \x00\x00\x00reference libFLAC 1.3.3 20190804\x00\x00\x00\x00\xff\xf8i\x0c\x00\t\x89\x02(\x12t\xec\xac\x1c\x93L6`[\xd9\x11n=\xd3\x0c\x9b\x7f\xff\xff\x18\xd9\xa9P\x87*\x80\x00\x00lr',
                    b'fLaC\x00\x00\x00"\x10\x00\x10\x00\x00\x00\x1e\x00\x00\x1e\x05b \xf0\x00\x00\x00\n\x1f/P\xd0\x01\xb2\x9e=\xe56\x88~\xe3\x0cs\xbe\x84\x00\x00( \x00\x00\x00reference libFLAC 1.3.3 20190804\x00\x00\x00\x00\xff\xf8f\x08\x00\t\xf0\x02\xeb\xe0\xa9GJ\xa8V6\xef4\xb5\x19\xea\x83\x7f\xff\xb5\x7f\x7f\xff\xc0\x17',
                    # Stereo
                    b'fLaC\x00\x00\x00"\x10\x00\x10\x00\x00\x00\x1f\x00\x00\x1f\x05b"\xf0\x00\x00\x00\x05\xd5|Z\xf9\x7f\xc7\xe1\x91\x88\xad\xa2:w}\x17\xd0\x84\x00\x00( \x00\x00\x00reference libFLAC 1.3.3 20190804\x00\x00\x00\x00\xff\xf8f\x18\x00\x04q\x02\xc5k\xa4\xfeT\xd8\xd4<\xe7\xd6\x02\x7fB\x0c\x7fd\xc5:WJ\xe6\xea\xb1',
                    b'fLaC\x00\x00\x00"\x10\x00\x10\x00\x00\x00\x1f\x00\x00\x1f\x05b"\xf0\x00\x00\x00\x056\x17\x17\xa2AN\xb0\x05Y\xa3\xf0\xb5,a\xde\xa7\x84\x00\x00( \x00\x00\x00reference libFLAC 1.3.3 20190804\x00\x00\x00\x00\xff\xf8f\x18\x00\x04q\x02\xfb\xe6\xaef\xfe\xf0|\xcaD\x1b\x02"C\xc5\xe8\x7f\xff\x80\x00i\x7f\xa2U',
                ],
                "mostly_png": [  # One pixel PNGs
                    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x00\x00\x00\x00:~\x9bU\x00\x00\x00\nIDATx\x9cc\xd0\x00\x00\x00*\x00)k1Ts\x00\x00\x00\x00IEND\xaeB`\x82",
                    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x03\x08\x02\x00\x00\x00\xdd\xbf\xf2\xd5\x00\x00\x00\rIDATx\x9cc\xd0`\x80\x01k\x00\x01\xff\x00d\xda\x15Wd\x00\x00\x00\x00IEND\xaeB`\x82",
                    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x03\x08\x02\x00\x00\x00\xdd\xbf\xf2\xd5\x00\x00\x00\rIDATx\x9cc`a\x80\x01V\x00\x00=\x00\n{\xe1\xb5y\x00\x00\x00\x00IEND\xaeB`\x82",
                    # Mono Sound
                    b'fLaC\x00\x00\x00"\x10\x00\x10\x00\x00\x00\x1e\x00\x00\x1e\x05b \xf0\x00\x00\x00\n\x1f/P\xd0\x01\xb2\x9e=\xe56\x88~\xe3\x0cs\xbe\x84\x00\x00( \x00\x00\x00reference libFLAC 1.3.3 20190804\x00\x00\x00\x00\xff\xf8f\x08\x00\t\xf0\x02\xeb\xe0\xa9GJ\xa8V6\xef4\xb5\x19\xea\x83\x7f\xff\xb5\x7f\x7f\xff\xc0\x17',
                ],
            },
            "tests": [
                {
                    "title": "pass_all_png",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {"column": "all_png", "mime_type": "image/png", "mostly": 1},
                    "out": {"success": True},
                },
                {
                    "title": "fail_all_png",
                    "include_in_gallery": True,
                    "exact_match_out": True,
                    "in": {"column": "all_png", "mime_type": "image/jpeg", "mostly": 1},
                    "out": {"success": False},
                },
                {
                    "title": "pass_all_flac",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {
                        "column": "all_flac",
                        "mime_type": "audio/flac",
                        "mostly": 1,
                    },
                    "out": {"success": True},
                },
                {
                    "title": "fail_all_flac",
                    "include_in_gallery": True,
                    "exact_match_out": True,
                    "in": {"column": "all_flac", "mime_type": "audio/mp3", "mostly": 1},
                    "out": {"success": False},
                },
                {
                    "title": "pass_mostly_png",
                    "include_in_gallery": True,
                    "exact_match_out": True,
                    "in": {
                        "column": "mostly_png",
                        "mime_type": "image/png",
                        "mostly": 0.5,
                    },
                    "out": {"success": True},
                },
                {
                    "title": "fail_mostly_png",
                    "include_in_gallery": True,
                    "exact_match_out": True,
                    "in": {
                        "column": "mostly_png",
                        "mime_type": "image/png",
                        "mostly": 0.8,
                    },
                    "out": {"success": False},
                },
            ],
        }
    ]

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _diagnostic_unexpected_statement_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        include_column_name = runtime_configuration.get("include_column_name", True)
        include_column_name = (
            include_column_name if include_column_name is not None else True
        )
        styling = runtime_configuration.get("styling")
        # get params dict with all expected kwargs
        params = substitute_none_for_missing(
            configuration.kwargs,
            [
                "column",
                "mime_type",
                "mostly",
                "row_condition",
                "condition_parser",
                "strict_min",
                "strict_max",
            ],
        )

        if params["mostly"] is not None:
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
            # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
            template_str = (
                "values must be of type $mime_type, at least $mostly_pct % of the time."
            )
        else:
            template_str = "values must be of type $mime_type."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]


if __name__ == "__main__":
    diagnostics_report = ExpectColumnValuesToBeOfMimeType().run_diagnostics()
    print(diagnostics_report)
