from abc import ABC
import json

from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    ExpectationConfiguration,
)
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.expectations.metrics.util import get_dialect_regex_expression
from great_expectations.render.renderer.renderer import (
    renderer
)
from great_expectations.render.util import (
    handle_strict_min_max,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.expectations.util import (
    render_evaluation_parameter_string,
)

class ColumnMapRegexMetricProvider(ColumnMapMetricProvider):
    condition_value_keys = ()

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column.astype(str).str.contains(cls.regex)

    # @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     regex_expression = get_dialect_regex_expression(column, cls.regex, _dialect)
    #     if regex_expression is None:
    #         logger.warning(
    #             "Regex is not supported for dialect %s" % str(_dialect.dialect.name)
    #         )
    #         raise NotImplementedError

    #     return regex_expression

    # @column_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column, **kwargs):
    #     return column.rlike(cls.regex)

class ColumnMapRegexExpectation(ColumnMapExpectation, ABC):

    @staticmethod
    def _register_metric(
        regex_snake_name : str,
        regex_camel_name : str,
        regex_ : str,
    ):
        map_metric = "column_values.match_"+regex_snake_name+"_regex"

        # Define the class using `type`. This allows us to name it dynamically.
        NewColumnRegexMetricProvider = type(
            f"(ColumnValuesMatch{regex_camel_name}Regex",
            (ColumnMapRegexMetricProvider,),
            {
                "condition_metric_name": map_metric,
                "regex": regex_,
            }
        )

        return map_metric

    # def validate_configuration(
    #     self,
    #     configuration: ExpectationConfiguration
    # ) -> bool :
    #     pass
    #     if not super().validate_configuration(configuration):
    #         return False
    #     try:
    #         assert (
    #             "column" in configuration.kwargs
    #         ), "'column' parameter is required for column map expectations"
    #         if "mostly" in configuration.kwargs:
    #             mostly = configuration.kwargs["mostly"]
    #             assert isinstance(
    #                 mostly, (int, float)
    #             ), "'mostly' parameter must be an integer or float"
    #             assert 0 <= mostly <= 1, "'mostly' parameter must be between 0 and 1"
    #     except AssertionError as e:
    #         raise InvalidExpectationConfigurationError(str(e))
    #     return True



    # question, descriptive, prescriptive, diagnostic
    @classmethod
    @renderer(renderer_type="renderer.question")
    def _question_renderer(
        cls, configuration, result=None, language=None, runtime_configuration=None
    ):
        column = configuration.kwargs.get("column")
        mostly = configuration.kwargs.get("mostly")
        regex = configuration.kwargs.get("regex")

        return f'Do at least {mostly * 100}% of values in column "{column}" match the regular expression {regex}?'

#     @classmethod
#     @renderer(renderer_type="renderer.answer")
#     def _answer_renderer(
#         cls, configuration=None, result=None, language=None, runtime_configuration=None
#     ):
#         column = result.expectation_config.kwargs.get("column")
#         mostly = result.expectation_config.kwargs.get("mostly")
#         regex = result.expectation_config.kwargs.get("regex")
#         if result.success:
#             return f'At least {mostly * 100}% of values in column "{column}" match the regular expression {regex}.'
#         else:
#             return f'Less than {mostly * 100}% of values in column "{column}" match the regular expression {regex}.'

#     @classmethod
#     def _atomic_prescriptive_template(
#         cls,
#         configuration=None,
#         result=None,
#         language=None,
#         runtime_configuration=None,
#         **kwargs,
#     ):
#         runtime_configuration = runtime_configuration or {}
#         include_column_name = runtime_configuration.get("include_column_name", True)
#         include_column_name = (
#             include_column_name if include_column_name is not None else True
#         )
#         styling = runtime_configuration.get("styling")
#         params = substitute_none_for_missing(
#             configuration.kwargs,
#             ["column", "regex", "mostly", "row_condition", "condition_parser"],
#         )
#         params_with_json_schema = {
#             "column": {"schema": {"type": "string"}, "value": params.get("column")},
#             "mostly": {"schema": {"type": "number"}, "value": params.get("mostly")},
#             "mostly_pct": {
#                 "schema": {"type": "number"},
#                 "value": params.get("mostly_pct"),
#             },
#             "regex": {"schema": {"type": "string"}, "value": params.get("regex")},
#             "row_condition": {
#                 "schema": {"type": "string"},
#                 "value": params.get("row_condition"),
#             },
#             "condition_parser": {
#                 "schema": {"type": "string"},
#                 "value": params.get("condition_parser"),
#             },
#         }

#         if not params.get("regex"):
#             template_str = (
#                 "values must match a regular expression but none was specified."
#             )
#         else:
#             template_str = "values must match this regular expression: $regex"
#             if params["mostly"] is not None:
#                 params_with_json_schema["mostly_pct"]["value"] = num_to_str(
#                     params["mostly"] * 100, precision=15, no_scientific=True
#                 )
#                 # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
#                 template_str += ", at least $mostly_pct % of the time."
#             else:
#                 template_str += "."

#         if include_column_name:
#             template_str = "$column " + template_str

#         if params["row_condition"] is not None:
#             (
#                 conditional_template_str,
#                 conditional_params,
#             ) = parse_row_condition_string_pandas_engine(
#                 params["row_condition"], with_schema=True
#             )
#             template_str = conditional_template_str + ", then " + template_str
#             params_with_json_schema.update(conditional_params)

#         return (template_str, params_with_json_schema, styling)

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        include_column_name = (
            include_column_name if include_column_name is not None else True
        )
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs,
            ["column", "regex", "mostly", "row_condition", "condition_parser"],
        )

        if not params.get("regex"):
            template_str = (
                "values must match a regular expression but none was specified."
            )
        else:
            template_str = "values must match this regular expression: $regex"
            if params["mostly"] is not None:
                params["mostly_pct"] = num_to_str(
                    params["mostly"] * 100, precision=15, no_scientific=True
                )
                # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
                template_str += ", at least $mostly_pct % of the time."
            else:
                template_str += "."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        params_with_json_schema = {
            "column": {"schema": {"type": "string"}, "value": params.get("column")},
            "mostly": {"schema": {"type": "number"}, "value": params.get("mostly")},
            "mostly_pct": {
                "schema": {"type": "number"},
                "value": params.get("mostly_pct"),
            },
            "regex": {"schema": {"type": "string"}, "value": params.get("regex")},
            "row_condition": {
                "schema": {"type": "string"},
                "value": params.get("row_condition"),
            },
            "condition_parser": {
                "schema": {"type": "string"},
                "value": params.get("condition_parser"),
            },
        }

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