import logging
from abc import ABC
from typing import TYPE_CHECKING, Optional

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.core._docs_decorators import public_api
from great_expectations.exceptions.exceptions import (
    InvalidExpectationConfigurationError,
)
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    render_evaluation_parameter_string,
)
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.expectations.metrics.util import get_dialect_regex_expression
from great_expectations.render import LegacyRendererType, RenderedStringTemplateContent
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.renderer_configuration import (
    RendererConfiguration,
    RendererValueType,
)
from great_expectations.render.util import (
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)
from great_expectations.util import camel_to_snake

if TYPE_CHECKING:
    from great_expectations.render.renderer_configuration import AddParamArgs

logger = logging.getLogger(__name__)


@public_api
class RegexColumnMapMetricProvider(ColumnMapMetricProvider):
    """Base class for all RegexColumnMapMetrics.

    RegexColumnMapMetric classes inheriting from RegexColumnMapMetricProvider are ephemeral,
    defined by their `regex` attribute, and registered during the execution of their associated RegexColumnMapExpectation.

    Metric Registration Example:

    ```python
    map_metric = RegexBasedColumnMapExpectation.register_metric(
        regex_camel_name='Vowel',
        regex_='^[aeiouyAEIOUY]*$',
    )
    ```

    In some cases, subclasses of MetricProvider, such as RegexColumnMapMetricProvider, will already
    have correct values that may simply be inherited by Metric classes.

    Args:
        regex (str): A valid regex pattern.
        metric_name (str): The name of the registered metric. Must be globally unique in a great_expectations installation.
            Constructed by the `register_metric(...)` function during Expectation execution.
        domain_keys (tuple): A tuple of the keys used to determine the domain of the metric.
        condition_value_keys (tuple): A tuple of the keys used to determine the value of the metric.

    ---Documentation---
        - https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_regex_based_column_map_expectations
    """

    condition_value_keys = ()

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column.astype(str).str.contains(cls.regex)

    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, _dialect, **kwargs):
        regex_expression = get_dialect_regex_expression(column, cls.regex, _dialect)

        if regex_expression is None:
            logger.warning(
                f"Regex is not supported for dialect {str(_dialect.dialect.name)}"
            )
            raise NotImplementedError

        return regex_expression

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        return column.rlike(cls.regex)


@public_api
class RegexBasedColumnMapExpectation(ColumnMapExpectation, ABC):
    """Base class for RegexBasedColumnMapExpectations.

    RegexBasedColumnMapExpectations facilitate regex parsing as the core logic for a Map Expectation.

    Example Definition:

    ```python
    ExpectColumnValuesToOnlyContainVowels(SetBasedColumnMapExpectation):
        regex_camel_name = 'Vowel'
        regex = '^[aeiouyAEIOUY]*$'
        semantic_type_name_plural = 'vowels'
        map_metric = RegexBasedColumnMapExpectation.register_metric(
            regex_camel_name=regex_camel_name,
            regex=regex
    )
    ```

    Args:
        regex_camel_name (str): A name describing a regex pattern, in camel case.
        regex_ (str): A valid regex pattern.
        semantic_type_name_plural (optional[str]): The plural form of a semantic type being validated by a regex pattern.
        map_metric (str): The name of an ephemeral metric, as returned by `register_metric(...)`.
    """

    @staticmethod
    def register_metric(
        regex_camel_name: str,
        regex_: str,
    ) -> str:
        """Register an ephemeral metric using a constructed name with the logic provided by RegexColumnMapMetricProvider.

        Args:
            regex_camel_name: A name describing a regex pattern, in camel case.
            regex_: A valid regex pattern.

        Returns:
            map_metric: The constructed name of the ephemeral metric.
        """
        regex_snake_name: str = camel_to_snake(regex_camel_name)
        map_metric: str = "column_values.match_" + regex_snake_name + "_regex"

        # Define the class using `type`. This allows us to name it dynamically.
        new_column_regex_metric_provider = type(  # noqa: F841 # never used
            f"(ColumnValuesMatch{regex_camel_name}Regex",
            (RegexColumnMapMetricProvider,),
            {
                "condition_metric_name": map_metric,
                "regex": regex_,
            },
        )

        return map_metric

    @public_api
    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        """Raise an exception if the configuration is not viable for an expectation.

        Args:
            configuration: An ExpectationConfiguration

        Raises:
            InvalidExpectationConfigurationError: If no `regex` or `column` specified, or if `mostly` parameter
                incorrectly defined.
        """
        super().validate_configuration(configuration)
        try:
            assert (
                getattr(self, "regex", None) is not None
            ), "regex is required for RegexBasedColumnMap Expectations"
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

    # question, descriptive, prescriptive, diagnostic
    @classmethod
    @renderer(renderer_type=LegacyRendererType.QUESTION)
    def _question_renderer(cls, configuration, result=None, runtime_configuration=None):
        column = configuration.kwargs.get("column")
        mostly = configuration.kwargs.get("mostly")
        regex = getattr(cls, "regex")
        semantic_type_name_plural = getattr(cls, "semantic_type_name_plural", None)

        if mostly == 1 or mostly is None:
            if semantic_type_name_plural is not None:
                return f'Are all values in column "{column}" valid {semantic_type_name_plural}, as judged by matching the regular expression {regex}?'
            else:
                return f'Do all values in column "{column}" match the regular expression {regex}?'
        else:
            if semantic_type_name_plural is not None:  # noqa: PLR5501
                return f'Are at least {mostly * 100}% of values in column "{column}" valid {semantic_type_name_plural}, as judged by matching the regular expression {regex}?'
            else:
                return f'Do at least {mostly * 100}% of values in column "{column}" match the regular expression {regex}?'

    @classmethod
    @renderer(renderer_type=LegacyRendererType.ANSWER)
    def _answer_renderer(
        cls, configuration=None, result=None, runtime_configuration=None
    ):
        column = result.expectation_config.kwargs.get("column")
        mostly = result.expectation_config.kwargs.get("mostly")
        regex = result.expectation_config.kwargs.get("regex")
        semantic_type_name_plural = configuration.kwargs.get(
            "semantic_type_name_plural"
        )

        if result.success:
            if mostly == 1 or mostly is None:
                if semantic_type_name_plural is not None:
                    return f'All values in column "{column}" are valid {semantic_type_name_plural}, as judged by matching the regular expression {regex}.'
                else:
                    return f'All values in column "{column}" match the regular expression {regex}.'
            else:
                if semantic_type_name_plural is not None:  # noqa: PLR5501
                    return f'At least {mostly * 100}% of values in column "{column}" are valid {semantic_type_name_plural}, as judged by matching the regular expression {regex}.'
                else:
                    return f'At least {mostly * 100}% of values in column "{column}" match the regular expression {regex}.'
        else:
            if semantic_type_name_plural is not None:  # noqa: PLR5501
                return f' Less than {mostly * 100}% of values in column "{column}" are valid {semantic_type_name_plural}, as judged by matching the regular expression {regex}.'
            else:
                return f'Less than {mostly * 100}% of values in column "{column}" match the regular expression {regex}.'

    @classmethod
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ):
        add_param_args: AddParamArgs = (
            ("column", RendererValueType.STRING),
            ("mostly", RendererValueType.NUMBER),
            ("regex", RendererValueType.STRING),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        params = renderer_configuration.params

        if not params.regex:
            template_str = (
                "values must match a regular expression but none was specified."
            )
        else:
            template_str = "values must match this regular expression: $regex"

            if params.mostly and params.mostly.value < 1.0:  # noqa: PLR2004
                renderer_configuration = cls._add_mostly_pct_param(
                    renderer_configuration=renderer_configuration
                )
                template_str += ", at least $mostly_pct % of the time."
            else:
                template_str += "."

        if renderer_configuration.include_column_name:
            template_str = "$column " + template_str

        renderer_configuration.template_str = template_str

        return renderer_configuration

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = (
            False if runtime_configuration.get("include_column_name") is False else True
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

        params_with_json_schema = {  # noqa: F841 # never used
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
