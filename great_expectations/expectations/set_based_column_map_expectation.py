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
class SetColumnMapMetricProvider(ColumnMapMetricProvider):
    """Base class for all SetColumnMapMetrics.

    SetColumnMapMetric classes inheriting from SetColumnMapMetricProvider are ephemeral,
    defined by their `set` attribute, and registered during the execution of their associated SetColumnMapExpectation.

    Metric Registration Example:

    ```python
    map_metric = SetBasedColumnMapExpectation.register_metric(
        set_camel_name='SolfegeScale',
        set_=['do', 're', 'mi', 'fa', 'so', 'la', 'ti'],
    )
    ```

    In some cases, subclasses of MetricProvider, such as SetColumnMapMetricProvider, will already
    have correct values that may simply be inherited by Metric classes.

    Args:
        set_ (union[list, set]): A value set.
        metric_name (str): The name of the registered metric. Must be globally unique in a great_expectations installation.
            Constructed by the `register_metric(...)` function during Expectation execution.
        domain_keys (tuple): A tuple of the keys used to determine the domain of the metric.
        condition_value_keys (tuple): A tuple of the keys used to determine the value of the metric.
    """

    condition_value_keys = ()

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column.isin(cls.set_)

    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, _dialect, **kwargs):
        return column.in_(cls.set_)

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        return column.isin(cls.set_)


@public_api
class SetBasedColumnMapExpectation(ColumnMapExpectation, ABC):
    """Base class for SetBasedColumnMapExpectations.

    SetBasedColumnMapExpectations facilitate set-based comparisons as the core logic for a Map Expectation.

    Example Definition:

    ```python
    ExpectColumnValuesToBeInSolfegeScaleSet(SetBasedColumnMapExpectation):
        set_camel_name = SolfegeScale
        set_ = ['do', 're', 'mi', 'fa', 'so', 'la', 'ti']
        set_semantic_name = "the Solfege scale"
        map_metric = SetBasedColumnMapExpectation.register_metric(
            set_camel_name=set_camel_name,
            set_=set_
    )
    ```

    Args:
        set_camel_name (str): A name describing a set of values, in camel case.
        set_ (str): A value set.
        set_semantic_name (optional[str]): A name for the semantic type representing the set being validated..
        map_metric (str): The name of an ephemeral metric, as returned by `register_metric(...)`.

    ---Documentation---
        - https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_set_based_column_map_expectations
    """

    @staticmethod
    def register_metric(
        set_camel_name: str,
        set_: str,
    ) -> str:
        """Register an ephemeral metric using a constructed name with the logic provided by SetColumnMapMetricProvider.

        Args:
            set_camel_name: A name describing a set of values, in camel case.
            set_: A value set.

        Returns:
            map_metric: The constructed name of the ephemeral metric.
        """
        set_snake_name = camel_to_snake(set_camel_name)
        map_metric: str = "column_values.match_" + set_snake_name + "_set"

        # Define the class using `type`. This allows us to name it dynamically.
        new_column_set_metric_provider = type(  # noqa: F841 # never used
            f"(ColumnValuesMatch{set_camel_name}Set",
            (SetColumnMapMetricProvider,),
            {
                "condition_metric_name": map_metric,
                "set_": set_,
            },
        )

        return map_metric

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        """Raise an exception if the configuration is not viable for an expectation.

        Args:
            configuration: An ExpectationConfiguration

        Raises:
            InvalidExpectationConfigurationError: If no `set_` or `column` specified, or if `mostly` parameter
                incorrectly defined.
        """
        super().validate_configuration(configuration)
        try:
            assert (
                getattr(self, "set_", None) is not None
            ), "set_ is required for SetBasedColumnMap Expectations"

            assert (
                "column" in configuration.kwargs
            ), "'column' parameter is required for ColumnMap expectations"

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
        set_ = getattr(cls, "set_")
        set_semantic_name = getattr(cls, "set_semantic_name", None)

        if mostly == 1 or mostly is None:
            if set_semantic_name is not None:
                return f'Are all values in column "{column}" in {set_semantic_name}: {str(set_)}?'
            else:
                return f'Are all values in column "{column}" in the set {str(set_)}?'
        else:
            if set_semantic_name is not None:  # noqa: PLR5501
                return f'Are at least {mostly * 100}% of values in column "{column}" in {set_semantic_name}: {str(set_)}?'
            else:
                return f'Are at least {mostly * 100}% of values in column "{column}" in the set {str(set_)}?'

    @classmethod
    @renderer(renderer_type=LegacyRendererType.ANSWER)
    def _answer_renderer(
        cls, configuration=None, result=None, runtime_configuration=None
    ):
        column = result.expectation_config.kwargs.get("column")
        mostly = result.expectation_config.kwargs.get("mostly")
        set_ = getattr(cls, "set_")
        set_semantic_name = getattr(cls, "set_semantic_name", None)

        if result.success:
            if mostly == 1 or mostly is None:
                if set_semantic_name is not None:
                    return f'All values in column "{column}" are in {set_semantic_name}: {str(set_)}.'
                else:
                    return (
                        f'All values in column "{column}" are in the set {str(set_)}.'
                    )
            else:
                if set_semantic_name is not None:  # noqa: PLR5501
                    return f'At least {mostly * 100}% of values in column "{column}" are in {set_semantic_name}: {str(set_)}.'
                else:
                    return f'At least {mostly * 100}% of values in column "{column}" are in the set {str(set)}.'
        else:
            if set_semantic_name is not None:  # noqa: PLR5501
                return f' Less than {mostly * 100}% of values in column "{column}" are in {set_semantic_name}: {str(set_)}.'
            else:
                return f'Less than {mostly * 100}% of values in column "{column}" are in the set {str(set_)}.'

    @classmethod
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        add_param_args: AddParamArgs = (
            ("column", RendererValueType.STRING),
            ("mostly", RendererValueType.NUMBER),
            ("set_", RendererValueType.STRING),
            ("set_semantic_name", RendererValueType.STRING),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        params = renderer_configuration.params

        if not params.set_:
            template_str = "values must match a set but none was specified."
        else:
            if params.set_semantic_name:
                template_str = "values must match the set $set_semantic_name: $set_"
            else:
                template_str = "values must match this set: $set_"

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
            [
                "column",
                "set_",
                "mostly",
                "row_condition",
                "condition_parser",
                "set_semantic_name",
            ],
        )

        if not params.get("set_"):
            template_str = "values must match a set but none was specified."
        else:
            if params.get("set_semantic_name"):
                template_str = "values must match the set $set_semantic_name: $set_"
            else:
                template_str = "values must match this set: $set_"
            if params["mostly"] is not None:
                params["mostly_pct"] = num_to_str(
                    params["mostly"] * 100, precision=15, no_scientific=True
                )
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
            "set_": {"schema": {"type": "string"}, "value": params.get("set_")},
            "row_condition": {
                "schema": {"type": "string"},
                "value": params.get("row_condition"),
            },
            "condition_parser": {
                "schema": {"type": "string"},
                "value": params.get("condition_parser"),
            },
            "set_semantic_name": {
                "schema": {"type": "string"},
                "value": params.get("set_semantic_name"),
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
