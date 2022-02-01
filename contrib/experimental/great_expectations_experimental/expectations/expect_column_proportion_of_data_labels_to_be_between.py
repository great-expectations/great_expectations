import json 
import dataprofiler as dp


from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
)

from great_expectations.expectations.expectation import (
    ColumnExpectation,
    Expectation,
    ExpectationConfiguration,
    InvalidExpectationConfigurationError,
    _format_map_output,
)
from great_expectations.expectations.metrics.column_aggregate_metric import (
    ColumnMetricProvider,
    column_aggregate_value,
)


class ColumnValuesProportionOfDataLabelsToBeBetween(ColumnMetricProvider):
    """MetricProvider Class for Data Label Probability Matching Threshold"""
    pass


class ExpectColumnProportionOfDataLabelsToBeBetween(ColumnExpectation):
    """
    This function 

    :param: column,  
    :param: data_label,
    :param: min,
    :param: max,
    :param: trained_model

    df.expect_column_proportion_of_data_labels_to_be_between(
        column,
        data_label=<>,
        min,
        max,
        trained_model
    )
    """
    # This method defines the business logic for evaluating your metric when using a PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, language, **kwargs):
        def identify_language(text):
            try:
                language, confidence = langid.classify(text)
            except Exception:
                language, confidence = None, None
            return {
                "label": language,
                "confidence": confidence,
            }

        labels = column.apply(lambda x: identify_language(x)["label"])
        return labels == language

