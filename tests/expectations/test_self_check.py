import json

from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial
)

from great_expectations.execution_engine import PandasExecutionEngine

from great_expectations.expectations.expectation import (
    Expectation,
    ColumnMapExpectation,
    ExpectationConfiguration,
)

class ColumnValuesEqualThree(ColumnMapMetricProvider):
    condition_metric_name = "column_values.equal_three"
    # condition_value_keys = {}
    # default_kwarg_values = {}

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column == 3  

class ExpectColumnValuesToEqualThree(ColumnMapExpectation):

    map_metric = "column_values.equal_three"
    success_keys = ("mostly",)
    # default_kwarg_values = ColumnMapExpectation.default_kwarg_values

def test_expectation_self_check():

    my_expectation = ExpectColumnValuesToEqualThree(
        configuration=ExpectationConfiguration(**{
            "expectation_type": "expect_column_values_to_equal_three",
            "kwargs": {
                "column": "threes"
            }
        })
    )
    report_object = my_expectation.self_check()
    print(json.dumps(report_object, indent=2))

    assert False