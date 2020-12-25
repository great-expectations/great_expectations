import json

from great_expectations.expectations.registry import (
    _registered_expectations,
    _registered_metrics,
    _registered_renderers,
)

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
from great_expectations.execution_engine import (
    PandasExecutionEngine
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

def test_all_expectation_self_checks():
    library_json = {}

    for expectation_name, expectation in _registered_expectations.items():
        # try:
            report_object = expectation().self_check()
            library_json[expectation_name] = report_object
            print(report_object["metrics"])
    #     except Exception as error:
    #         print("=====", expectation_name, "=====")
    #         pass
    
def test_self_check_on_an_existing_expectation():
    expectation_name = "expect_column_values_to_match_regex"
    expectation = _registered_expectations[expectation_name]

    report_object = expectation().self_check()
    print(json.dumps(report_object, indent=2))

    report_object["description"].pop("short_description") # Don't try to exact match the docstring
    report_object["description"].pop("docstring") # Don't try to exact match the docstring

    assert report_object == {
        "description": {
            "camel_name": "ExpectColumnValuesToMatchRegex",
            "snake_name": "expect_column_values_to_match_regex",
            # "short_description": "Expect column entries to be strings that match a given regular expression.",
            # "docstring": "Expect column entries to be strings that match a given regular expression. Valid matches can be found     anywhere in the string, for example \"[at]+\" will identify the following strings as expected: \"cat\", \"hat\",     \"aa\", \"a\", and \"t\", and the following strings as unexpected: \"fish\", \"dog\".\n\n    expect_column_values_to_match_regex is a     :func:`column_map_expectation <great_expectations.execution_engine.execution_engine.MetaExecutionEngine\n    .column_map_expectation>`.\n\n    Args:\n        column (str):             The column name.\n        regex (str):             The regular expression the column entries should match.\n\n    Keyword Args:\n        mostly (None or a float between 0 and 1):             Return `\"success\": True` if at least mostly fraction of values match the expectation.             For more detail, see :ref:`mostly`.\n\n    Other Parameters:\n        result_format (str or None):             Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n            For more detail, see :ref:`result_format <result_format>`.\n        include_config (boolean):             If True, then include the expectation config as part of the result object.             For more detail, see :ref:`include_config`.\n        catch_exceptions (boolean or None):             If True, then catch exceptions and include them as part of the result object.             For more detail, see :ref:`catch_exceptions`.\n        meta (dict or None):             A JSON-serializable dictionary (nesting allowed) that will be included in the output without             modification. For more detail, see :ref:`meta`.\n\n    Returns:\n        An ExpectationSuiteValidationResult\n\n        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n    See Also:\n        :func:`expect_column_values_to_not_match_regex         <great_expectations.execution_engine.execution_engine.ExecutionEngine\n        .expect_column_values_to_not_match_regex>`\n\n        :func:`expect_column_values_to_match_regex_list         <great_expectations.execution_engine.execution_engine.ExecutionEngine\n        .expect_column_values_to_match_regex_list>`\n\n    ",
            "question": "Do at least 90.0% of values in column \"a\" match the regular expression ^a?",
            "answer": "Less than 90.0% of values in column \"a\" match the regular expression ^a.",
        },
        "execution_engines": {
            "PandasExecutionEngine": True,
            "SqlAlchemyExecutionEngine": True,
            "Spark": True
        },
        "renderers": [
            "answer",
            "renderer.diagnostic.observed_value",
            "renderer.diagnostic.status_icon",
            "renderer.diagnostic.unexpected_statement",
            "renderer.diagnostic.unexpected_table",
            "renderer.prescriptive",
            "question"
        ],
        "metrics": [
            "column_values.nonnull.unexpected_count",
            "column_values.match_regex.unexpected_count",
            "table.row_count",
            "column_values.match_regex.unexpected_values"
        ],
        "examples": [
            {
            "data": {
                "a": ["aaa","abb","acc","add","bee"],
                "b": ["aaa","abb","acc","bdd",None],
                "column_name with space": ["aaa","abb","acc","add","bee"],
            },
            "tests": [{
                "title": "negative_test_insufficient_mostly_and_one_non_matching_value",
                "exact_match_out": False,
                "in": {
                    "column": "a",
                    "regex": "^a",
                    "mostly": 0.9
                },
                "out": {
                    "success": False,
                    "unexpected_index_list": [4],
                    "unexpected_list": ["bee"]
                },
                "suppress_test_for": [
                    "sqlite",
                    "mssql"
                ]
            },
            {
                "title": "positive_test_exact_mostly_w_one_non_matching_value",
                "exact_match_out": False,
                "in": {
                    "column": "a",
                    "regex": "^a",
                    "mostly": 0.8
                },
                "out": {
                    "success": True,
                    "unexpected_index_list": [4],
                    "unexpected_list": ["bee"]
                },
                "suppress_test_for": [
                    "sqlite",
                    "mssql"
                ]}
            ]}
        ]}
