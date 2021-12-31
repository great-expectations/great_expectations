import pytest

from great_expectations.core.expectation_diagnostic_report import (
    ExpectationDiagnosticReportDescription,
    LibraryMetadata,
    RendererDiagnostics,
    TestData,
    ExpectationTestCase,
    ExpectationTestDataCases,
    ExpectationDiagnosticReport,
)

def test_basic_instantiation_of_ExpectatationDiagnosticReportDescription():
    ExpectationDiagnosticReportDescription(**{
        "camel_name": "ExpectColumnValuesToMatchRegex",
        "snake_name": "expect_column_values_to_match_regex",
        "short_description": "Expect column entries to be strings that match a given regular expression.",
        "docstring": "Expect column entries to be strings that match a given regular expression.\n    \n    Valid matches can be found     anywhere in the string, for example \"[at]+\" will identify the following strings as expected: \"cat\", \"hat\",     \"aa\", \"a\", and \"t\", and the following strings as unexpected: \"fish\", \"dog\".\n\n    expect_column_values_to_match_regex is a     :func:`column_map_expectation <great_expectations.execution_engine.execution_engine.MetaExecutionEngine\n    .column_map_expectation>`.\n\n    Args:\n        column (str):             The column name.\n        regex (str):             The regular expression the column entries should match.\n\n    Keyword Args:\n        mostly (None or a float between 0 and 1):             Return `\"success\": True` if at least mostly fraction of values match the expectation.             For more detail, see :ref:`mostly`.\n\n    Other Parameters:\n        result_format (str or None):             Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n            For more detail, see :ref:`result_format <result_format>`.\n        include_config (boolean):             If True, then include the expectation config as part of the result object.             For more detail, see :ref:`include_config`.\n        catch_exceptions (boolean or None):             If True, then catch exceptions and include them as part of the result object.             For more detail, see :ref:`catch_exceptions`.\n        meta (dict or None):             A JSON-serializable dictionary (nesting allowed) that will be included in the output without             modification. For more detail, see :ref:`meta`.\n\n    Returns:\n        An ExpectationSuiteValidationResult\n\n        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n    See Also:\n        :func:`expect_column_values_to_not_match_regex         <great_expectations.execution_engine.execution_engine.ExecutionEngine\n        .expect_column_values_to_not_match_regex>`\n\n        :func:`expect_column_values_to_match_regex_list         <great_expectations.execution_engine.execution_engine.ExecutionEngine\n        .expect_column_values_to_match_regex_list>`\n\n    "
    })

@pytest.mark.skip(reason="Not yet supported")
def test_basic_instantiation_of_TestData():
    TestData(**{
      "a": [
        "aaa",
        "abb",
        "acc",
        "add",
        "bee"
      ],
      "b": [
        "aaa",
        "abb",
        "acc",
        "bdd",
        None
      ],
      "column_name with space": [
        "aaa",
        "abb",
        "acc",
        "add",
        "bee"
      ]
    })

@pytest.mark.skip(reason="Not yet supported")
def test_basic_instantiation_of_ExpectationTestCase():
    ExpectationTestCase(**{
        "title": "basic_negative_test",
        "exact_match_out": False,
        "in": {
            "column": "a",
            "regex": "^a",
            "mostly": 0.9
        },
        "out": {
            "success": False,
            "unexpected_index_list": [
                4
            ],
            "unexpected_list": [
                "bee"
            ]
        },
        "suppress_test_for": ["sqlite", "mssql"]
    })


def test_LibraryMetdata():
    {
      "maturity": "beta",
      "package": "my_package",
      "tags": ["some_tag", "other_tag"],
      "contributors": [
        "@shinnyshinshin",
        "@abegong"
      ]
    }

def test_ExpectationDiagnosticReport():
    expectation_test_data_case = ExpectationTestDataCases(
        data = TestData(**{
            "a": [ "aaa", "abb", "acc", "add", "bee"],
            "b": [ "aaa", "abb", "acc", "bdd", None],
            "column_name with space": [ "aaa", "abb", "acc", "add", "bee"],
        }),
        tests = [
            ExpectationTestCase(**{
                "title": "negative_test_insufficient_mostly_and_one_non_matching_value",
                "exact_match_out": False,
                "in": {
                    "column": "a",
                    "regex": "^a",
                    "mostly": 0.9
                },
                "out": {
                    "success": False,
                    "unexpected_index_list": [
                        4
                    ],
                    "unexpected_list": [
                        "bee"
                    ]
                },
                "suppress_test_for": [
                    "sqlite",
                    "mssql"
                ]
            }),
            ExpectationTestCase(**{
                "title": "positive_test_exact_mostly_w_one_non_matching_value",
                "exact_match_out": False,
                "in": {
                    "column": "a",
                    "regex": "^a",
                    "mostly": 0.8
                },
                "out": {
                    "success": True,
                    "unexpected_index_list": [
                        4
                    ],
                    "unexpected_list": [
                        "bee"
                    ]
                },
                "suppress_test_for": [
                    "sqlite",
                    "mssql"
                ]
            })
        ]
    )

    edr = ExpectationDiagnosticReport(
        description = ExpectationDiagnosticReportDescription(**{
            "camel_name": "ExpectColumnValuesToMatchRegex",
            "snake_name": "expect_column_values_to_match_regex",
            "short_description": "Expect column entries to be strings that match a given regular expression.",
            "docstring": "Expect column entries to be strings that match a given regular expression.\n    \n    Valid matches can be found     anywhere in the string, for example \"[at]+\" will identify the following strings as expected: \"cat\", \"hat\",     \"aa\", \"a\", and \"t\", and the following strings as unexpected: \"fish\", \"dog\".\n\n    expect_column_values_to_match_regex is a     :func:`column_map_expectation <great_expectations.execution_engine.execution_engine.MetaExecutionEngine\n    .column_map_expectation>`.\n\n    Args:\n        column (str):             The column name.\n        regex (str):             The regular expression the column entries should match.\n\n    Keyword Args:\n        mostly (None or a float between 0 and 1):             Return `\"success\": True` if at least mostly fraction of values match the expectation.             For more detail, see :ref:`mostly`.\n\n    Other Parameters:\n        result_format (str or None):             Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n            For more detail, see :ref:`result_format <result_format>`.\n        include_config (boolean):             If True, then include the expectation config as part of the result object.             For more detail, see :ref:`include_config`.\n        catch_exceptions (boolean or None):             If True, then catch exceptions and include them as part of the result object.             For more detail, see :ref:`catch_exceptions`.\n        meta (dict or None):             A JSON-serializable dictionary (nesting allowed) that will be included in the output without             modification. For more detail, see :ref:`meta`.\n\n    Returns:\n        An ExpectationSuiteValidationResult\n\n        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n    See Also:\n        :func:`expect_column_values_to_not_match_regex         <great_expectations.execution_engine.execution_engine.ExecutionEngine\n        .expect_column_values_to_not_match_regex>`\n\n        :func:`expect_column_values_to_match_regex_list         <great_expectations.execution_engine.execution_engine.ExecutionEngine\n        .expect_column_values_to_match_regex_list>`\n\n    "
        }),
        library_metadata = LibraryMetadata(**{
            "maturity": "production",
            # "package": "great_expectations",
            "tags": [
                "arrows",
                "design",
                "flows",
            ],
            "contributors": [
                "@shinnyshinshin",
                "@abegong"
            ]
        }),
        renderers = RendererDiagnostics(),
        # **{
        #     "standard": {
        #         "renderer.answer": "Less than 90.0% of values in column \"a\" match the regular expression ^a.",
        #         "renderer.diagnostic.unexpected_statement": "\n\n1 unexpected values found. 20% of 5 total rows.",
        #         "renderer.diagnostic.observed_value": "20% unexpected",
        #         "renderer.diagnostic.status_icon": "",
        #         "renderer.diagnostic.unexpected_table": None,
        #         "renderer.prescriptive": "a values must match this regular expression: ^a, at least 90 % of the time.",
        #         "renderer.question": "Do at least 90.0% of values in column \"a\" match the regular expression ^a?"
        #     },
        #     "custom": []
        # }),
        examples = [expectation_test_data_case],
        metrics = [],
        #     "column_values.nonNone.unexpected_count",
        #     "column_values.match_regex.unexpected_count",
        #     "table.row_count",
        #     "column_values.match_regex.unexpected_values"
        # ],
        execution_engines = {
            "PandasExecutionEngine": True,
            "SqlAlchemyExecutionEngine": True,
            "SparkDFExecutionEngine": True
        }
    )

    print(edr)