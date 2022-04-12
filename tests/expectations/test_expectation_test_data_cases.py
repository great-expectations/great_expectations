from great_expectations.core.expectation_diagnostics.expectation_test_data_cases import (
    ExpectationLegacyTestCaseAdapter,
    TestData,
)


# @pytest.mark.skip(reason="Not yet supported")
def test_basic_instantiation_of_TestData():
    TestData(
        **{
            "a": ["aaa", "abb", "acc", "add", "bee"],
            "b": ["aaa", "abb", "acc", "bdd", None],
            "column_name with space": ["aaa", "abb", "acc", "add", "bee"],
        }
    )


def test_basic_instantiation_of_ExpectationTestCase():
    my_test_case = ExpectationLegacyTestCaseAdapter(
        **{
            "title": "basic_negative_test",
            "exact_match_out": False,
            "in": {"column": "a", "regex": "^a", "mostly": 0.9},
            "out": {
                "success": False,
                "unexpected_index_list": [4],
                "unexpected_list": ["bee"],
            },
            "suppress_test_for": ["sqlite", "mssql"],
        }
    )

    assert my_test_case.keys() == {
        "title",
        "input",
        "output",
        "suppress_test_for",
        "exact_match_out",
        "include_in_gallery",
        "only_for",
    }

    assert my_test_case.input == {"column": "a", "regex": "^a", "mostly": 0.9}
    assert my_test_case.input["column"] == "a"
    # assert my_test_case.input.column == "a" #Not supported yet. We'll need to add types to input and output to get there.
