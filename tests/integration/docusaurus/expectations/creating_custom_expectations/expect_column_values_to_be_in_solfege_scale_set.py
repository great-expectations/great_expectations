from great_expectations.expectations.set_based_column_map_expectation import (
    SetBasedColumnMapExpectation,
)


# <snippet>
class ExpectColumnValuesToBeInSolfegeScaleSet(SetBasedColumnMapExpectation):
    # </snippet>
    """Values in this column should be valid members of the Solfege scale: do, re, mi, etc."""

    set_ = [
        "do",
        "re",
        "mi",
        "fa",
        "so",
        "la",
        "ti",
        "Do",
        "Re",
        "Mi",
        "Fa",
        "So",
        "La",
        "Ti",
        "DO",
        "RE",
        "MI",
        "FA",
        "SO",
        "LA",
        "TI",
    ]
    set_camel_name = "SolfegeScale"
    set_semantic_name = "the Solfege scale"

    examples = [
        {
            "data": {
                "lowercase_solfege_scale": [
                    "do",
                    "re",
                    "mi",
                    "fa",
                    "so",
                    "la",
                    "ti",
                    "do",
                ],
                "uppercase_solfege_scale": [
                    "DO",
                    "RE",
                    "MI",
                    "FA",
                    "SO",
                    "LA",
                    "TI",
                    "DO",
                ],
                "mixed": ["do", "od", "re", "er", "mi", "im", "fa", "af"],
            },
            "tests": [
                {
                    "title": "positive_test_lowercase",
                    "exact_match_out": False,
                    "in": {"column": "lowercase_solfege_scale"},
                    "out": {
                        "success": True,
                    },
                    "include_in_gallery": True,
                },
                {
                    "title": "negative_test",
                    "exact_match_out": False,
                    "in": {"column": "mixed"},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [1, 3, 5, 7],
                    },
                    "include_in_gallery": True,
                },
                {
                    "title": "postive_test_uppercase",
                    "exact_match_out": False,
                    "in": {"column": "uppercase_solfege_scale"},
                    "out": {
                        "success": True,
                    },
                    "include_in_gallery": True,
                },
                {
                    "title": "positive_test_with_mostly",
                    "exact_match_out": False,
                    "in": {"column": "mixed", "mostly": 0.4},
                    "out": {
                        "success": True,
                        "unexpected_index_list": [1, 3, 5, 7],
                    },
                    "include_in_gallery": True,
                },
            ],
            "test_backends": [
                {
                    "backend": "pandas",
                    "dialects": None,
                },
                {
                    "backend": "sqlalchemy",
                    "dialects": ["sqlite", "postgresql"],
                },
                {
                    "backend": "spark",
                    "dialects": None,
                },
            ],
        }
    ]

    map_metric = SetBasedColumnMapExpectation.register_metric(
        set_camel_name=set_camel_name,
        set_=set_,
    )

    library_metadata = {
        "tags": ["set-based"],
        "contributors": ["@joegargery"],
    }


# </snippet>
if __name__ == "__main__":
    ExpectColumnValuesToBeInSolfegeScaleSet().print_diagnostic_checklist()

# Note to users: code below this line is only for integration testing -- ignore!

diagnostics = ExpectColumnValuesToBeInSolfegeScaleSet().run_diagnostics()

for check in diagnostics["tests"]:
    assert check["test_passed"] is True
    assert check["error_diagnostics"] is None

for check in diagnostics["errors"]:
    assert check is None

for check in diagnostics["maturity_checklist"]["experimental"]:
    if check["message"] == "Passes all linting checks":
        continue
    assert check["passed"] is True
