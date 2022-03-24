from great_expectations.expectations.api_based_column_map_expectation import (
    APIBasedColumnMapExpectation,
)


# <snippet>
class ExpectColumnValuesToBeValidMediumArticles(APIBasedColumnMapExpectation):
    """To-Do"""

    url_ = "https://medium.com/"
    api_camel_name = "Medium"
    api_semantic_name = "Medium"

    examples = [
        {
            "data": {
                "valid_articles": [
                    "analytics-vidhya/term-frequency-text-summarization-cc4e6381254c"
                ],
                "invalid_articles": [
                    "analytics-vidhya/term-frequency-text-summarization"
                ],
            },
            "tests": [
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "in": {"column": "valid_articles"},
                    "out": {
                        "success": True,
                    },
                    "include_in_gallery": True,
                },
                {
                    "title": "negative_test",
                    "exact_match_out": False,
                    "in": {"column": "invalid_articles"},
                    "out": {
                        "success": False,
                    },
                    "include_in_gallery": True,
                },
            ],
            "test_backends": [
                {
                    "backend": "pandas",
                    "dialects": None,
                },
                # {
                #     "backend": "sqlalchemy",
                #     "dialects": ["sqlite", "postgresql"],
                # },
                # {
                #     "backend": "spark",
                #     "dialects": None,
                # },
            ],
        }
    ]

    map_metric = APIBasedColumnMapExpectation.register_metric(
        api_camel_name=api_camel_name,
        url_=url_,
    )

    library_metadata = {
        "tags": ["api-based"],
        "contributors": ["@joegargery"],
    }


# </snippet>
if __name__ == "__main__":
    ExpectColumnValuesToBeValidMediumArticles().print_diagnostic_checklist()

# Note to users: code below this line is only for integration testing -- ignore!

# diagnostics = ExpectColumnValuesToBeValidMediumArticles().run_diagnostics()

# for check in diagnostics["tests"]:
#     assert check["test_passed"] is True
#     assert check["error_message"] is None
#     assert check["stack_trace"] is None
#
# for check in diagnostics["errors"]:
#     assert check is None
#
# for check in diagnostics["maturity_checklist"]["experimental"]:
#     assert check["passed"] is True
