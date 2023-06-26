from __future__ import annotations

from great_expectations_experimental.api_based_column_map_expectation import (
    APIBasedColumnMapExpectation,
)
import requests


class ExpectColumnValuesToBeValidMediumArticles(APIBasedColumnMapExpectation):
    """To-Do"""

    @staticmethod
    def make_request(endpoint, method, header, body, auth, data_key, result_key, data):
        try:
            if method == "GET":
                url = endpoint + data
                r = requests.get(url=url)
                return not all(x in r.text for x in result_key)
        except requests.ConnectionError:
            print("failed to connect")
            return False

    endpoint_ = "https://medium.com/"
    method_ = "GET"
    header_ = None
    body_ = None
    auth_ = None
    data_key_ = None
    result_key_ = ["PAGE NOT FOUND", "404"]
    api_camel_name = "Medium"
    api_semantic_name = "Medium"
    request_func_ = make_request

    examples = [
        {
            "data": {
                "valid_articles": [
                    "analytics-vidhya/term-frequency-text-summarization-cc4e6381254c",
                    "analytics-vidhya/dbscan-from-scratch-almost-b02096600c14",
                    "analytics-vidhya/the-nature-of-trying-to-predict-student-success-is-tricky-16168faba8ec",
                    "analytics-vidhya/how-can-i-see-a-ufo-f8518b1e1920",
                ],
                "invalid_articles": [
                    "analytics-vidhya/term-frequency-text-summarization",
                    "analytics-vidhya/dbscan-from-scratch-almost",
                    "analytics-vidhya/the-nature-of-trying-to-predict-student-success-is-tricky",
                    "analytics-vidhya/how-can-i-see-a-ufo",
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
            ],
        }
    ]

    map_metric = APIBasedColumnMapExpectation.register_metric(
        api_camel_name=api_camel_name,
        endpoint_=endpoint_,
        method_=method_,
        header_=header_,
        body_=body_,
        auth_=auth_,
        data_key_=data_key_,
        result_key_=result_key_,
        request_func_=request_func_,
    )

    library_metadata = {
        "tags": ["api-based"],
        "contributors": ["@joegargery"],
    }


if __name__ == "__main__":
    ExpectColumnValuesToBeValidMediumArticles().print_diagnostic_checklist()

# Note to users: code below this line is only for integration testing -- ignore!

diagnostics = ExpectColumnValuesToBeValidMediumArticles().run_diagnostics()

for check in diagnostics["tests"]:
    assert check["test_passed"] is True
    assert check["error_diagnostics"] is None

for check in diagnostics["errors"]:
    assert check is None

for check in diagnostics["maturity_checklist"]["experimental"]:
    if check["message"] == "Passes all linting checks":
        continue
    assert check["passed"] is True
