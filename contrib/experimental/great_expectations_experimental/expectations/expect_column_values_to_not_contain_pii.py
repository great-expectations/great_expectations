from great_expectations_experimental.api_based_column_map_expectation import (
    APIBasedColumnMapExpectation,
)
import requests


class ExpectColumnValuesToNotContainPII(APIBasedColumnMapExpectation):
    """Expect values in a given column to not contain PII as identified by the Private-AI API."""

    @staticmethod
    def make_request(endpoint, method, header, body, auth, data_key, result_key, data):
        try:
            if method == "POST":
                body[data_key] = data
                r = requests.post(url=endpoint, headers=header, json=body)
                return r.json()[result_key] == data
        except requests.ConnectionError:
            print("failed to connect")
            return False

    endpoint_ = "https://api.private-ai.com/deid/v1/deidentify_text"
    method_ = "POST"
    header_ = {"content-type": "application/json"}
    body_ = {"key": "<PRIVATE_AI_KEY>", "fake_entity_accuracy_mode": "standard"}
    auth_ = None
    data_key_ = "text"
    result_key_ = "result"
    api_camel_name = "PrivateAI"
    request_func_ = make_request

    examples = [
        {
            "data": {
                "pii": [
                    "so, it expires the 21st; and the 3 digits on the back?; 456",
                    "can I claim massage therapy I had in the states or only from Canadian providers?",
                    "Yes, under your primary policy, that's all covered. 4 3 5 5 2 4 5 5 6 3 4, that's right",
                    "Hi John, my name is Grace. John, could you pass me the salt please?",
                    "grace we'''re at 223 spadina #6. yeah, it'''s 416 451-4516",
                ],
                "redacted": [
                    "Hi [NAME_1], my name is [NAME_2]. [NAME_1], could you pass me the salt please?",
                    "[NAME_1] we're at [LOCATION_ADDRESS_1]. yeah, it's [PHONE_NUMBER_1]",
                    "Message-ID: <[NUMERICAL_PII]> \n [DATE] [TIME] \n To: [EMAIL_ADDRESS]",
                    "My name is: [NAME_1]",
                    "Hi [NAME_1], could you tell me your phone number please?",
                ],
                "clean": [
                    "Hi, my name is name. Friend, could you pass me the salt please?",
                    "Person we're at place. yeah, it's number.",
                    "Message-ID: <*****> \n date-time \n To: address",
                    "My name is: name",
                    "Hi name, could you tell me your phone number please?",
                ],
            },
            "tests": [
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "in": {"column": "pii"},
                    "out": {
                        "success": False,
                    },
                    "include_in_gallery": True,
                },
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "in": {"column": "redacted"},
                    "out": {
                        "success": True,
                    },
                    "include_in_gallery": True,
                },
                {
                    "title": "no_change_positive_test",
                    "exact_match_out": False,
                    "in": {"column": "redacted"},
                    "out": {
                        "success": True,
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
    ExpectColumnValuesToNotContainPII().print_diagnostic_checklist()

# Note to users: code below this line is only for integration testing -- ignore!

diagnostics = ExpectColumnValuesToNotContainPII().run_diagnostics()

for check in diagnostics["tests"]:
    assert check["test_passed"] is True
    assert check["error_diagnostics"] is None

for check in diagnostics["errors"]:
    assert check is None

for check in diagnostics["maturity_checklist"]["experimental"]:
    if check["message"] == "Passes all linting checks":
        continue
    assert check["passed"] is True
