from great_expectations.expectations.set_based_column_map_expectation import (
    SetBasedColumnMapExpectation,
)


class ExpectColumnValuesToBeValidHTTPMethods(SetBasedColumnMapExpectation):
    """Ensures all values are a valid http method:
    GET, HEAD, POST, PUT, DELETE, CONNECT, OPTIONS, TRACE, PATCH
    """

    # These values will be used to configure the metric created by your expectation
    set_ = [
        "GET",
        "HEAD",
        "POST",
        "PUT",
        "DELETE",
        "CONNECT",
        "OPTIONS",
        "TRACE",
        "PATCH",
    ]

    set_camel_name = "HTTPMethods"
    set_semantic_name = None

    examples = [
        {
            "data": {
                "uppercase_http_methods": [
                    "GET",
                    "HEAD",
                    "POST",
                    "PUT",
                    "DELETE",
                    "CONNECT",
                    "OPTIONS",
                    "TRACE",
                    "PATCH",
                ],
                "bad_stuff": [
                    "GET",
                    "foo",
                    "POST",
                    "bar",
                    "GET",
                    "GET",
                    "GET",
                    "PUT",
                    "baz",
                ],
            },
            "tests": [
                {
                    "title": "positive_test_lowercase",
                    "exact_match_out": False,
                    "in": {"column": "uppercase_http_methods"},
                    "out": {
                        "success": True,
                    },
                    "include_in_gallery": True,
                },
                {
                    "title": "negative_test",
                    "exact_match_out": False,
                    "in": {"column": "bad_stuff"},
                    "out": {"success": False, "unexpected_index_list": [1, 3, 8]},
                    "include_in_gallery": True,
                },
            ],
            "test_backends": [
                {
                    "backend": "pandas",
                    "dialects": None,
                }
            ],
        }
    ]

    map_metric = SetBasedColumnMapExpectation.register_metric(
        set_camel_name=set_camel_name,
        set_=set_,
    )

    library_metadata = {
        "tags": ["type-entities", "hackathon", "set-based"],
        "contributors": [
            "@zachlindsey",
        ],
    }


if __name__ == "__main__":
    ExpectColumnValuesToBeValidHTTPMethods().print_diagnostic_checklist()
