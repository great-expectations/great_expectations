from great_expectations.expectations.regex_based_column_map_expectation import (
    RegexBasedColumnMapExpectation,
)


class ExpectColumnValuesToBeValidScientificNotation(RegexBasedColumnMapExpectation):
    """Expect values in this column to be a valid scientific notation string."""

    # These values will be used to configure the metric created by your expectation
    regex_camel_name = "ScientificNotation"
    regex = r"^[+\-]?(?=\.\d|\d)(?:0|[1-9]\d*)?(?:\.\d+)?(?:(?<=\d)(?:[eE][+\-]?\d+))?$"
    semantic_type_name_plural = "scientific_notations"

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "valid": ["-3.14", "1.07E-10", "3.14e-12"],
                "invalid": ["11.e-12", "0E+5", "007"],
                "empty": ["", None, None],
            },
            "suppress_test_for": ["mssql", "bigquery", "redshift", "snowflake"],
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "valid"},
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "invalid", "mostly": 1},
                    "out": {
                        "success": False,
                    },
                },
                {
                    "title": "empty",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "empty", "mostly": 1},
                    "out": {
                        "success": False,
                    },
                },
            ],
        }
    ]

    # Here your regex is used to create a custom metric for this expectation
    map_metric = RegexBasedColumnMapExpectation.register_metric(
        regex_camel_name=regex_camel_name,
        regex_=regex,
    )

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "maturity": "experimental",
        "tags": [
            "scientific_notation",
            "expectation",
        ],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@rdodev",  # Don't forget to add your github handle here!
            "@mkopec87",
        ],
    }


if __name__ == "__main__":
    ExpectColumnValuesToBeValidScientificNotation().print_diagnostic_checklist()
