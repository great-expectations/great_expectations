from great_expectations.expectations.regex_based_column_map_expectation import (
    RegexBasedColumnMapExpectation,
)


class ExpectColumnValuesToHaveValidICD10CodeFormat(RegexBasedColumnMapExpectation):
    """Checks if each value matches a regex for ICD10 codes. Does NOT ensure
    the given code actually exists in any version of the ICD10.
    """

    regex_camel_name = "ICD10Codes"
    regex = "[A-Za-z][0-9][A-Za-z0-9](?:\\.[A-Za-z0-9]{0,4})?\\Z"
    semantic_type_name_plural = None

    examples = [
        {
            "data": {
                "valid_icd10": ["Z99.0", "Z08", "J09.X2", "S22.000A"],
                "invalid_icd10": ["XXX.X", "AA2.01", "2A", "S22.0000A"],
            },
            "tests": [
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "in": {"column": "valid_icd10"},
                    "out": {"success": True},
                    "include_in_gallery": True,
                },
                {
                    "title": "negative_test",
                    "exact_match_out": False,
                    "in": {"column": "invalid_icd10"},
                    "out": {"success": False, "unexpected_index_list": [0, 1, 2, 3]},
                },
            ],
            "test_backends": [{"backend": "pandas", "dialects": None}],
        }
    ]

    # Here your regex is used to create a custom metric for this expectation
    map_metric = RegexBasedColumnMapExpectation.register_metric(
        regex_camel_name=regex_camel_name,
        regex_=regex,
    )

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": ["typed-entities", "hackathon"],
        "contributors": [
            "@zachlindsey",
        ],
    }


if __name__ == "__main__":
    ExpectColumnValuesToHaveValidICD10CodeFormat().print_diagnostic_checklist()
