from great_expectations.expectations.regex_based_column_map_expectation import (
    RegexBasedColumnMapExpectation,
)


class ExpectColumnValuesToHaveValidICD10CodeFormat(RegexBasedColumnMapExpectation):
    """Expect column values to have valid ICD10 code format.

    Checks if each value matches a regex for ICD10 codes. Does NOT ensure \
    the given code actually exists in any version of the ICD10.
    """

    def validate_configuration(self, configuration) -> None:
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            None. Raises InvalidExpectationConfigurationError if the config is not validated successfully
        """

        super().validate_configuration(configuration)
        configuration = configuration or self.configuration

    regex_camel_name = "ICD10Codes"
    regex = "[A-Za-z][0-9][A-Za-z0-9](?:\\.[A-Za-z0-9]{0,4})?\\Z"
    semantic_type_name_plural = None

    examples = [
        {
            "data": {
                "valid_icd10": ["Z99.0", "Z08", "J09.X2", "S22.000A"],
                "invalid_icd10": ["XXX.X", "AA2.01", "2A", "S22.0000A"],
            },
            "suppress_test_for": ["mssql", "bigquery", "redshift", "snowflake"],
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
        "contributors": ["@zachlindsey", "@mkopec87"],
    }


if __name__ == "__main__":
    ExpectColumnValuesToHaveValidICD10CodeFormat().print_diagnostic_checklist()
