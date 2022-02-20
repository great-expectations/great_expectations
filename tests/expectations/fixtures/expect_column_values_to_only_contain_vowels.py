from great_expectations.expectations.column_map_regex_expectation import ColumnMapRegexExpectation

class ExpectColumnValuesToOnlyContainVowels(ColumnMapRegexExpectation):
    """Values in this column should only contain vowels"""

    regex_snake_name = "vowel"
    regex_camel_name = "Vowel"
    regex = '^[aeiouyAEIOUY]*$'

    examples = [
        {
            "data": {
                "only_vowels": ["a", "e", "I", "O", "U", "y", ""],
                "mixed": ["A", "b", "c", "D", "E", "F", "g"],
                "longer_vowels": ["aei", "YAY", "oYu", "eee", "", "aeIOUY", None],
                "contains_vowels_but_also_other_stuff": ["baa", "aba", "aab", "1a1", "a a", " ", "*"],
            },
            "tests": [
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "in": {"column": "only_vowels"},
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
                        "unexpected_index_list": [1,2,3,5,6],
                    },
                    "include_in_gallery": True,
                },
                {
                    "title": "another_postive_test",
                    "exact_match_out": False,
                    "in": {"column": "longer_vowels"},
                    "out": {
                        "success": True,
                    },
                    "include_in_gallery": True,
                },
                {
                    "title": "another_negative_test",
                    "exact_match_out": False,
                    "in": {"column": "contains_vowels_but_also_other_stuff"},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [0,1,2,3,4,5,6],
                    },
                    "include_in_gallery": True,
                },
            ],
        }
    ]

    library_metadata = {
        "maturity": "experimental",
        "package": "great_expectations",
        "tags": [],
        "contributors": [],
    }

    map_metric = ColumnMapRegexExpectation._register_metric(
        regex_snake_name=regex_snake_name,
        regex_camel_name=regex_camel_name,
        regex_=regex,
    )