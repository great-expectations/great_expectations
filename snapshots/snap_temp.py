# snapshottest: v1 - https://goo.gl/zC4yUc

from snapshottest import Snapshot

snapshots = Snapshot()

snapshots["test_atomic_prescriptive_expect_column_distinct_values_to_be_in_set 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "value_set": {"schema": {"type": "array"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column distinct values must belong to this set: [ ]",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_column_distinct_values_to_contain_set 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "parse_strings_as_datetimes": {
                "schema": {"type": "boolean"},
                "value": None,
            },
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "value_set": {"schema": {"type": "array"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column distinct values must contain this set: [ ].",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_column_distinct_values_to_equal_set 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "parse_strings_as_datetimes": {
                "schema": {"type": "boolean"},
                "value": None,
            },
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "value_set": {"schema": {"type": "array"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column distinct values must match this set: [ ].",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_column_kl_divergence_to_be_less_than 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "expectation_type": {
                "schema": {"type": "string"},
                "value": "expect_column_kl_divergence_to_be_less_than",
            },
            "kwargs": {"schema": {"type": "string"}, "value": {}},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$expectation_type(**$kwargs)",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_column_max_to_be_between 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "max_value": {"schema": {"type": "number"}, "value": None},
            "min_value": {"schema": {"type": "number"}, "value": None},
            "parse_strings_as_datetimes": {
                "schema": {"type": "boolean"},
                "value": None,
            },
            "strict_max": {"schema": {"type": "boolean"}, "value": None},
            "strict_min": {"schema": {"type": "boolean"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column maximum value may have any numerical value.",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_column_mean_to_be_between 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "max_value": {"schema": {"type": "number"}, "value": None},
            "min_value": {"schema": {"type": "number"}, "value": None},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "strict_max": {"schema": {"type": "boolean"}, "value": None},
            "strict_min": {"schema": {"type": "boolean"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column mean may have any numerical value.",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_column_median_to_be_between 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "max_value": {"schema": {"type": "number"}, "value": None},
            "min_value": {"schema": {"type": "number"}, "value": None},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "strict_max": {"schema": {"type": "boolean"}, "value": None},
            "strict_min": {"schema": {"type": "boolean"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column median may have any numerical value.",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_column_min_to_be_between 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "max_value": {"schema": {"type": "number"}, "value": None},
            "min_value": {"schema": {"type": "number"}, "value": None},
            "parse_strings_as_datetimes": {
                "schema": {"type": "boolean"},
                "value": None,
            },
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "strict_max": {"schema": {"type": "boolean"}, "value": None},
            "strict_min": {"schema": {"type": "boolean"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column minimum value may have any numerical value.",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_column_most_common_value_to_be_in_set 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "ties_okay": {"schema": {"type": "boolean"}, "value": None},
            "value_set": {"schema": {"type": "array"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column most common value must belong to this set: [ ].",
    },
    "valuetype": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_expect_column_pair_cramers_phi_value_to_be_less_than 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column_A": {"schema": {"type": "string"}, "value": None},
            "column_B": {"schema": {"type": "string"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": " unrecognized kwargs for expect_column_pair_cramers_phi_value_to_be_less_than: missing column.",
    },
    "valuetype": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_expect_column_pair_values_a_to_be_greater_than_b 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column_A": {"schema": {"type": "string"}, "value": None},
            "column_B": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "ignore_row_if": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": None},
            "or_equal": {"schema": {"type": "boolean"}, "value": None},
            "parse_strings_as_datetimes": {
                "schema": {"type": "boolean"},
                "value": None,
            },
            "row_condition": {"schema": {"type": "string"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "Values in $column_A must always be greater than those in $column_B.",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_column_pair_values_to_be_equal 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column_A": {"schema": {"type": "string"}, "value": None},
            "column_B": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "ignore_row_if": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": None},
            "parse_strings_as_datetimes": {
                "schema": {"type": "boolean"},
                "value": None,
            },
            "row_condition": {"schema": {"type": "string"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "Values in $column_A and $column_B must always be equal.",
    },
    "valuetype": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_expect_column_proportion_of_unique_values_to_be_between 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "max_value": {"schema": {"type": "number"}, "value": None},
            "min_value": {"schema": {"type": "number"}, "value": None},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "strict_max": {"schema": {"type": "boolean"}, "value": None},
            "strict_min": {"schema": {"type": "boolean"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column may have any fraction of unique values.",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_column_quantile_values_to_be_between 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "expectation_type": {
                "schema": {"type": "string"},
                "value": "expect_column_quantile_values_to_be_between",
            },
            "kwargs": {"schema": {"type": "string"}, "value": {}},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$expectation_type(**$kwargs)",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_column_stdev_to_be_between 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "max_value": {"schema": {"type": "number"}, "value": None},
            "min_value": {"schema": {"type": "number"}, "value": None},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "strict_max": {"schema": {"type": "boolean"}, "value": None},
            "strict_min": {"schema": {"type": "boolean"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column standard deviation may have any numerical value.",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_column_sum_to_be_between 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "max_value": {"schema": {"type": "number"}, "value": None},
            "min_value": {"schema": {"type": "number"}, "value": None},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "strict_max": {"schema": {"type": "boolean"}, "value": None},
            "strict_min": {"schema": {"type": "boolean"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column sum may have any numerical value.",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_column_to_exist 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "column_index": {"schema": {"type": "number"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column is a required field.",
    },
    "valuetype": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_expect_column_unique_value_count_to_be_between 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "max_value": {"schema": {"type": "number"}, "value": None},
            "min_value": {"schema": {"type": "number"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": None},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "strict_max": {"schema": {"type": "boolean"}, "value": None},
            "strict_min": {"schema": {"type": "boolean"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column may have any number of unique values.",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_column_value_lengths_to_be_between 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "max_value": {"schema": {"type": "number"}, "value": None},
            "min_value": {"schema": {"type": "number"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": None},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "strict_max": {"schema": {"type": "boolean"}, "value": None},
            "strict_min": {"schema": {"type": "boolean"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values may have any length.",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_column_value_lengths_to_equal 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": None},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "value": {"schema": {"type": "number"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values may have any length.",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_column_value_z_scores_to_be_less_than 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "expectation_type": {
                "schema": {"type": "string"},
                "value": "expect_column_value_z_scores_to_be_less_than",
            },
            "kwargs": {"schema": {"type": "string"}, "value": {}},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$expectation_type(**$kwargs)",
    },
    "valuetype": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_expect_column_values_to_be_dateutil_parseable 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": None},
            "row_condition": {"schema": {"type": "string"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must be parseable by dateutil.",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_column_values_to_be_decreasing 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": None},
            "parse_strings_as_datetimes": {
                "schema": {"type": "boolean"},
                "value": None,
            },
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "strictly": {"schema": {"type": "boolean"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must be less than or equal to previous values.",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_column_values_to_be_in_set 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": None},
            "parse_strings_as_datetimes": {
                "schema": {"type": "boolean"},
                "value": None,
            },
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "value_set": {"schema": {"type": "array"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must belong to this set: [ ].",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_column_values_to_be_increasing 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": None},
            "parse_strings_as_datetimes": {
                "schema": {"type": "boolean"},
                "value": None,
            },
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "strictly": {"schema": {"type": "boolean"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must be greater than or equal to previous values.",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_column_values_to_be_json_parseable 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": None},
            "row_condition": {"schema": {"type": "string"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must be parseable as JSON.",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_column_values_to_be_null 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": None},
            "row_condition": {"schema": {"type": "string"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must be null.",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_column_values_to_be_of_type 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": None},
            "mostly_pct": {"schema": {"type": "number"}, "value": None},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "type_": {"schema": {"type": "string"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must be of type $type_.",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_column_values_to_be_unique 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": None},
            "row_condition": {"schema": {"type": "string"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must be unique.",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_column_values_to_match_json_schema 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "json_schema": {"schema": {"type": "object"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": None},
            "row_condition": {"schema": {"type": "string"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must match a JSON Schema but none was specified.",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_column_values_to_match_regex 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": None},
            "mostly_pct": {"schema": {"type": "number"}, "value": None},
            "regex": {"schema": {"type": "string"}, "value": None},
            "row_condition": {"schema": {"type": "string"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must match a regular expression but none was specified.",
    },
    "valuetype": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_expect_column_values_to_match_strftime_format 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": None},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "strftime_format": {"schema": {"type": "string"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must match a strftime format but none was specified.",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_column_values_to_not_be_in_set 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": None},
            "parse_strings_as_datetimes": {
                "schema": {"type": "boolean"},
                "value": None,
            },
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "value_set": {"schema": {"type": "array"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must not belong to this set: [ ].",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_column_values_to_not_be_null 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": None},
            "mostly_pct": {"schema": {"type": "number"}, "value": None},
            "row_condition": {"schema": {"type": "string"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must never be null.",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_column_values_to_not_match_regex 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column": {"schema": {"type": "string"}, "value": None},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": None},
            "regex": {"schema": {"type": "string"}, "value": None},
            "row_condition": {"schema": {"type": "string"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "values must not match a regular expression but none was specified.",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_table_column_count_to_be_between 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "max_value": {"schema": {"type": "number"}, "value": None},
            "min_value": {"schema": {"type": "number"}, "value": None},
            "strict_max": {"schema": {"type": "boolean"}, "value": None},
            "strict_min": {"schema": {"type": "boolean"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "May have any number of columns.",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_table_columns_to_match_set 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "column_list": {"schema": {"type": "array"}, "value": ["a", "b", "c"]},
            "exact_match": {"schema": {"type": "boolean"}, "value": True},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "Must have exactly these columns (in any order): $column_list_0, $column_list_1, $column_list_2",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_table_row_count_to_be_between 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "max_value": {"schema": {"type": "number"}, "value": None},
            "min_value": {"schema": {"type": "number"}, "value": 1},
            "strict_max": {"schema": {"type": "boolean"}, "value": None},
            "strict_min": {"schema": {"type": "boolean"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "Must have greater than or equal to $min_value rows.",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_table_row_count_to_equal 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "value": {"schema": {"type": "number"}, "value": 10},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "Must have exactly $value rows.",
    },
    "valuetype": "StringValueType",
}

snapshots["test_atomic_prescriptive_expect_table_row_count_to_equal_other_table 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "params": {
            "other_table_name": {
                "schema": {"type": "string"},
                "value": {"schema": {"type": "string"}, "value": "other_table_name"},
            }
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "Row count must equal the row count of table $other_table_name.",
    },
    "valuetype": "StringValueType",
}
