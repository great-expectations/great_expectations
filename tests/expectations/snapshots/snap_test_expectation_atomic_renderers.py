# snapshottest: v1 - https://goo.gl/zC4yUc

from snapshottest import Snapshot

snapshots = Snapshot()

snapshots[
    "test_atomic_diagnostic_observed_value_expect_column_kl_divergence_to_be_less_than 1"
] = {
    "name": "atomic.diagnostic.observed_value",
    "value": {
        "graph": {
            "$schema": "https://vega.github.io/schema/vega-lite/v4.8.1.json",
            "autosize": "fit",
            "config": {"view": {"continuousHeight": 300, "continuousWidth": 400}},
            "data": {"name": "data-c49f2d3d7bdab36f9ec81f5a314a430c"},
            "datasets": {
                "data-c49f2d3d7bdab36f9ec81f5a314a430c": [
                    {"fraction": 0.3754, "values": 1},
                    {"fraction": 0.615, "values": 2},
                    {"fraction": 0.0096, "values": 4},
                ]
            },
            "encoding": {
                "tooltip": [
                    {"field": "values", "type": "quantitative"},
                    {"field": "fraction", "type": "quantitative"},
                ],
                "x": {"field": "values", "type": "nominal"},
                "y": {"field": "fraction", "type": "quantitative"},
            },
            "height": 400,
            "mark": "bar",
            "width": 250,
        },
        "header": {
            "schema": {"type": "StringValueType"},
            "value": {
                "params": {
                    "observed_value": {
                        "schema": {"type": "string"},
                        "value": "None (-infinity, infinity, or NaN)",
                    }
                },
                "template": "KL Divergence: $observed_value",
            },
        },
        "schema": {"type": "GraphType"},
    },
    "value_type": "GraphType",
}

snapshots[
    "test_atomic_diagnostic_observed_value_expect_column_quantile_values_to_be_between 1"
] = {
    "name": "atomic.diagnostic.observed_value",
    "value": {
        "header": None,
        "header_row": [
            {"schema": {"type": "string"}, "value": "Quantile"},
            {"schema": {"type": "string"}, "value": "Value"},
        ],
        "schema": {"type": "TableType"},
        "table": [
            [
                {"schema": {"type": "string"}, "value": "0.05"},
                {"schema": {"type": "number"}, "value": 67},
            ],
            [
                {"schema": {"type": "string"}, "value": "Q1"},
                {"schema": {"type": "number"}, "value": 329},
            ],
            [
                {"schema": {"type": "string"}, "value": "Median"},
                {"schema": {"type": "number"}, "value": 657},
            ],
            [
                {"schema": {"type": "string"}, "value": "Q3"},
                {"schema": {"type": "number"}, "value": 985},
            ],
            [
                {"schema": {"type": "string"}, "value": "0.95"},
                {"schema": {"type": "number"}, "value": 1247},
            ],
        ],
    },
    "value_type": "TableType",
}

snapshots["test_atomic_diagnostic_observed_value_with_empty_result 1"] = {
    "name": "atomic.diagnostic.observed_value",
    "value": {
        "header": None,
        "params": {},
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "--",
    },
    "value_type": "StringValueType",
}

snapshots["test_atomic_diagnostic_observed_value_with_numeric_observed_value 1"] = {
    "name": "atomic.diagnostic.observed_value",
    "value": {
        "header": None,
        "params": {},
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "1,776",
    },
    "value_type": "StringValueType",
}

snapshots["test_atomic_diagnostic_observed_value_with_str_observed_value 1"] = {
    "name": "atomic.diagnostic.observed_value",
    "value": {
        "header": None,
        "params": {},
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "foo",
    },
    "value_type": "StringValueType",
}

snapshots["test_atomic_diagnostic_observed_value_with_unexpected_percent 1"] = {
    "name": "atomic.diagnostic.observed_value",
    "value": {
        "header": None,
        "params": {},
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "10% unexpected",
    },
    "value_type": "StringValueType",
}

snapshots["test_atomic_diagnostic_observed_value_without_result 1"] = {
    "name": "atomic.diagnostic.observed_value",
    "value": {
        "header": None,
        "params": {},
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "--",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_column_distinct_values_to_be_in_set 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "v__0": {"schema": {"type": "string"}, "value": 1},
            "v__1": {"schema": {"type": "string"}, "value": 2},
            "v__2": {"schema": {"type": "string"}, "value": 3},
            "value_set": {"schema": {"type": "array"}, "value": [1, 2, 3]},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column distinct values must belong to this set: $v__0 $v__1 $v__2.",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_column_distinct_values_to_contain_set 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "parse_strings_as_datetimes": {
                "schema": {"type": "boolean"},
                "value": True,
            },
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "v__0": {"schema": {"type": "string"}, "value": "a"},
            "v__1": {"schema": {"type": "string"}, "value": "b"},
            "v__2": {"schema": {"type": "string"}, "value": "c"},
            "value_set": {"schema": {"type": "array"}, "value": ["a", "b", "c"]},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column distinct values must contain this set: $v__0 $v__1 $v__2. Values should be parsed as datetimes.",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_column_distinct_values_to_equal_set 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "parse_strings_as_datetimes": {
                "schema": {"type": "boolean"},
                "value": True,
            },
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "v__0": {"schema": {"type": "string"}, "value": "a"},
            "v__1": {"schema": {"type": "string"}, "value": "b"},
            "v__2": {"schema": {"type": "string"}, "value": "c"},
            "value_set": {"schema": {"type": "array"}, "value": ["a", "b", "c"]},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column distinct values must match this set: $v__0 $v__1 $v__2. Values should be parsed as datetimes.",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_column_kl_divergence_to_be_less_than 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "graph": {
            "$schema": "https://vega.github.io/schema/vega-lite/v4.8.1.json",
            "autosize": "fit",
            "config": {"view": {"continuousHeight": 300, "continuousWidth": 400}},
            "data": {"name": "data-1cb20570b53cc3e67cb4883fd45e64cb"},
            "datasets": {
                "data-1cb20570b53cc3e67cb4883fd45e64cb": [
                    {"bin_max": 5, "bin_min": 0, "fraction": 0.2},
                    {"bin_max": 10, "bin_min": 5, "fraction": 0.3},
                    {"bin_max": 30, "bin_min": 10, "fraction": 0.1},
                    {"bin_max": 50, "bin_min": 30, "fraction": 0.4},
                ]
            },
            "encoding": {
                "tooltip": [
                    {"field": "bin_min", "type": "quantitative"},
                    {"field": "bin_max", "type": "quantitative"},
                    {"field": "fraction", "type": "quantitative"},
                ],
                "x": {"field": "bin_min", "type": "ordinal"},
                "x2": {"field": "bin_max"},
                "y": {"field": "fraction", "type": "quantitative"},
            },
            "height": 400,
            "mark": "bar",
            "width": 250,
        },
        "header": {
            "schema": {"type": "StringValueType"},
            "value": {
                "params": {
                    "column": {"schema": {"type": "string"}, "value": "min_event_time"},
                    "condition_parser": {"schema": {"type": "string"}, "value": None},
                    "mostly": {"schema": {"type": "number"}, "value": None},
                    "row_condition": {"schema": {"type": "string"}, "value": None},
                    "threshold": {"schema": {"type": "number"}, "value": 0.1},
                },
                "template": "$column Kullback-Leibler (KL) divergence with respect to the following distribution must be lower than $threshold.",
            },
        },
        "schema": {"type": "GraphType"},
    },
    "value_type": "GraphType",
}

snapshots["test_atomic_prescriptive_summary_expect_column_max_to_be_between 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "max_value": {"schema": {"type": "number"}, "value": 5},
            "min_value": {"schema": {"type": "number"}, "value": 1},
            "parse_strings_as_datetimes": {
                "schema": {"type": "boolean"},
                "value": True,
            },
            "strict_max": {"schema": {"type": "boolean"}, "value": None},
            "strict_min": {"schema": {"type": "boolean"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column maximum value must be greater than or equal to $min_value and less than or equal to $max_value. Values should be parsed as datetimes.",
    },
    "value_type": "StringValueType",
}

snapshots["test_atomic_prescriptive_summary_expect_column_mean_to_be_between 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "max_value": {"schema": {"type": "number"}, "value": 7},
            "min_value": {"schema": {"type": "number"}, "value": 3},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "strict_max": {"schema": {"type": "boolean"}, "value": None},
            "strict_min": {"schema": {"type": "boolean"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column mean must be greater than or equal to $min_value and less than or equal to $max_value.",
    },
    "value_type": "StringValueType",
}

snapshots["test_atomic_prescriptive_summary_expect_column_median_to_be_between 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "max_value": {"schema": {"type": "number"}, "value": 10},
            "min_value": {"schema": {"type": "number"}, "value": 5},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "strict_max": {"schema": {"type": "boolean"}, "value": None},
            "strict_min": {"schema": {"type": "boolean"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column median must be greater than or equal to $min_value and less than or equal to $max_value.",
    },
    "value_type": "StringValueType",
}

snapshots["test_atomic_prescriptive_summary_expect_column_min_to_be_between 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "max_value": {"schema": {"type": "number"}, "value": 5},
            "min_value": {"schema": {"type": "number"}, "value": 1},
            "parse_strings_as_datetimes": {
                "schema": {"type": "boolean"},
                "value": True,
            },
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "strict_max": {"schema": {"type": "boolean"}, "value": None},
            "strict_min": {"schema": {"type": "boolean"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column minimum value must be greater than or equal to $min_value and less than or equal to $max_value. Values should be parsed as datetimes.",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_column_most_common_value_to_be_in_set 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "ties_okay": {"schema": {"type": "boolean"}, "value": True},
            "v__0": {"schema": {"type": "string"}, "value": 1},
            "v__1": {"schema": {"type": "string"}, "value": 2},
            "v__2": {"schema": {"type": "string"}, "value": 3},
            "value_set": {"schema": {"type": "array"}, "value": [1, 2, 3]},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column most common value must belong to this set: $v__0 $v__1 $v__2. Values outside this set that are as common (but not more common) are allowed.",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_column_pair_cramers_phi_value_to_be_less_than 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column_A": {"schema": {"type": "string"}, "value": "foo"},
            "column_B": {"schema": {"type": "string"}, "value": "bar"},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "Values in $column_A and $column_B must be independent.",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_column_pair_values_a_to_be_greater_than_b 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column_A": {"schema": {"type": "string"}, "value": "foo"},
            "column_B": {"schema": {"type": "string"}, "value": "bar"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "ignore_row_if": {"schema": {"type": "string"}, "value": "baz"},
            "mostly": {"schema": {"type": "number"}, "value": 0.8},
            "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            "or_equal": {"schema": {"type": "boolean"}, "value": True},
            "parse_strings_as_datetimes": {
                "schema": {"type": "boolean"},
                "value": True,
            },
            "row_condition": {"schema": {"type": "string"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "Values in $column_A must be greater than or equal to those in $column_B, at least $mostly_pct % of the time. Values should be parsed as datetimes.",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_column_pair_values_to_be_equal 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column_A": {"schema": {"type": "string"}, "value": "foo"},
            "column_B": {"schema": {"type": "string"}, "value": "bar"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "ignore_row_if": {"schema": {"type": "string"}, "value": "baz"},
            "mostly": {"schema": {"type": "number"}, "value": 0.8},
            "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            "parse_strings_as_datetimes": {
                "schema": {"type": "boolean"},
                "value": None,
            },
            "row_condition": {"schema": {"type": "string"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "Values in $column_A and $column_B must be equal, at least $mostly_pct % of the time.",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_column_proportion_of_unique_values_to_be_between 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "max_value": {"schema": {"type": "number"}, "value": 20},
            "min_value": {"schema": {"type": "number"}, "value": 10},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "strict_max": {"schema": {"type": "boolean"}, "value": None},
            "strict_min": {"schema": {"type": "boolean"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column fraction of unique values must be greater than or equal to $min_value and less than or equal to $max_value.",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_column_quantile_values_to_be_between 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": {
            "schema": {"type": "StringValueType"},
            "value": {
                "params": {
                    "column": {"schema": {"type": "string"}, "value": "Unnamed: 0"},
                    "condition_parser": {"schema": {"type": "string"}, "value": None},
                    "mostly": {"schema": {"type": "number"}, "value": None},
                    "row_condition": {"schema": {"type": "string"}, "value": None},
                },
                "template": "$column quantiles must be within the following value ranges.",
            },
        },
        "header_row": [
            {"schema": {"type": "string"}, "value": "Quantile"},
            {"schema": {"type": "string"}, "value": "Min Value"},
            {"schema": {"type": "string"}, "value": "Max Value"},
        ],
        "schema": {"type": "TableType"},
        "table": [
            [
                {"schema": {"type": "string"}, "value": "0.05"},
                {"schema": {"type": "number"}, "value": 66},
                {"schema": {"type": "number"}, "value": 68},
            ],
            [
                {"schema": {"type": "string"}, "value": "Q1"},
                {"schema": {"type": "number"}, "value": 328},
                {"schema": {"type": "number"}, "value": 330},
            ],
            [
                {"schema": {"type": "string"}, "value": "Median"},
                {"schema": {"type": "number"}, "value": 656},
                {"schema": {"type": "number"}, "value": 658},
            ],
            [
                {"schema": {"type": "string"}, "value": "Q3"},
                {"schema": {"type": "number"}, "value": 984},
                {"schema": {"type": "number"}, "value": 986},
            ],
            [
                {"schema": {"type": "string"}, "value": "0.95"},
                {"schema": {"type": "number"}, "value": 1246},
                {"schema": {"type": "number"}, "value": 1248},
            ],
        ],
    },
    "value_type": "TableType",
}

snapshots["test_atomic_prescriptive_summary_expect_column_stdev_to_be_between 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "max_value": {"schema": {"type": "number"}, "value": 20},
            "min_value": {"schema": {"type": "number"}, "value": 10},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "strict_max": {"schema": {"type": "boolean"}, "value": None},
            "strict_min": {"schema": {"type": "boolean"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column standard deviation must be greater than or equal to $min_value and less than or equal to $max_value.",
    },
    "value_type": "StringValueType",
}

snapshots["test_atomic_prescriptive_summary_expect_column_sum_to_be_between 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "max_value": {"schema": {"type": "number"}, "value": 20},
            "min_value": {"schema": {"type": "number"}, "value": 10},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "strict_max": {"schema": {"type": "boolean"}, "value": None},
            "strict_min": {"schema": {"type": "boolean"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column sum must be greater than or equal to $min_value and less than or equal to $max_value.",
    },
    "value_type": "StringValueType",
}

snapshots["test_atomic_prescriptive_summary_expect_column_to_exist 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "column_index": {"schema": {"type": "number"}, "value": 5},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column must be the $column_indexth field.",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_column_unique_value_count_to_be_between 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "max_value": {"schema": {"type": "number"}, "value": 20},
            "min_value": {"schema": {"type": "number"}, "value": 10},
            "mostly": {"schema": {"type": "number"}, "value": 0.8},
            "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "strict_max": {"schema": {"type": "boolean"}, "value": None},
            "strict_min": {"schema": {"type": "boolean"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column must have greater than or equal to $min_value and less than or equal to $max_value unique values, at least $mostly_pct % of the time.",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_column_value_lengths_to_be_between 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "max_value": {"schema": {"type": "number"}, "value": 20},
            "min_value": {"schema": {"type": "number"}, "value": 10},
            "mostly": {"schema": {"type": "number"}, "value": 0.8},
            "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "strict_max": {"schema": {"type": "boolean"}, "value": None},
            "strict_min": {"schema": {"type": "boolean"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must be greater than or equal to $min_value and less than or equal to $max_value characters long, at least $mostly_pct % of the time.",
    },
    "value_type": "StringValueType",
}

snapshots["test_atomic_prescriptive_summary_expect_column_value_lengths_to_equal 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": 0.8},
            "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "value": {"schema": {"type": "number"}, "value": 100},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must be $value characters long, at least $mostly_pct % of the time.",
    },
    "value_type": "StringValueType",
}

snapshots["test_atomic_prescriptive_summary_expect_column_values_to_be_between 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "max_value": {"schema": {"type": "number"}, "value": 5},
            "min_value": {"schema": {"type": "number"}, "value": 1},
            "mostly": {"schema": {"type": "number"}, "value": 0.8},
            "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "strict_max": {"schema": {"type": "boolean"}, "value": None},
            "strict_min": {"schema": {"type": "boolean"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must be greater than or equal to $min_value and less than or equal to $max_value, at least $mostly_pct % of the time.",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_column_values_to_be_dateutil_parseable 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": 0.8},
            "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            "row_condition": {"schema": {"type": "string"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must be parseable by dateutil, at least $mostly_pct % of the time.",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_column_values_to_be_decreasing 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": 0.8},
            "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            "parse_strings_as_datetimes": {
                "schema": {"type": "boolean"},
                "value": True,
            },
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "strictly": {"schema": {"type": "boolean"}, "value": 50},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must be strictly less than previous values, at least $mostly_pct % of the time. Values should be parsed as datetimes.",
    },
    "value_type": "StringValueType",
}

snapshots["test_atomic_prescriptive_summary_expect_column_values_to_be_in_set 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": 0.8},
            "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            "parse_strings_as_datetimes": {
                "schema": {"type": "boolean"},
                "value": True,
            },
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "v__0": {"schema": {"type": "string"}, "value": 1},
            "v__1": {"schema": {"type": "string"}, "value": 2},
            "v__2": {"schema": {"type": "string"}, "value": 3},
            "v__3": {"schema": {"type": "string"}, "value": 4},
            "value_set": {"schema": {"type": "array"}, "value": [1, 2, 3, 4]},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must belong to this set: $v__0 $v__1 $v__2 $v__3, at least $mostly_pct % of the time. Values should be parsed as datetimes.",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_column_values_to_be_in_type_list 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": 0.8},
            "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "type_list": {
                "schema": {"type": "array"},
                "value": ["type_a", "type_b", "type_c"],
            },
            "v__0": {"schema": {"type": "string"}, "value": "type_a"},
            "v__1": {"schema": {"type": "string"}, "value": "type_b"},
            "v__2": {"schema": {"type": "string"}, "value": "type_c"},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column value types must belong to this set: $v__0 $v__1 $v__2, at least $mostly_pct % of the time.",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_column_values_to_be_increasing 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": 0.8},
            "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            "parse_strings_as_datetimes": {
                "schema": {"type": "boolean"},
                "value": True,
            },
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "strictly": {"schema": {"type": "boolean"}, "value": 10},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must be strictly greater than previous values, at least $mostly_pct % of the time. Values should be parsed as datetimes.",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_column_values_to_be_json_parseable 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": 0.8},
            "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            "row_condition": {"schema": {"type": "string"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must be parseable as JSON, at least $mostly_pct % of the time.",
    },
    "value_type": "StringValueType",
}

snapshots["test_atomic_prescriptive_summary_expect_column_values_to_be_null 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": 0.8},
            "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            "row_condition": {"schema": {"type": "string"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must be null, at least $mostly_pct % of the time.",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_column_values_to_be_null_with_mostly_equals_1 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": 1.0},
            "mostly_pct": {"schema": {"type": "string"}, "value": None},
            "row_condition": {"schema": {"type": "string"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must be null.",
    },
    "value_type": "StringValueType",
}

snapshots["test_atomic_prescriptive_summary_expect_column_values_to_be_of_type 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": 0.8},
            "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "type_": {"schema": {"type": "string"}, "value": "my_type"},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must be of type $type_, at least $mostly_pct % of the time.",
    },
    "value_type": "StringValueType",
}

snapshots["test_atomic_prescriptive_summary_expect_column_values_to_be_unique 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": 0.8},
            "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            "row_condition": {"schema": {"type": "string"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must be unique, at least $mostly_pct % of the time.",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_column_values_to_match_json_schema 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "json_schema": {"schema": {"type": "object"}, "value": {"foo": "bar"}},
            "mostly": {"schema": {"type": "number"}, "value": 0.8},
            "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            "row_condition": {"schema": {"type": "string"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must match the following JSON Schema, at least $mostly_pct % of the time: $formatted_json",
    },
    "value_type": "StringValueType",
}

snapshots["test_atomic_prescriptive_summary_expect_column_values_to_match_regex 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": 0.8},
            "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            "regex": {"schema": {"type": "string"}, "value": "^superconductive$"},
            "row_condition": {"schema": {"type": "string"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must match this regular expression: $regex, at least $mostly_pct % of the time.",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_column_values_to_match_regex_list 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "match_on": {"schema": {"type": "string"}, "value": "all"},
            "mostly": {"schema": {"type": "number"}, "value": 0.8},
            "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            "regex_list": {
                "schema": {"type": "array"},
                "value": ["^superconductive$", "ge|great_expectations"],
            },
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "v__0": {"schema": {"type": "string"}, "value": "^superconductive$"},
            "v__1": {"schema": {"type": "string"}, "value": "ge|great_expectations"},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must match all of the following regular expressions: $v__0 $v__1, at least $mostly_pct % of the time.",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_column_values_to_match_strftime_format 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": 0.8},
            "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "strftime_format": {"schema": {"type": "string"}, "value": "%Y-%m"},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must match the following strftime format: $strftime_format, at least $mostly_pct % of the time.",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_column_values_to_not_be_in_set 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": 0.8},
            "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            "parse_strings_as_datetimes": {
                "schema": {"type": "boolean"},
                "value": None,
            },
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "v__0": {"schema": {"type": "string"}, "value": 1},
            "v__1": {"schema": {"type": "string"}, "value": 2},
            "v__2": {"schema": {"type": "string"}, "value": 3},
            "value_set": {"schema": {"type": "array"}, "value": [1, 2, 3]},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must not belong to this set: $v__0 $v__1 $v__2, at least $mostly_pct % of the time.",
    },
    "value_type": "StringValueType",
}

snapshots["test_atomic_prescriptive_summary_expect_column_values_to_not_be_null 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": 0.8},
            "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            "row_condition": {"schema": {"type": "string"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must not be null, at least $mostly_pct % of the time.",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_column_values_to_not_match_regex 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": 0.8},
            "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            "regex": {"schema": {"type": "string"}, "value": "^superconductive$"},
            "row_condition": {"schema": {"type": "string"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must not match this regular expression: $regex, at least $mostly_pct % of the time.",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_column_values_to_not_match_regex_list 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column": {"schema": {"type": "string"}, "value": "my_column"},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": 0.8},
            "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            "regex_list": {"schema": {"type": "array"}, "value": ["^a", "^b", "^c"]},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "v__0": {"schema": {"type": "string"}, "value": "^a"},
            "v__1": {"schema": {"type": "string"}, "value": "^b"},
            "v__2": {"schema": {"type": "string"}, "value": "^c"},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "$column values must not match any of the following regular expressions: $v__0 $v__1 $v__2, at least $mostly_pct % of the time.",
    },
    "value_type": "StringValueType",
}

snapshots["test_atomic_prescriptive_summary_expect_compound_columns_to_be_unique 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column_list": {
                "schema": {"type": "array"},
                "value": ["my_first_col", "my_second_col", "my_third_col"],
            },
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "ignore_row_if": {"schema": {"type": "string"}, "value": None},
            "mostly": {"schema": {"type": "number"}, "value": 0.8},
            "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            "row_condition": {"schema": {"type": "string"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "Values for given compound columns must be unique together, at least $mostly_pct % of the time: $column_list_0, $column_list_1, $column_list_2",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_multicolumn_values_to_be_unique 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column_list": {"schema": {"type": "array"}, "value": ["A", "B", "C"]},
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "ignore_row_if": {"schema": {"type": "string"}, "value": "foo"},
            "mostly": {"schema": {"type": "number"}, "value": 0.8},
            "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "v__0": {"schema": {"type": "string"}, "value": "A"},
            "v__1": {"schema": {"type": "string"}, "value": "B"},
            "v__2": {"schema": {"type": "string"}, "value": "C"},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "Values must be unique across columns, at least $mostly_pct % of the time: $column_list_0, $column_list_1, $column_list_2",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_select_column_values_to_be_unique_within_record 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column_list": {
                "schema": {"type": "array"},
                "value": ["my_first_column", "my_second_column"],
            },
            "condition_parser": {"schema": {"type": "string"}, "value": None},
            "ignore_row_if": {"schema": {"type": "string"}, "value": "foo"},
            "mostly": {"schema": {"type": "number"}, "value": 0.8},
            "mostly_pct": {"schema": {"type": "string"}, "value": "80"},
            "row_condition": {"schema": {"type": "string"}, "value": None},
            "v__0": {"schema": {"type": "string"}, "value": "my_first_column"},
            "v__1": {"schema": {"type": "string"}, "value": "my_second_column"},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "Values must be unique across columns, at least $mostly_pct % of the time: $column_list_0, $column_list_1",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_table_column_count_to_be_between 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "max_value": {"schema": {"type": "number"}, "value": None},
            "min_value": {"schema": {"type": "number"}, "value": 5},
            "strict_max": {"schema": {"type": "boolean"}, "value": None},
            "strict_min": {"schema": {"type": "boolean"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "Must have greater than or equal to $min_value columns.",
    },
    "value_type": "StringValueType",
}

snapshots["test_atomic_prescriptive_summary_expect_table_column_count_to_equal 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {"value": {"schema": {"type": "number"}, "value": 10}},
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "Must have exactly $value columns.",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_table_columns_to_match_ordered_list 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column_list": {"schema": {"type": "array"}, "value": ["a", "b", "c"]},
            "v__0": {"schema": {"type": "string"}, "value": "a"},
            "v__1": {"schema": {"type": "string"}, "value": "b"},
            "v__2": {"schema": {"type": "string"}, "value": "c"},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "Must have these columns in this order: $column_list_0, $column_list_1, $column_list_2",
    },
    "value_type": "StringValueType",
}

snapshots["test_atomic_prescriptive_summary_expect_table_columns_to_match_set 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "column_list": {"schema": {"type": "array"}, "value": ["a", "b", "c"]},
            "exact_match": {"schema": {"type": "boolean"}, "value": True},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "Must have exactly these columns (in any order): $column_list_0, $column_list_1, $column_list_2",
    },
    "value_type": "StringValueType",
}

snapshots["test_atomic_prescriptive_summary_expect_table_row_count_to_be_between 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "max_value": {"schema": {"type": "number"}, "value": None},
            "min_value": {"schema": {"type": "number"}, "value": 1},
            "strict_max": {"schema": {"type": "boolean"}, "value": None},
            "strict_min": {"schema": {"type": "boolean"}, "value": None},
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "Must have greater than or equal to $min_value rows.",
    },
    "value_type": "StringValueType",
}

snapshots["test_atomic_prescriptive_summary_expect_table_row_count_to_equal 1"] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {"value": {"schema": {"type": "number"}, "value": 10}},
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "Must have exactly $value rows.",
    },
    "value_type": "StringValueType",
}

snapshots[
    "test_atomic_prescriptive_summary_expect_table_row_count_to_equal_other_table 1"
] = {
    "name": "atomic.prescriptive.summary",
    "value": {
        "header": None,
        "params": {
            "other_table_name": {
                "schema": {"type": "string"},
                "value": {"schema": {"type": "string"}, "value": "other_table_name"},
            }
        },
        "schema": {"type": "com.superconductive.rendered.string"},
        "template": "Row count must equal the row count of table $other_table_name.",
    },
    "value_type": "StringValueType",
}
