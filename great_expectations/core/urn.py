from pyparsing import Combine, LineEnd, Literal, Optional, Suppress, Word, alphanums

urn_word = Word(alphanums + "_$?=%.&,")
ge_metrics_urn = Combine(
    Suppress(Literal("urn:great_expectations:"))
    + Literal("metrics").setResultsName("urn_type")
    + Suppress(":")
    + urn_word.setResultsName("run_id")
    + Suppress(":")
    + urn_word.setResultsName("expectation_suite_name")
    + Suppress(":")
    + urn_word.setResultsName("metric_name")
    + Optional(Suppress(":") + urn_word.setResultsName("metric_kwargs"))
    + Suppress(LineEnd())
)
ge_validations_urn = Combine(
    Suppress(Literal("urn:great_expectations:"))
    + Literal("validations").setResultsName("urn_type")
    + Suppress(":")
    + urn_word.setResultsName("expectation_suite_name")
    + Suppress(":")
    + urn_word.setResultsName("metric_name")
    + Optional(Suppress(":") + urn_word.setResultsName("metric_kwargs"))
    + Suppress(LineEnd())
)
ge_stores_urn = Combine(
    Suppress(Literal("urn:great_expectations:"))
    + Literal("stores").setResultsName("urn_type")
    + Suppress(":")
    + urn_word.setResultsName("store_name")
    + Suppress(":")
    + urn_word.setResultsName("metric_name")
    + Optional(Suppress(":") + urn_word.setResultsName("metric_kwargs"))
    + Suppress(LineEnd())
)


"""
# Design for b
#works today, but nobody uses it (we think)
ExpectationConfiguration(
  expectation_type="expect_table_row_count_to_be_between",
  kwargs = {
    "min_value": {"$PARAMETER":
    "urn:great_expectations:run_id:expectation_suite_name:expect_table_row_count_to_be_between
    .observed_value * 0.9"},  # urn:ge:stores:my_store:foo:bar
    "max_value": {"$PARAMETER": "$prev_rows * 1.1"},
  }
)
#works today, but nobody uses it (we think)
ExpectationConfiguration(
  expectation_type="expect_table_row_count_to_be_between",
  kwargs = {
    "min_value": {"$PARAMETER":
    "urn:great_expectations:stores:my_metrics_store:more_stuff
    :expect_table_row_count_to_be_between.observed_value * 0.9"},  # urn:ge:stores:my_store:foo:bar
    "max_value": {"$PARAMETER": "$prev_rows * 1.1"},
  }
)
# Shortcut batch query
ExpectationConfiguration(
  expectation_type="expect_table_row_count_to_be_between",
  kwargs = {
    "min_value": {"$PARAMETER": "last_batch_rows * 0.9"},  # urn:ge:stores:my_store:foo:bar
    "max_value": {"$PARAMETER": "last_batch_rows * 1.1"},
    "_evaluation_parameters": {
      "last_batch_rows": { # MetricConfiguration
        "metric_name": "table.row_count"
        "metric_domain_kwargs": {
          "batch": "$PREV_BATCH"
        }
      }
    }
  }
)
# Full batch query
ExpectationConfiguration(
  expectation_type="expect_table_row_count_to_be_between",
  kwargs = {
    "min_value": {"$PARAMETER": "$prev_rows * 0.9"},  # urn:ge:stores:my_store:foo:bar =
    $STORE.store.l;akjsdfkl;as
    "max_value": {"$PARAMETER": "$prev_rows * 1.1"},
    "_evaluation_parameters": {
      "prev_rows": {
        "metric_name": "table.row_count"
        "metric_domain_kwargs": {
          "batch": {
            "year": 2020
            "month": 8
            "day": 5
          }
        }
      }
    }
  }
)



# Magic Option - 1
ExpectationConfiguration(
  expectation_type="expect_table_row_count_to_be_between",
  kwargs = {
    "min_value": {"$PARAMETER": "$PREV_BATCH.observed_value * 0.9},
    "max_value": {"$PARAMETER": "$PREV.observed_value * 1.1"},
  }
)

$PREV is a special evaluation parameter variable that refers to the name
# Option 0 - Not implemented
"max_value": {"$PARAMETER": "$PREV * 1.1"},  # valid, uses same expectation type AND same domain
and value kwargs
# Option 1
"max_value": {"$PARAMETER": "$PREV.observed_value * 1.1"},  # valid, uses same expectation type
AND same domain and value kwargs
"mostly": {"$PARAMETER": "$PREV.unexpected_percent * 1.1"},  # valid, uses same expectation type
AND same domain and value kwargs
# Option 2
"min_value": {"$PARAMETER": "$PREV_BATCH.expect_table_row_count_to_be_between.observed_value *
0.9}, # valid, uses specified expectation type, and same domain and value kwargs
# Option 3
"max_value": {"$PARAMETER": "$prev_rows * 1.1"},
"_evaluation_parameters": {  # general option to fully specify is available.
  "prev_rows": {
    "metric_name": "table.row_count"
    "metric_domain_kwargs": {
      "batch": {
        "year": 2020
        "month": 8
        "day": 5
      }
    }
  }

# Magic Option - 2
ExpectationConfiguration(
  expectation_type="expect_table_row_count_to_be_between",
  kwargs = {
    "min_value": {"$PARAMETER": "$PREV.expect_table_row_count_to_be_between.observed_value * 0.9},
    "min_value": {"$PARAMETER": "$PREV_BATCH_EXPECTATION.observed_value * 0.9},
    "max_value": {"$PARAMETER": "$PREV_BATCH.table.row_count.observed_value * 1.1"},
  }
)
"""

ge_magic_batch_expectation_urn = Combine(
    Literal("$PREV").setResultsName("urn_type")
    + Suppress(Literal("."))
    + (Literal("observed_value") | Literal("unexpected_percent")).setResultsName(
        "metric_suffix"
    )
)

ge_magic_batch_urn = Combine(
    Literal("$PREV_BATCH").setResultsName("urn_type")
    + Suppress(Literal("."))
    + (
        # Do not allow urn_word here because we reserve dot. Should we use : instead?
        Word(alphanums + "_").setResultsName("metric_name")
    )
    + Suppress(Literal("."))
    + (Literal("observed_value") | Literal("unexpected_percent")).setResultsName(
        "metric_suffix"
    )
)

ge_urn = (
    ge_metrics_urn
    | ge_validations_urn
    | ge_stores_urn
    | ge_magic_batch_urn
    | ge_magic_batch_expectation_urn
)
