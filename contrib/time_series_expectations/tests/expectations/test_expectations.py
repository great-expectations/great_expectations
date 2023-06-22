from time_series_expectations.expectations.expect_batch_row_count_to_match_prophet_date_model import (
    ExpectBatchRowCountToMatchProphetDateModel,
)
from time_series_expectations.expectations.expect_column_max_to_match_prophet_date_model import (
    ExpectColumnMaxToMatchProphetDateModel,
)
from time_series_expectations.expectations.expect_column_pair_values_to_match_prophet_date_model import (
    ExpectColumnPairValuesToMatchProphetDateModel,
)


def test_ExpectColumnPairValuesToMatchProphetDateModel():
    ExpectColumnPairValuesToMatchProphetDateModel().run_diagnostics()

    raise AssertionError()


def test_ExpectBatchRowCountToMatchProphetDateModel():
    ExpectBatchRowCountToMatchProphetDateModel().run_diagnostics()


def test_ExpectColumnMaxToMatchProphetDateModel():
    ExpectColumnMaxToMatchProphetDateModel().run_diagnostics()
