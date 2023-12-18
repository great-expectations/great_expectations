"""
Some form of these tests will live in doc snippets for our quickstart guides. This is here for now to gate closing our
current epic that begins refactoring how we author expectation suites for V1.
"""

import great_expectations as gx
import great_expectations.expectations as gxe


def test_csv_batch_validate():
    context = gx.get_context()
    batch = context.sources.pandas_default.read_csv(
        "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
    )
    expectation = gxe.ExpectColumnValuesToNotBeNull(
        "pu_datetime",
        notes="These are filtered out upstream, because the entire record is garbage if there is no pu_datetime",
    )
    result = batch.validate(expectation)
    assert not result.success
    expectation.mostly = 0.8
    result = batch.validate(expectation)
    assert result.success
    suite = context.add_expectation_suite("quickstart")
    suite.add(expectation)
    suite.add(
        gxe.ExpectColumnValuesToBeBetween("passenger_count", min_value=1, max_value=6)
    )
    suite_result = batch.validate(suite)
    assert suite_result
