import pytest

from great_expectations import DataContext
from great_expectations.core import ExpectationConfiguration


@pytest.fixture
def titanic_pandas_data_context_stats_enabled_and_expectation_suite_with_one_expectation(
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context: DataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    # create expectation suite
    suite = context.create_expectation_suite("my_expectation_suite")
    expectation = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "col1", "min_value": 1, "max_value": 2},
    )
    # NOTE Will 20211208 _add_expectation() method, although being called by an ExpectationSuite instance, is being
    # called within a fixture, and so will call the private method _add_expectation() and prevent it from sending a
    # usage_event.
    suite._add_expectation(expectation, send_usage_event=False)
    context.save_expectation_suite(suite)
    return context
