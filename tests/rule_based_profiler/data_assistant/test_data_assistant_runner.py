import pytest

from great_expectations.data_context import DataContext
from great_expectations.rule_based_profiler.data_assistant_result import (
    DataAssistantResult,
)
from great_expectations.rule_based_profiler.domain import MetricDomainTypes


@pytest.mark.integration
def test_onboarding_data_assistant_runner_top_level_kwargs_allowed(
    bobby_columnar_table_multi_batch_probabilistic_data_context,
):
    context: DataContext = bobby_columnar_table_multi_batch_probabilistic_data_context
    batch_request = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }
    try:
        context.assistants.onboarding.run(
            batch_request=batch_request,
            include_column_names=["passenger_count"],
            exclude_column_names=["tpep_pickup_datetime"],
            include_column_name_suffixes=["amount"],
            exclude_column_name_suffixes=["ID"],
            cardinality_limit_mode="very_few",
        )
    except Exception as exc:
        assert (False, f"context.assistants.onboarding.run raise an exception '{exc}'")

    with pytest.raises(TypeError) as e:
        context.assistants.onboarding.run(
            batch_request=batch_request, non_existent_parameter="break_this"
        )
    assert (
        e.value.args[0]
        == "run() got an unexpected keyword argument 'non_existent_parameter'"
    )


@pytest.mark.integration
def test_onboarding_data_assistant_runner_top_level_kwargs_behavior(
    bobby_columnar_table_multi_batch_probabilistic_data_context,
):
    context: DataContext = bobby_columnar_table_multi_batch_probabilistic_data_context
    batch_request = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    # Ensure that no columns with the suffix "ID" are used when override is passed at the top-level API
    data_assistant_result: DataAssistantResult = context.assistants.onboarding.run(
        batch_request=batch_request,
        exclude_column_name_suffixes=["ID"],
    )
    columns_used = {
        domain["domain_kwargs"]["column"]
        for domain in data_assistant_result.metrics_by_domain.keys()
        if domain["domain_type"] == MetricDomainTypes.COLUMN
    }
    assert not any([column_name.endswith("ID") for column_name in columns_used])
