import re

import pandas as pd
import pytest

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.data_context import DataContext
from great_expectations.rule_based_profiler.data_assistant_result import (
    DataAssistantResult,
)


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
        raise AssertionError(
            f"context.assistants.onboarding.run raise an exception '{exc}'"
        )

    with pytest.raises(
        TypeError,
        match=re.escape(
            "run() got an unexpected keyword argument 'non_existent_parameter'"
        ),
    ):
        context.assistants.onboarding.run(
            batch_request=batch_request, non_existent_parameter="break_this"
        )


@pytest.mark.integration
def test_onboarding_data_assistant_runner_top_level_kwargs_override(
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


@pytest.mark.integration
def test_onboarding_data_assistant_runner_top_level_kwargs_explicit_none(
    data_context_with_datasource_pandas_engine,
):
    context: DataContext = data_context_with_datasource_pandas_engine

    df = pd.DataFrame(
        {
            "user_id": [1, 2, 3, 4, 5, 6],
            "total_spend": [131.24, 42.21, 516.55, 0.00, 12351.92, 0.52],
        }
    )

    batch_request = RuntimeBatchRequest(
        datasource_name="my_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="users_df",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "onboarding_test"},
    )

    # Ensure that user_id is still excluded from categorical_columns_rule when we pass an explicit None to
    # exclude_column_name_suffixes, because CategoricalColumnBuilder defaults will continue to be respected.
    data_assistant_result: DataAssistantResult = context.assistants.onboarding.run(
        batch_request=batch_request,
        exclude_column_name_suffixes=None,
    )
    categorical_columns_used = {
        domain["domain_kwargs"]["column"]
        for domain in data_assistant_result.metrics_by_domain.keys()
        if domain["domain_type"] == MetricDomainTypes.COLUMN
        and domain["rule_name"] == "categorical_columns_rule"
    }

    assert "user_id" not in categorical_columns_used
