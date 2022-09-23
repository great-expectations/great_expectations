import pytest

from great_expectations.data_context import DataContext


@pytest.mark.integration
def test_onboarding_data_assistant_runner_top_level_kwargs(
    bobby_columnar_table_multi_batch_probabilistic_data_context,
):
    context: DataContext = bobby_columnar_table_multi_batch_probabilistic_data_context
    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }
    context.assistants.onboarding.run(
        batch_request=batch_request,
        include_column_names=["passenger_count"],
        exclude_column_names=["tpep_pickup_datetime"],
        include_column_name_suffixes=["amount"],
        exclude_column_name_suffixes=["ID"],
        cardinality_limit_mode="very_few",
    )
    with pytest.raises(TypeError):
        context.assistants.onboarding.run(
            batch_request=batch_request, non_existant_parameter="break_this"
        )
