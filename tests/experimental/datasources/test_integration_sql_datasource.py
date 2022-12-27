import os
import pathlib
import sys
from typing import Any, Dict

import pytest

from great_expectations import DataContext
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.data_context.util import file_relative_path
from great_expectations.render import (
    AtomicDiagnosticRendererType,
    AtomicPrescriptiveRendererType,
)


@pytest.mark.integration
@pytest.mark.parametrize("include_rendered_content", [False, True])
def test_run_checkpoint_and_data_doc(empty_data_context, include_rendered_content):
    """An integration test for running checkpoints on sqlalchemy datasources.

    This test does the following:
    1. Creates a brand new datasource using a sqlalchemy backend.
    2. Creates an expectation suite associated with this datasource.
    3. Runs the checkpoint and validates that it ran correctly.
    4. Creates datadocs from the checkpoint run and checks that no errors occurred.
    """
    db_file = file_relative_path(
        __file__,
        os.path.join(
            "..",
            "..",
            "test_sets",
            "taxi_yellow_tripdata_samples",
            "sqlite",
            "yellow_tripdata.db",
        ),
    )
    context: DataContext = empty_data_context

    if include_rendered_content:
        context.variables.include_rendered_content.globally = True

    # Add sqlalchemy datasource.
    # The current method is called `add_postgres` it really should be `add_sql`.
    # See https://superconductive.atlassian.net/browse/GREAT-1400
    datasource = context.sources.add_postgres(
        name="test_datasource",
        connection_string=f"sqlite:///{db_file}",
    )

    # Add and configure a data asset
    table = "yellow_tripdata_sample_2019_01"
    split_col = "pickup_datetime"
    asset = (
        datasource.add_table_asset(
            name="my_asset",
            table_name=table,
        )
        .add_year_and_month_splitter(column_name=split_col)
        .add_sorters(["year", "month"])
    )

    # Define an expectation suite
    suite_name = "my_suite"
    context.create_expectation_suite(expectation_suite_name=suite_name)
    batch_request = asset.get_batch_request({"year": 2019, "month": 1})
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )
    validator.expect_table_row_count_to_be_between(0, 10000)
    validator.expect_column_max_to_be_between(
        column="passenger_count", min_value=1, max_value=7
    )
    validator.expect_column_median_to_be_between(
        column="passenger_count", min_value=1, max_value=4
    )
    validator.save_expectation_suite(discard_failed_expectations=False)

    # Configure and run a checkpoint
    checkpoint_config = {
        "class_name": "SimpleCheckpoint",
        "validations": [
            {"batch_request": batch_request, "expectation_suite_name": suite_name}
        ],
    }
    metadata = validator.active_batch.metadata
    checkpoint = SimpleCheckpoint(
        f"batch_with_year_{metadata['year']}_month_{metadata['month']}_{suite_name}",
        context,
        **checkpoint_config,
    )
    checkpoint_result = checkpoint.run()

    # Verify checkpoint runs successfully
    assert checkpoint_result._success, "Running expectation suite failed"
    number_of_runs = len(checkpoint_result.run_results)
    assert (
        number_of_runs == 1
    ), f"{number_of_runs} runs were done when we only expected 1"

    # Grab the validation result and verify it is correct
    result = checkpoint_result["run_results"][
        list(checkpoint_result["run_results"].keys())[0]
    ]
    validation_result = result["validation_result"]
    assert validation_result.success

    expected_metric_values = {
        "expect_table_row_count_to_be_between": {
            "value": 10000,
            "rendered_template": "Must have greater than or equal to $min_value and less than or equal to $max_value rows.",
        },
        "expect_column_max_to_be_between": {
            "value": 6,
            "rendered_template": "$column maximum value must be greater than or equal to $min_value and less than or equal to $max_value.",
        },
        "expect_column_median_to_be_between": {
            "value": 1,
            "rendered_template": "$column median must be greater than or equal to $min_value and less than or equal to $max_value.",
        },
    }
    assert len(validation_result.results) == 3

    for r in validation_result.results:
        assert r.success
        assert (
            r.result["observed_value"]
            == expected_metric_values[r.expectation_config.expectation_type]["value"]
        )

        if include_rendered_content:
            # There is a prescriptive atomic renderer on r.expectation_config
            num_prescriptive_renderer = len(r.expectation_config.rendered_content)
            assert (
                num_prescriptive_renderer == 1
            ), f"Expected exactly 1 rendered content, found {num_prescriptive_renderer}"
            rendered_content = r.expectation_config.rendered_content[0]
            assert rendered_content.name == AtomicPrescriptiveRendererType.SUMMARY
            assert (
                rendered_content.value.template
                == expected_metric_values[r.expectation_config.expectation_type][
                    "rendered_template"
                ]
            )

            # There is a diagnostic atomic renderer on r, a validation result result.
            num_diagnostic_render = len(r.rendered_content)
            assert (
                num_diagnostic_render == 1
            ), f"Expected 1 diagnostic renderer, found {num_diagnostic_render}"
            diagnostic_renderer = r.rendered_content[0]
            assert (
                diagnostic_renderer.name == AtomicDiagnosticRendererType.OBSERVED_VALUE
            )
            assert (
                diagnostic_renderer.value.schema["type"]
                == "com.superconductive.rendered.string"
            )
        else:
            assert r.rendered_content is None
            assert r.expectation_config.rendered_content is None

    # Rudimentary test for data doc generation
    docs_dict = context.build_data_docs()
    assert "local_site" in docs_dict, "build_data_docs returned dictionary has changed"
    assert docs_dict["local_site"].startswith(
        "file://"
    ), "build_data_docs returns file path in unexpected form"
    path = docs_dict["local_site"][7:]
    with open(path) as f:
        data_doc_index = f.read()

    # Checking for ge-success-icon tests the result table was generated and it was populated with a successful run.
    assert "ge-success-icon" in data_doc_index
    assert "ge-failed-icon" not in data_doc_index


@pytest.mark.integration
@pytest.mark.slow
def test_run_data_assistant_and_checkpoint(empty_data_context):
    """Test using data assistants to create expectation suite and run checkpoint"""
    path = pathlib.Path(
        "../../test_sets/taxi_yellow_tripdata_samples/sqlite/yellow_tripdata.db"
    )
    (
        data_assistant_result,
        checkpoint_result,
    ) = configure_and_run_data_assistant_checkpoint(
        context=empty_data_context,
        sqlite_db_file_relative_path=str(path),
        table_name="yellow_tripdata_sample_2019_01",
        date_splitter_column="pickup_datetime",
        batch_request_options={"year": 2019, "month": 1},
    )
    # Assert only 1 batch was processed
    batch_num = len(
        data_assistant_result._batch_id_to_batch_identifier_display_name_map
    )
    assert batch_num == 1, f"Only expected 1 batch but found {batch_num}"

    # We assert the data assistant successfully generated expectations.
    # We don't care about the exact number since that may change as data assistants evolve.
    expectation_num = len(data_assistant_result.expectation_configurations)
    assert expectation_num > 100
    assert checkpoint_result.success, "Running expectation suite failed"
    # Verify that the number of checkpoint validations is the number of expectations generated by the data assistant
    assert (
        len(
            checkpoint_result["run_results"][
                list(checkpoint_result["run_results"].keys())[0]
            ]["validation_result"].results
        )
        == expectation_num
    )


@pytest.mark.integration
@pytest.mark.slow
def test_run_multibatch_data_assistant_and_checkpoint(empty_data_context):
    """Test using data assistants to create expectation suite and run checkpoint"""
    db_path = pathlib.Path(
        "../../test_sets/taxi_yellow_tripdata_samples/sqlite/yellow_tripdata_sample_2020_all_months_combined.db"
    )
    (
        data_assistant_result,
        checkpoint_result,
    ) = configure_and_run_data_assistant_checkpoint(
        context=empty_data_context,
        sqlite_db_file_relative_path=str(db_path),
        table_name="yellow_tripdata_sample_2020",
        date_splitter_column="pickup_datetime",
        batch_request_options={"year": 2020},
    )
    # Assert multiple batches were processed
    batch_num = len(
        data_assistant_result._batch_id_to_batch_identifier_display_name_map
    )
    assert batch_num == 12, f"Expected exactly 3 batches but found {batch_num}"

    expectation_num = len(data_assistant_result.expectation_configurations)
    assert expectation_num > 100
    assert checkpoint_result.success, "Running expectation suite failed"
    # Verify that the number of checkpoint validations is the number of expectations generated by the data assistant
    assert (
        len(
            checkpoint_result["run_results"][
                list(checkpoint_result["run_results"].keys())[0]
            ]["validation_result"].results
        )
        == expectation_num
    )


def configure_and_run_data_assistant_checkpoint(
    context: DataContext,
    sqlite_db_file_relative_path: str,
    table_name: str,
    date_splitter_column: str,
    batch_request_options: Dict[str, Any],
):
    db_file = file_relative_path(__file__, sqlite_db_file_relative_path)
    # Add sqlalchemy datasource.
    # The current method is called `add_postgres` it really should be `add_sql`.
    # See https://superconductive.atlassian.net/browse/GREAT-1400
    datasource = context.sources.add_postgres(
        name="test_datasource",
        connection_string=f"sqlite:///{db_file}",
    )

    # Add and configure a data asset
    table = table_name
    split_col = date_splitter_column
    asset = (
        datasource.add_table_asset(
            name="my_asset",
            table_name=table,
        )
        .add_year_and_month_splitter(column_name=split_col)
        .add_sorters(["year", "month"])
    )

    # Setup an expectation suite using the onboarding data assistant
    expectation_suite_name = "my_onboarding_assistant_suite"
    context.create_expectation_suite(
        expectation_suite_name=expectation_suite_name, overwrite_existing=True
    )
    batch_request = asset.get_batch_request(batch_request_options)
    data_assistant_result = context.assistants.onboarding.run(
        batch_request=batch_request,
        numeric_columns_rule={
            "estimator": "exact",
            "random_seed": 2022080401,
        },
        # We exclude congestion_surcharge due to this bug:
        # https://greatexpectations.atlassian.net/browse/GREAT-1465
        exclude_column_names=["congestion_surcharge"],
    )
    expectation_suite = data_assistant_result.get_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    context.save_expectation_suite(
        expectation_suite=expectation_suite, discard_failed_expectations=False
    )

    # Run a checkpoint
    checkpoint_config = {
        "class_name": "SimpleCheckpoint",
        "validations": [
            {
                "batch_request": batch_request,
                "expectation_suite_name": expectation_suite_name,
            }
        ],
    }
    checkpoint = SimpleCheckpoint(
        f"yellow_tripdata_sample_{expectation_suite_name}",
        context,
        **checkpoint_config,
    )
    checkpoint_result = checkpoint.run()

    return data_assistant_result, checkpoint_result
