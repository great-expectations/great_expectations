import datetime
import os
import shutil
from typing import Any, Dict, List, cast

import pandas as pd
import pytest
from ruamel.yaml import YAML

from great_expectations import DataContext
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.util import file_relative_path
from great_expectations.datasource import DataConnector, Datasource
from great_expectations.rule_based_profiler.profiler import Profiler
from great_expectations.validator.validation_graph import MetricConfiguration
from great_expectations.validator.validator import Validator

yaml = YAML()

# TODO: Move these tests to integration tests


@pytest.fixture
def alice_columnar_table_single_batch_context(
    monkeypatch,
    empty_data_context,
    alice_columnar_table_single_batch,
):
    context: DataContext = empty_data_context
    monkeypatch.chdir(context.root_directory)
    data_relative_path: str = "../data"
    data_path: str = os.path.join(context.root_directory, data_relative_path)
    os.makedirs(data_path, exist_ok=True)

    # Copy data
    filename: str = alice_columnar_table_single_batch["sample_data_relative_path"]
    shutil.copy(
        file_relative_path(__file__, f"data/{filename}"),
        str(os.path.join(data_path, filename)),
    )

    data_connector_base_directory: str = "./"
    monkeypatch.setenv("base_directory", data_connector_base_directory)
    monkeypatch.setenv("data_fixtures_root", data_relative_path)

    datasource_name: str = "alice_columnar_table_single_batch_datasource"
    data_connector_name: str = "alice_columnar_table_single_batch_data_connector"
    data_asset_name: str = "alice_columnar_table_single_batch_data_asset"
    datasource_config: str = fr"""
class_name: Datasource
module_name: great_expectations.datasource
execution_engine:
  module_name: great_expectations.execution_engine
  class_name: PandasExecutionEngine
data_connectors:
  {data_connector_name}:
    class_name: ConfiguredAssetFilesystemDataConnector
    assets:
      {data_asset_name}:
        module_name: great_expectations.datasource.data_connector.asset
        group_names:
          - filename
        pattern: (.*)\.csv
        reader_options:
          delimiter: ","
        class_name: Asset
        base_directory: $data_fixtures_root
        glob_directive: "*.csv"
    base_directory: $base_directory
    module_name: great_expectations.datasource.data_connector
        """

    context.add_datasource(name=datasource_name, **yaml.load(datasource_config))

    assert context.list_datasources() == [
        {
            "class_name": "Datasource",
            "data_connectors": {
                data_connector_name: {
                    "assets": {
                        data_asset_name: {
                            "base_directory": data_relative_path,
                            "class_name": "Asset",
                            "glob_directive": "*.csv",
                            "group_names": ["filename"],
                            "module_name": "great_expectations.datasource.data_connector.asset",
                            "pattern": "(.*)\\.csv",
                        }
                    },
                    "base_directory": data_connector_base_directory,
                    "class_name": "ConfiguredAssetFilesystemDataConnector",
                    "module_name": "great_expectations.datasource.data_connector",
                }
            },
            "execution_engine": {
                "class_name": "PandasExecutionEngine",
                "module_name": "great_expectations.execution_engine",
            },
            "module_name": "great_expectations.datasource",
            "name": datasource_name,
        }
    ]
    return context


@pytest.fixture
def bobby_columnar_table_multi_batch_context(
    tmp_path_factory,
    monkeypatch,
) -> DataContext:
    """
    # TODO: <Alex>ALEX -- Provide DocString</Alex>
    """
    # Reenable GE_USAGE_STATS
    monkeypatch.delenv("GE_USAGE_STATS")

    project_path: str = str(tmp_path_factory.mktemp("taxi_data_context"))
    context_path: str = os.path.join(project_path, "great_expectations")
    os.makedirs(os.path.join(context_path, "expectations"), exist_ok=True)
    data_path: str = os.path.join(context_path, "..", "data")
    os.makedirs(os.path.join(data_path), exist_ok=True)
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(
                "..",
                "integration",
                "fixtures",
                "yellow_trip_data_pandas_fixture",
                "great_expectations",
                "great_expectations.yml",
            ),
        ),
        str(os.path.join(context_path, "great_expectations.yml")),
    )
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(
                "..",
                "test_sets",
                "taxi_yellow_trip_data_samples",
                "random_subsamples",
                "yellow_trip_data_7500_lines_sample_2019-01.csv",
            ),
        ),
        str(
            os.path.join(
                context_path, "..", "data", "yellow_trip_data_sample_2019-01.csv"
            )
        ),
    )
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(
                "..",
                "test_sets",
                "taxi_yellow_trip_data_samples",
                "random_subsamples",
                "yellow_trip_data_8500_lines_sample_2019-02.csv",
            ),
        ),
        str(
            os.path.join(
                context_path, "..", "data", "yellow_trip_data_sample_2019-02.csv"
            )
        ),
    )
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(
                "..",
                "test_sets",
                "taxi_yellow_trip_data_samples",
                "random_subsamples",
                "yellow_trip_data_9000_lines_sample_2019-03.csv",
            ),
        ),
        str(
            os.path.join(
                context_path, "..", "data", "yellow_trip_data_sample_2019-03.csv"
            )
        ),
    )

    context: DataContext = DataContext(context_root_dir=context_path)
    assert context.root_directory == context_path

    return context


def test_alice_columnar_table_single_batch_batches_are_accessible(
    monkeypatch,
    alice_columnar_table_single_batch_context,
    alice_columnar_table_single_batch,
):
    """
    What does this test and why?
    Batches created in the multibatch_generic_csv_generator fixture should be available using the
    multibatch_generic_csv_generator_context
    This test most likely duplicates tests elsewhere, but it is more of a test of the configurable fixture.
    """

    context: DataContext = alice_columnar_table_single_batch_context

    datasource_name: str = "alice_columnar_table_single_batch_datasource"
    data_connector_name: str = "alice_columnar_table_single_batch_data_connector"
    data_asset_name: str = "alice_columnar_table_single_batch_data_asset"

    datasource: Datasource = cast(Datasource, context.datasources[datasource_name])
    data_connector: DataConnector = datasource.data_connectors[data_connector_name]

    file_list: List[str] = [
        alice_columnar_table_single_batch["sample_data_relative_path"]
    ]

    assert (
        data_connector._get_data_reference_list_from_cache_by_data_asset_name(
            data_asset_name=data_asset_name
        )
        == file_list
    )

    batch_request_1: BatchRequest = BatchRequest(
        datasource_name=datasource_name,
        data_connector_name=data_connector_name,
        data_asset_name=data_asset_name,
        data_connector_query={
            "index": -1,
        },
    )
    # Should give most recent batch
    validator_1: Validator = context.get_validator(
        batch_request=batch_request_1,
        create_expectation_suite_with_name="my_expectation_suite_name_1",
    )
    metric_max: int = validator_1.get_metric(
        MetricConfiguration("column.max", metric_domain_kwargs={"column": "event_type"})
    )
    assert metric_max == 73


def test_alice_profiler_user_workflow_single_batch(
    alice_columnar_table_single_batch_context,
    alice_columnar_table_single_batch,
):
    # Load data context
    data_context: DataContext = alice_columnar_table_single_batch_context
    # Load profiler configs & loop (run tests for each one)
    yaml_config: str = alice_columnar_table_single_batch["profiler_config"]

    # Instantiate Profiler
    profiler_config: dict = yaml.load(yaml_config)

    datasource_name: str = "alice_columnar_table_single_batch_datasource"
    data_connector_name: str = "alice_columnar_table_single_batch_data_connector"
    data_asset_name: str = "alice_columnar_table_single_batch_data_asset"

    validator: Validator = data_context.get_validator(
        datasource_name=datasource_name,
        data_connector_name=data_connector_name,
        data_asset_name=data_asset_name,
        create_expectation_suite_with_name=alice_columnar_table_single_batch[
            "expected_expectation_suite_name"
        ],
    )

    profiler: Profiler = Profiler(
        validator=validator,
        profiler_config=profiler_config,
        data_context=data_context,
    )

    expectation_suite: ExpectationSuite = profiler.profile(
        expectation_suite_name=alice_columnar_table_single_batch[
            "expected_expectation_suite_name"
        ],
    )

    assert (
        expectation_suite
        == alice_columnar_table_single_batch["expected_expectation_suite"]
    )


def test_bobby_columnar_table_multi_batch_batches_are_accessible(
    monkeypatch,
    bobby_columnar_table_multi_batch_context,
    bobby_columnar_table_multi_batch,
):
    """
    # TODO: <Alex>ALEX</Alex>
    What does this test and why?
    """

    context: DataContext = bobby_columnar_table_multi_batch_context

    datasource_name: str = "taxi_pandas"
    data_connector_name: str = "monthly"
    data_asset_name: str = "my_reports"

    datasource: Datasource = cast(Datasource, context.datasources[datasource_name])
    data_connector: DataConnector = datasource.data_connectors[data_connector_name]

    file_list: List[str] = [
        "yellow_trip_data_sample_2019-01.csv",
        "yellow_trip_data_sample_2019-02.csv",
        "yellow_trip_data_sample_2019-03.csv",
    ]

    assert (
        data_connector._get_data_reference_list_from_cache_by_data_asset_name(
            data_asset_name=data_asset_name
        )
        == file_list
    )

    batch_request_latest: BatchRequest = BatchRequest(
        datasource_name=datasource_name,
        data_connector_name=data_connector_name,
        data_asset_name=data_asset_name,
        data_connector_query={
            "index": -1,
        },
    )
    validator_latest: Validator = context.get_validator(
        batch_request=batch_request_latest,
        create_expectation_suite_with_name="my_expectation_suite_name_1",
    )

    metric_configuration_arguments: Dict[str, Any] = {
        "metric_name": "table.row_count",
        "metric_domain_kwargs": {
            "batch_id": validator_latest.active_batch_id,
        },
        "metric_value_kwargs": None,
        "metric_dependencies": None,
    }
    metric_value: int = validator_latest.get_metric(
        metric=MetricConfiguration(**metric_configuration_arguments)
    )
    assert metric_value == 9000

    # noinspection PyUnresolvedReferences
    pickup_datetime: datetime.datetime = pd.to_datetime(
        validator_latest.head(n_rows=1)["pickup_datetime"][0]
    ).to_pydatetime()
    month: int = pickup_datetime.month
    assert month == 3


def test_bobby_profiler_user_workflow_multi_batch(
    bobby_columnar_table_multi_batch_context,
    bobby_columnar_table_multi_batch,
):
    # Load data context
    data_context: DataContext = bobby_columnar_table_multi_batch_context
    # Load profiler configs & loop (run tests for each one)
    yaml_config: str = bobby_columnar_table_multi_batch["profiler_config"]

    # Instantiate Profiler
    profiler_config: dict = yaml.load(yaml_config)

    datasource_name: str = "taxi_pandas"
    data_connector_name: str = "monthly"
    data_asset_name: str = "my_reports"

    validator: Validator = data_context.get_validator(
        datasource_name=datasource_name,
        data_connector_name=data_connector_name,
        data_asset_name=data_asset_name,
        data_connector_query={
            "index": -1,
        },
        create_expectation_suite_with_name=bobby_columnar_table_multi_batch[
            "expected_expectation_suite_name"
        ],
    )

    profiler: Profiler = Profiler(
        validator=validator,
        profiler_config=profiler_config,
        data_context=data_context,
    )

    expectation_suite: ExpectationSuite = profiler.profile(
        expectation_suite_name=bobby_columnar_table_multi_batch[
            "expected_expectation_suite_name"
        ],
    )

    assert (
        expectation_suite
        == bobby_columnar_table_multi_batch["expected_expectation_suite"]
    )
