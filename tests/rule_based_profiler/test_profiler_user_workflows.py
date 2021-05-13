import os
import shutil

import pytest
from ruamel.yaml import YAML

from great_expectations import DataContext
from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.util import file_relative_path
from great_expectations.profiler.profiler import Profiler
from great_expectations.validator.validation_graph import MetricConfiguration

yaml = YAML()

# TODO: Move these tests to integration tests


@pytest.fixture
def alice_columnar_table_single_batch_context(
    monkeypatch, empty_data_context, alice_columnar_table_single_batch
):
    context: DataContext = empty_data_context
    monkeypatch.chdir(context.root_directory)
    data_relative_path = "../data"
    data_path = os.path.join(context.root_directory, data_relative_path)
    os.makedirs(data_path, exist_ok=True)

    # Copy data
    filename: str = alice_columnar_table_single_batch["sample_data_relative_path"]
    shutil.copy(
        file_relative_path(__file__, f"data/{filename}"),
        str(os.path.join(data_path, filename)),
    )

    data_connector_base_directory = "./"
    monkeypatch.setenv("base_directory", data_connector_base_directory)
    monkeypatch.setenv("data_fixtures_root", data_relative_path)

    datasource_name = "alice_columnar_table_single_batch_datasource"
    data_connector_name = "alice_columnar_table_single_batch_data_connector"
    asset_name = "alice_columnar_table_single_batch_data_asset"
    datasource_config = fr"""
class_name: Datasource
module_name: great_expectations.datasource
execution_engine:
  module_name: great_expectations.execution_engine
  class_name: PandasExecutionEngine
data_connectors:
  {data_connector_name}:
    class_name: ConfiguredAssetFilesystemDataConnector
    assets:
      {asset_name}:
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
                        asset_name: {
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
    data_relative_path: str = "../data"
    data_path: str = os.path.join(context.root_directory, data_relative_path)
    datasource_name: str = "alice_columnar_table_single_batch_datasource"
    data_connector_name: str = "alice_columnar_table_single_batch_data_connector"
    asset_name: str = "alice_columnar_table_single_batch_data_asset"

    datasource = context.datasources[datasource_name]

    data_connector = datasource.data_connectors[data_connector_name]

    file_list = [alice_columnar_table_single_batch["sample_data_relative_path"]]

    assert (
        data_connector._get_data_reference_list_from_cache_by_data_asset_name(
            data_asset_name=asset_name
        )
        == file_list
    )

    batch_request_1 = BatchRequest(
        datasource_name=datasource_name,
        data_connector_name=data_connector_name,
        data_asset_name=asset_name,
        data_connector_query={
            "index": -1,
        },
    )
    # Should give most recent batch
    validator_1 = context.get_validator(
        batch_request=batch_request_1,
        create_expectation_suite_with_name="my_expectation_suite_name_1",
    )
    metric_max = validator_1.get_metric(
        MetricConfiguration("column.max", metric_domain_kwargs={"column": "event_type"})
    )
    assert metric_max == 73


def test_alice_profiler_user_workflow_single_batch(
    alice_columnar_table_single_batch_context, alice_columnar_table_single_batch
):
    # Load data context
    data_context: DataContext = alice_columnar_table_single_batch_context
    # Load profiler configs & loop (run tests for each one)
    for profiler_config in alice_columnar_table_single_batch["profiler_configs"]:

        # Instantiate Profiler
        full_profiler_config_dict = yaml.load(profiler_config)
        rule_configs = full_profiler_config_dict.get("rules")
        variable_configs = full_profiler_config_dict.get("variables")
        profiler = Profiler(
            rule_configs=rule_configs,
            variable_configs=variable_configs,
            data_context=data_context,
        )

        # Profiler.profile()
        validator = data_context.get_validator(
            datasource_name="alice_columnar_table_single_batch_datasource",
            data_connector_name="alice_columnar_table_single_batch_data_connector",
            data_asset_name="alice_columnar_table_single_batch_data_asset",
            create_expectation_suite_with_name=alice_columnar_table_single_batch[
                "expected_expectation_suite_name"
            ],
        )
        suite = profiler.profile(
            validator=validator,
            data_context=data_context,
            expectation_suite_name=alice_columnar_table_single_batch[
                "expected_expectation_suite_name"
            ],
        )

        assert suite == alice_columnar_table_single_batch["expected_expectation_suite"]
