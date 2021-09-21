import datetime
import os
from typing import List, Optional

import pandas as pd
import pytest
from ruamel.yaml import YAML

from great_expectations import DataContext
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import BatchRequest
from great_expectations.rule_based_profiler.profiler import Profiler
from great_expectations.validator.metric_configuration import MetricConfiguration

yaml = YAML()


# TODO: AJB 20210525 This fixture is not yet used but may be helpful to generate batches for unit tests of multibatch workflows. It should probably be extended to add different column types / data.
@pytest.fixture
def multibatch_generic_csv_generator():
    """
    Construct a series of csv files with many data types for use in multibatch testing
    """

    def _multibatch_generic_csv_generator(
        data_path: str,
        start_date: Optional[datetime.datetime] = None,
        num_event_batches: Optional[int] = 20,
        num_events_per_batch: Optional[int] = 5,
    ) -> List[str]:

        if start_date is None:
            start_date = datetime.datetime(2000, 1, 1)

        file_list = []
        category_strings = {
            0: "category0",
            1: "category1",
            2: "category2",
            3: "category3",
            4: "category4",
            5: "category5",
            6: "category6",
        }
        for batch_num in range(num_event_batches):
            # generate a dataframe with multiple column types
            batch_start_date = start_date + datetime.timedelta(
                days=(batch_num * num_events_per_batch)
            )
            # TODO: AJB 20210416 Add more column types
            df = pd.DataFrame(
                {
                    "event_date": [
                        (batch_start_date + datetime.timedelta(days=i)).strftime(
                            "%Y-%m-%d"
                        )
                        for i in range(num_events_per_batch)
                    ],
                    "batch_num": [batch_num + 1 for _ in range(num_events_per_batch)],
                    "string_cardinality_3": [
                        category_strings[i % 3] for i in range(num_events_per_batch)
                    ],
                }
            )
            filename = f"csv_batch_{batch_num + 1:03}_of_{num_event_batches:03}.csv"
            file_list.append(filename)
            df.to_csv(
                os.path.join(data_path, filename),
                index_label="intra_batch_index",
            )

        return file_list

    return _multibatch_generic_csv_generator


@pytest.fixture
def multibatch_generic_csv_generator_context(monkeypatch, empty_data_context):
    context: DataContext = empty_data_context
    monkeypatch.chdir(context.root_directory)
    data_relative_path = "../data"
    data_path = os.path.join(context.root_directory, data_relative_path)
    os.makedirs(data_path, exist_ok=True)

    data_connector_base_directory = "./"
    monkeypatch.setenv("base_directory", data_connector_base_directory)
    monkeypatch.setenv("data_fixtures_root", data_relative_path)

    datasource_name = "generic_csv_generator"
    data_connector_name = "daily_data_connector"
    asset_name = "daily_data_asset"
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
          - batch_num
          - total_batches
        pattern: csv_batch_(\d.+)_of_(\d.+)\.csv
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
                            "group_names": ["batch_num", "total_batches"],
                            "module_name": "great_expectations.datasource.data_connector.asset",
                            "pattern": "csv_batch_(\\d.+)_of_(\\d.+)\\.csv",
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
            "name": "generic_csv_generator",
        }
    ]
    return context


def test_batches_are_accessible(
    monkeypatch,
    multibatch_generic_csv_generator,
    multibatch_generic_csv_generator_context,
):
    """
    What does this test and why?
    Batches created in the multibatch_generic_csv_generator fixture should be available using the
    multibatch_generic_csv_generator_context
    This test most likely duplicates tests elsewhere, but it is more of a test of the configurable fixture.
    """

    context: DataContext = multibatch_generic_csv_generator_context
    data_relative_path = "../data"
    data_path = os.path.join(context.root_directory, data_relative_path)
    datasource_name = "generic_csv_generator"
    data_connector_name = "daily_data_connector"
    asset_name = "daily_data_asset"

    datasource = context.datasources[datasource_name]

    data_connector = datasource.data_connectors[data_connector_name]

    total_batches: int = 20
    file_list = multibatch_generic_csv_generator(
        data_path=data_path, num_event_batches=total_batches
    )

    assert (
        data_connector._get_data_reference_list_from_cache_by_data_asset_name(
            data_asset_name=asset_name
        )
        == file_list
    )

    batch_request_1 = BatchRequest(
        datasource_name="generic_csv_generator",
        data_connector_name="daily_data_connector",
        data_asset_name="daily_data_asset",
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
        MetricConfiguration("column.max", metric_domain_kwargs={"column": "batch_num"})
    )
    assert metric_max == total_batches
    metric_value_set = validator_1.get_metric(
        MetricConfiguration(
            "column.distinct_values",
            metric_domain_kwargs={"column": "string_cardinality_3"},
        )
    )
    assert metric_value_set == {"category0", "category1", "category2"}

    batch_request_2 = BatchRequest(
        datasource_name="generic_csv_generator",
        data_connector_name="daily_data_connector",
        data_asset_name="daily_data_asset",
        data_connector_query={
            "index": -2,
        },
    )
    validator_2 = context.get_validator(
        batch_request=batch_request_2,
        create_expectation_suite_with_name="my_expectation_suite_name_2",
    )
    metric_max = validator_2.get_metric(
        MetricConfiguration("column.max", metric_domain_kwargs={"column": "batch_num"})
    )
    assert metric_max == total_batches - 1
    metric_value_set = validator_2.get_metric(
        MetricConfiguration(
            "column.distinct_values",
            metric_domain_kwargs={"column": "string_cardinality_3"},
        )
    )
    assert metric_value_set == {"category0", "category1", "category2"}

    for batch_num in range(1, total_batches + 1):
        batch_request = BatchRequest(
            datasource_name="generic_csv_generator",
            data_connector_name="daily_data_connector",
            data_asset_name="daily_data_asset",
            data_connector_query={
                "index": -batch_num,
            },
        )
        validator = context.get_validator(
            batch_request=batch_request,
            create_expectation_suite_with_name=f"my_expectation_suite_name__{batch_num}",
        )
        metric_max = validator.get_metric(
            MetricConfiguration(
                "column.max", metric_domain_kwargs={"column": "batch_num"}
            )
        )
        assert metric_max == (total_batches + 1) - batch_num
        metric_value_set = validator.get_metric(
            MetricConfiguration(
                "column.distinct_values",
                metric_domain_kwargs={"column": "string_cardinality_3"},
            )
        )
        assert metric_value_set == {"category0", "category1", "category2"}


def test_profile_includes_citations(
    alice_columnar_table_single_batch_context,
    alice_columnar_table_single_batch,
):
    # Load data context
    data_context: DataContext = alice_columnar_table_single_batch_context
    # Load profiler configs & loop (run tests for each one)
    yaml_config: str = alice_columnar_table_single_batch["profiler_config"]

    # Instantiate Profiler
    profiler_config: dict = yaml.load(yaml_config)

    profiler: Profiler = Profiler(
        profiler_config=profiler_config,
        data_context=data_context,
    )

    expectation_suite: ExpectationSuite = profiler.profile(
        expectation_suite_name=alice_columnar_table_single_batch[
            "expected_expectation_suite_name"
        ],
        include_citation=True,
    )

    assert len(expectation_suite.meta["citations"]) > 0


def test_profile_excludes_citations(
    alice_columnar_table_single_batch_context,
    alice_columnar_table_single_batch,
):
    # Load data context
    data_context: DataContext = alice_columnar_table_single_batch_context
    # Load profiler configs & loop (run tests for each one)
    yaml_config: str = alice_columnar_table_single_batch["profiler_config"]

    # Instantiate Profiler
    profiler_config: dict = yaml.load(yaml_config)

    profiler: Profiler = Profiler(
        profiler_config=profiler_config,
        data_context=data_context,
    )

    expectation_suite: ExpectationSuite = profiler.profile(
        expectation_suite_name=alice_columnar_table_single_batch[
            "expected_expectation_suite_name"
        ],
        include_citation=False,
    )

    assert expectation_suite.meta.get("citations") is None
