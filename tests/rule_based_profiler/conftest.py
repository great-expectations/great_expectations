import datetime
import os
import shutil
from typing import Any, Dict

import pandas as pd
import pytest
from ruamel.yaml import YAML

from great_expectations import DataContext
from great_expectations.data_context.util import file_relative_path
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder.domain import Domain

# noinspection PyUnresolvedReferences
from great_expectations.rule_based_profiler.parameter_builder.parameter_container import (
    ParameterContainer,
    ParameterNode,
)
from great_expectations.rule_based_profiler.rule.rule import Rule

# noinspection PyUnresolvedReferences
from tests.rule_based_profiler.alice_user_workflow_fixture import (
    alice_columnar_table_single_batch,
)

# noinspection PyUnresolvedReferences
from tests.rule_based_profiler.bob_user_workflow_fixture import (
    bob_columnar_table_multi_batch,
)

# noinspection PyUnresolvedReferences
from tests.rule_based_profiler.bobby_user_workflow_fixture import (
    bobby_columnar_table_multi_batch,
)

yaml = YAML()


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
                },
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


@pytest.fixture
def pandas_test_df():
    df: pd.DataFrame = pd.DataFrame(
        {
            "Age": pd.Series(
                [
                    7,
                    15,
                    21,
                    39,
                    None,
                ],
                dtype="float64",
            ),
            "Date": pd.Series(
                [
                    datetime.date(2020, 12, 31),
                    datetime.date(2021, 1, 1),
                    datetime.date(2021, 2, 21),
                    datetime.date(2021, 3, 20),
                    None,
                ],
                dtype="object",
            ),
            "Description": pd.Series(
                [
                    "child",
                    "teenager",
                    "young adult",
                    "adult",
                    None,
                ],
                dtype="object",
            ),
        }
    )
    df["Date"] = pd.to_datetime(df["Date"])
    return df


# noinspection PyPep8Naming
@pytest.fixture
def table_Users_domain():
    return Domain(
        domain_type=MetricDomainTypes.TABLE,
        domain_kwargs=None,
        details=None,
    )


# noinspection PyPep8Naming
@pytest.fixture
def column_Age_domain():
    return Domain(
        domain_type=MetricDomainTypes.COLUMN,
        domain_kwargs={
            "column": "Age",
            "batch_id": "c260e179bb1bc81d84bba72a8110d8e2",
        },
        details=None,
    )


# noinspection PyPep8Naming
@pytest.fixture
def column_Date_domain():
    return Domain(
        domain_type=MetricDomainTypes.COLUMN,
        domain_kwargs={
            "column": "Date",
            "batch_id": "c260e179bb1bc81d84bba72a8110d8e2",
        },
        details=None,
    )


# noinspection PyPep8Naming
@pytest.fixture
def column_Description_domain():
    return Domain(
        domain_type=MetricDomainTypes.COLUMN,
        domain_kwargs={
            "column": "Description",
            "batch_id": "c260e179bb1bc81d84bba72a8110d8e2",
        },
        details=None,
    )


@pytest.fixture
def single_part_name_parameter_container():
    return ParameterContainer(
        parameter_nodes={
            "mean": ParameterNode(
                {
                    "mean": 5.0,
                }
            ),
        }
    )


@pytest.fixture
def multi_part_name_parameter_container():
    """
    $parameter.date_strings.yyyy_mm_dd_hh_mm_ss_tz_date_format
    $parameter.date_strings.yyyy_mm_dd_date_format
    $parameter.date_strings.mm_yyyy_dd_hh_mm_ss_tz_date_format
    $parameter.date_strings.mm_yyyy_dd_date_format
    $parameter.date_strings.tolerances.max_abs_error_time_milliseconds
    $parameter.date_strings.tolerances.max_num_conversion_attempts
    $parameter.tolerances.mostly
    $mean
    """
    root_mean_node: ParameterNode = ParameterNode(
        {
            "mean": 6.5e-1,
        }
    )
    financial_tolerances_parameter_node: ParameterNode = ParameterNode(
        {
            "usd": 1.0,
        }
    )
    tolerances_parameter_node: ParameterNode = ParameterNode(
        {
            "mostly": 9.1e-1,
            "financial": financial_tolerances_parameter_node,
        }
    )
    date_strings_tolerances_parameter_node: ParameterNode = ParameterNode(
        {
            "max_abs_error_time_milliseconds": 100,
            "max_num_conversion_attempts": 5,
        }
    )
    date_strings_parameter_node: ParameterNode = ParameterNode(
        {
            "yyyy_mm_dd_hh_mm_ss_tz_date_format": ParameterNode(
                {
                    "value": "%Y-%m-%d %H:%M:%S %Z",
                    "details": ParameterNode(
                        {
                            "confidence": 7.8e-1,
                        },
                    ),
                }
            ),
            "yyyy_mm_dd_date_format": ParameterNode(
                {
                    "value": "%Y-%m-%d",
                    "details": ParameterNode(
                        {
                            "confidence": 7.8e-1,
                        },
                    ),
                }
            ),
            "mm_yyyy_dd_hh_mm_ss_tz_date_format": ParameterNode(
                {
                    "value": "%m-%Y-%d %H:%M:%S %Z",
                    "details": ParameterNode(
                        {
                            "confidence": 7.8e-1,
                        },
                    ),
                }
            ),
            "mm_yyyy_dd_date_format": ParameterNode(
                {
                    "value": "%m-%Y-%d",
                    "details": ParameterNode(
                        {
                            "confidence": 7.8e-1,
                        },
                    ),
                }
            ),
            "tolerances": date_strings_tolerances_parameter_node,
        }
    )
    parameter_multi_part_name_parameter_node: ParameterNode = ParameterNode(
        {
            "date_strings": date_strings_parameter_node,
            "tolerances": tolerances_parameter_node,
        }
    )
    root_parameter_node: ParameterNode = ParameterNode(
        {
            "parameter": parameter_multi_part_name_parameter_node,
        }
    )
    return ParameterContainer(
        parameter_nodes={
            "parameter": root_parameter_node,
            "mean": root_mean_node,
        }
    )


@pytest.fixture
def parameter_values_eight_parameters_multiple_depths():
    parameter_values: Dict[str, Any] = {
        "$parameter.date_strings.yyyy_mm_dd_hh_mm_ss_tz_date_format.value": "%Y-%m-%d %H:%M:%S %Z",
        "$parameter.date_strings.yyyy_mm_dd_hh_mm_ss_tz_date_format.details": {
            "confidence": 7.8e-1
        },
        "$parameter.date_strings.yyyy_mm_dd_date_format.value": "%Y-%m-%d",
        "$parameter.date_strings.yyyy_mm_dd_date_format.details": {
            "confidence": 7.8e-1
        },
        "$parameter.date_strings.mm_yyyy_dd_hh_mm_ss_tz_date_format.value": "%m-%Y-%d %H:%M:%S %Z",
        "$parameter.date_strings.mm_yyyy_dd_hh_mm_ss_tz_date_format.details": {
            "confidence": 7.8e-1
        },
        "$parameter.date_strings.mm_yyyy_dd_date_format.value": "%m-%Y-%d",
        "$parameter.date_strings.mm_yyyy_dd_date_format.details": {
            "confidence": 7.8e-1
        },
        "$parameter.date_strings.tolerances.max_abs_error_time_milliseconds": 100,
        "$parameter.date_strings.tolerances.max_num_conversion_attempts": 5,
        "$parameter.tolerances.mostly": 9.1e-1,
        "$parameter.tolerances.financial.usd": 1.0,
        "$mean": 6.5e-1,
    }
    return parameter_values


# noinspection PyPep8Naming
@pytest.fixture
def rule_without_variables_without_parameters():
    rule: Rule = Rule(
        name="rule_with_no_variables_no_parameters",
        domain_builder=None,
        parameter_builders=None,
        expectation_configuration_builders=None,
        variables=None,
    )
    return rule


# noinspection PyPep8Naming
@pytest.fixture
def rule_with_variables_with_parameters(
    column_Age_domain,
    column_Date_domain,
    single_part_name_parameter_container,
    multi_part_name_parameter_container,
):
    variables_multi_part_name_parameter_node: ParameterNode = ParameterNode(
        {
            "false_positive_threshold": 1.0e-2,
        }
    )
    root_variables_node: ParameterNode = ParameterNode(
        {
            "variables": variables_multi_part_name_parameter_node,  # $variables.false_positive_threshold
        }
    )
    rule: Rule = Rule(
        name="rule_with_variables_with_parameters",
        domain_builder=None,
        parameter_builders=None,
        expectation_configuration_builders=None,
        variables=ParameterContainer(
            parameter_nodes={
                "variables": root_variables_node,
            }
        ),
    )
    rule._parameters = {
        column_Age_domain.id: single_part_name_parameter_container,
        column_Date_domain.id: multi_part_name_parameter_container,
    }
    return rule
