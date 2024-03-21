from typing import List

import pytest

import great_expectations.exceptions.exceptions as gx_exceptions
from great_expectations.core.batch import BatchRequest, IDDict, LegacyBatchDefinition
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource import Datasource
from great_expectations.datasource.data_connector import (
    InferredAssetFilesystemDataConnector,
)
from great_expectations.execution_engine import PandasExecutionEngine
from tests.test_utils import create_files_in_directory

yaml = YAMLHandler()

# module level markers
pytestmark = pytest.mark.filesystem


def test_basic_instantiation(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("test_basic_instantiation"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "path/A-100.csv",
            "path/A-101.csv",
            "directory/B-1.csv",
            "directory/B-2.csv",
        ],
    )

    my_data_connector: InferredAssetFilesystemDataConnector = InferredAssetFilesystemDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine=PandasExecutionEngine(),
        default_regex={
            "pattern": r"(.+)/(.+)-(\d+)\.csv",
            "group_names": ["data_asset_name", "letter", "number"],
        },
        glob_directive="*/*.csv",
        base_directory=base_directory,
    )

    # noinspection PyProtectedMember
    my_data_connector._refresh_data_references_cache()

    assert my_data_connector.get_data_reference_count() == 4
    assert my_data_connector.get_unmatched_data_references() == []

    # Illegal execution environment name
    with pytest.raises(ValueError):
        print(
            my_data_connector.get_batch_definition_list_from_batch_request(
                batch_request=BatchRequest(
                    datasource_name="something",
                    data_connector_name="my_data_connector",
                    data_asset_name="something",
                )
            )
        )


def test_complex_regex_example_with_implicit_data_asset_names(tmp_path_factory):
    base_directory = str(
        tmp_path_factory.mktemp("test_complex_regex_example_with_implicit_data_asset_names")
    )
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "2020/01/alpha-1001.csv",
            "2020/01/beta-1002.csv",
            "2020/02/alpha-1003.csv",
            "2020/02/beta-1004.csv",
            "2020/03/alpha-1005.csv",
            "2020/03/beta-1006.csv",
            "2020/04/beta-1007.csv",
        ],
    )

    my_data_connector: InferredAssetFilesystemDataConnector = InferredAssetFilesystemDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine=PandasExecutionEngine(),
        default_regex={
            "pattern": r"(\d{4})/(\d{2})/(.+)-\d+\.csv",
            "group_names": ["year_dir", "month_dir", "data_asset_name"],
        },
        glob_directive="*/*/*.csv",
        base_directory=base_directory,
    )

    # noinspection PyProtectedMember
    my_data_connector._refresh_data_references_cache()

    # Test for an unknown execution environment
    with pytest.raises(ValueError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[LegacyBatchDefinition] = (
            my_data_connector.get_batch_definition_list_from_batch_request(
                batch_request=BatchRequest(
                    datasource_name="non_existent_datasource",
                    data_connector_name="my_data_connector",
                    data_asset_name="my_data_asset",
                )
            )
        )

    # Test for an unknown data_connector
    with pytest.raises(ValueError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[  # noqa: F841
            LegacyBatchDefinition
        ] = my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="FAKE_DATASOURCE_NAME",
                data_connector_name="non_existent_data_connector",
                data_asset_name="my_data_asset",
            )
        )

    assert (
        len(
            my_data_connector.get_batch_definition_list_from_batch_request(
                batch_request=BatchRequest(
                    datasource_name="FAKE_DATASOURCE_NAME",
                    data_connector_name="my_data_connector",
                    data_asset_name="alpha",
                )
            )
        )
        == 3
    )

    assert (
        len(
            my_data_connector.get_batch_definition_list_from_batch_request(
                batch_request=BatchRequest(
                    datasource_name="FAKE_DATASOURCE_NAME",
                    data_connector_name="my_data_connector",
                    data_asset_name="beta",
                )
            )
        )
        == 4
    )

    assert my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="FAKE_DATASOURCE_NAME",
            data_connector_name="my_data_connector",
            data_asset_name="alpha",
            data_connector_query={
                "batch_filter_parameters": {
                    "year_dir": "2020",
                    "month_dir": "03",
                }
            },
        )
    ) == [
        LegacyBatchDefinition(
            datasource_name="FAKE_DATASOURCE_NAME",
            data_connector_name="my_data_connector",
            data_asset_name="alpha",
            batch_identifiers=IDDict(
                year_dir="2020",
                month_dir="03",
            ),
        )
    ]


def test_redundant_information_in_naming_convention_bucket_sorted(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("logs"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "some_bucket/2021/01/01/log_file-20210101.txt.gz",
            "some_bucket/2021/01/02/log_file-20210102.txt.gz",
            "some_bucket/2021/01/03/log_file-20210103.txt.gz",
            "some_bucket/2021/01/04/log_file-20210104.txt.gz",
            "some_bucket/2021/01/05/log_file-20210105.txt.gz",
            "some_bucket/2021/01/06/log_file-20210106.txt.gz",
            "some_bucket/2021/01/07/log_file-20210107.txt.gz",
        ],
    )

    my_data_connector_yaml = yaml.load(
        f"""
          module_name: great_expectations.datasource.data_connector
          class_name: InferredAssetFilesystemDataConnector
          datasource_name: test_environment
          name: my_inferred_asset_filesystem_data_connector
          base_directory: {base_directory}/
          glob_directive: "*/*/*/*/*.txt.gz"
          default_regex:
              group_names:
                  - data_asset_name
                  - year
                  - month
                  - day
                  - full_date
              pattern: (\\w{{11}})/(\\d{{4}})/(\\d{{2}})/(\\d{{2}})/log_file-(.*)\\.txt\\.gz
          sorters:
              - orderby: desc
                class_name: DateTimeSorter
                name: full_date

          """,
    )

    my_data_connector: InferredAssetFilesystemDataConnector = instantiate_class_from_config(
        config=my_data_connector_yaml,
        runtime_environment={
            "name": "my_inferred_asset_filesystem_data_connector",
            "datasource_name": "test_environment",
            "execution_engine": "BASE_ENGINE",
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )

    sorted_batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        BatchRequest(
            datasource_name="test_environment",
            data_connector_name="my_inferred_asset_filesystem_data_connector",
            data_asset_name="some_bucket",
        )
    )

    expected = [
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="my_inferred_asset_filesystem_data_connector",
            data_asset_name="some_bucket",
            batch_identifiers=IDDict(
                {"year": "2021", "month": "01", "day": "07", "full_date": "20210107"}
            ),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="my_inferred_asset_filesystem_data_connector",
            data_asset_name="some_bucket",
            batch_identifiers=IDDict(
                {"year": "2021", "month": "01", "day": "06", "full_date": "20210106"}
            ),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="my_inferred_asset_filesystem_data_connector",
            data_asset_name="some_bucket",
            batch_identifiers=IDDict(
                {"year": "2021", "month": "01", "day": "05", "full_date": "20210105"}
            ),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="my_inferred_asset_filesystem_data_connector",
            data_asset_name="some_bucket",
            batch_identifiers=IDDict(
                {"year": "2021", "month": "01", "day": "04", "full_date": "20210104"}
            ),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="my_inferred_asset_filesystem_data_connector",
            data_asset_name="some_bucket",
            batch_identifiers=IDDict(
                {"year": "2021", "month": "01", "day": "03", "full_date": "20210103"}
            ),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="my_inferred_asset_filesystem_data_connector",
            data_asset_name="some_bucket",
            batch_identifiers=IDDict(
                {"year": "2021", "month": "01", "day": "02", "full_date": "20210102"}
            ),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="my_inferred_asset_filesystem_data_connector",
            data_asset_name="some_bucket",
            batch_identifiers=IDDict(
                {"year": "2021", "month": "01", "day": "01", "full_date": "20210101"}
            ),
        ),
    ]
    assert expected == sorted_batch_definition_list


def test_redundant_information_in_naming_convention_bucket_sorter_does_not_match_group(
    tmp_path_factory,
):
    base_directory = str(tmp_path_factory.mktemp("logs"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "some_bucket/2021/01/01/log_file-20210101.txt.gz",
            "some_bucket/2021/01/02/log_file-20210102.txt.gz",
            "some_bucket/2021/01/03/log_file-20210103.txt.gz",
            "some_bucket/2021/01/04/log_file-20210104.txt.gz",
            "some_bucket/2021/01/05/log_file-20210105.txt.gz",
            "some_bucket/2021/01/06/log_file-20210106.txt.gz",
            "some_bucket/2021/01/07/log_file-20210107.txt.gz",
        ],
    )

    my_data_connector_yaml = yaml.load(
        f"""
          module_name: great_expectations.datasource.data_connector
          class_name: InferredAssetFilesystemDataConnector
          datasource_name: test_environment
          name: my_inferred_asset_filesystem_data_connector
          base_directory: {base_directory}/
          glob_directive: "*/*/*/*/*.txt.gz"
          default_regex:
              group_names:
                  - data_asset_name
                  - year
                  - month
                  - day
                  - full_date
              pattern: (\\w{{11}})/(\\d{{4}})/(\\d{{2}})/(\\d{{2}})/log_file-(.*)\\.txt\\.gz
          sorters:
              - orderby: desc
                class_name: DateTimeSorter
                name: not_matching_anything

          """,
    )

    with pytest.raises(gx_exceptions.DataConnectorError):
        # noinspection PyUnusedLocal
        my_data_connector: InferredAssetFilesystemDataConnector = (  # noqa: F841
            instantiate_class_from_config(
                config=my_data_connector_yaml,
                runtime_environment={
                    "name": "my_inferred_asset_filesystem_data_connector",
                    "datasource_name": "test_environment",
                    "execution_engine": "BASE_ENGINE",
                },
                config_defaults={"module_name": "great_expectations.datasource.data_connector"},
            )
        )


def test_redundant_information_in_naming_convention_bucket_too_many_sorters(
    tmp_path_factory,
):
    base_directory = str(tmp_path_factory.mktemp("logs"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "some_bucket/2021/01/01/log_file-20210101.txt.gz",
            "some_bucket/2021/01/02/log_file-20210102.txt.gz",
            "some_bucket/2021/01/03/log_file-20210103.txt.gz",
            "some_bucket/2021/01/04/log_file-20210104.txt.gz",
            "some_bucket/2021/01/05/log_file-20210105.txt.gz",
            "some_bucket/2021/01/06/log_file-20210106.txt.gz",
            "some_bucket/2021/01/07/log_file-20210107.txt.gz",
        ],
    )

    my_data_connector_yaml = yaml.load(
        f"""
        module_name: great_expectations.datasource.data_connector
        class_name: InferredAssetFilesystemDataConnector
        datasource_name: test_environment
        name: my_inferred_asset_filesystem_data_connector
        base_directory: {base_directory}/
        glob_directive: "*/*/*/*/*.txt.gz"
        default_regex:
            group_names:
                - data_asset_name
                - year
                - month
                - day
                - full_date
            pattern: (\\w{{11}})/(\\d{{4}})/(\\d{{2}})/(\\d{{2}})/log_file-(.*)\\.txt\\.gz
        sorters:
            - datetime_format: "%Y%m%d"
              orderby: desc
              class_name: DateTimeSorter
              name: timestamp
            - orderby: desc
              class_name: NumericSorter
              name: price
          """,
    )

    with pytest.raises(gx_exceptions.DataConnectorError):
        # noinspection PyUnusedLocal
        my_data_connector: InferredAssetFilesystemDataConnector = (  # noqa: F841
            instantiate_class_from_config(
                config=my_data_connector_yaml,
                runtime_environment={
                    "name": "my_inferred_asset_filesystem_data_connector",
                    "datasource_name": "test_environment",
                    "execution_engine": "BASE_ENGINE",
                },
                config_defaults={"module_name": "great_expectations.datasource.data_connector"},
            )
        )


def test_one_year_as_12_data_assets_1_batch_each(empty_data_context, tmp_path_factory):
    context = empty_data_context
    base_directory: str = str(tmp_path_factory.mktemp("log_data"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "some_bucket/report_2018-01.csv",
            "some_bucket/report_2018-02.csv",
            "some_bucket/report_2018-03.csv",
            "some_bucket/report_2018-04.csv",
            "some_bucket/report_2018-05.csv",
            "some_bucket/report_2018-06.csv",
            "some_bucket/report_2018-07.csv",
            "some_bucket/report_2018-08.csv",
            "some_bucket/report_2018-09.csv",
            "some_bucket/report_2018-10.csv",
            "some_bucket/report_2018-11.csv",
            "some_bucket/report_2018-12.csv",
        ],
    )
    datasource_yaml: str = f"""
    name: taxi_datasource
    class_name: Datasource
    module_name: great_expectations.datasource
    execution_engine:
      module_name: great_expectations.execution_engine
      class_name: PandasExecutionEngine
    data_connectors:
        default_inferred_data_connector_name:
            class_name: InferredAssetFilesystemDataConnector
            base_directory: {base_directory}/some_bucket/
            default_regex:
              group_names:
                - data_asset_name
              pattern: (.*_2018-.*)\\.csv
    """
    context.add_datasource(**yaml.load(datasource_yaml))
    datasource: Datasource = context.get_datasource(datasource_name="taxi_datasource")
    data_asset_names: dict = datasource.get_available_data_asset_names(
        data_connector_names="default_inferred_data_connector_name"
    )
    # making the result deterministic
    data_asset_names["default_inferred_data_connector_name"].sort()
    assert data_asset_names == {
        "default_inferred_data_connector_name": [
            "report_2018-01",
            "report_2018-02",
            "report_2018-03",
            "report_2018-04",
            "report_2018-05",
            "report_2018-06",
            "report_2018-07",
            "report_2018-08",
            "report_2018-09",
            "report_2018-10",
            "report_2018-11",
            "report_2018-12",
        ]
    }
    assert len(data_asset_names["default_inferred_data_connector_name"]) == 12


def test_one_year_as_1_data_asset_12_batches(empty_data_context, tmp_path_factory):
    context = empty_data_context
    base_directory: str = str(tmp_path_factory.mktemp("log_data"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "some_bucket/report_2018-01.csv",
            "some_bucket/report_2018-02.csv",
            "some_bucket/report_2018-03.csv",
            "some_bucket/report_2018-04.csv",
            "some_bucket/report_2018-05.csv",
            "some_bucket/report_2018-06.csv",
            "some_bucket/report_2018-07.csv",
            "some_bucket/report_2018-08.csv",
            "some_bucket/report_2018-09.csv",
            "some_bucket/report_2018-10.csv",
            "some_bucket/report_2018-11.csv",
            "some_bucket/report_2018-12.csv",
        ],
    )
    datasource_yaml: str = f"""
        name: taxi_datasource
        class_name: Datasource
        module_name: great_expectations.datasource
        execution_engine:
          module_name: great_expectations.execution_engine
          class_name: PandasExecutionEngine
        data_connectors:
            default_inferred_data_connector_name:
                class_name: InferredAssetFilesystemDataConnector
                base_directory: {base_directory}/some_bucket/
                default_regex:
                  group_names:
                    - data_asset_name
                    - month
                  pattern: (report_2018)-(\\d.*)\\.csv
        """
    context.add_datasource(**yaml.load(datasource_yaml))
    datasource: Datasource = context.get_datasource(datasource_name="taxi_datasource")
    data_asset_names: dict = datasource.get_available_data_asset_names(
        data_connector_names="default_inferred_data_connector_name"
    )
    # making the result deterministic
    data_asset_names["default_inferred_data_connector_name"].sort()
    assert data_asset_names == {"default_inferred_data_connector_name": ["report_2018"]}
    assert len(data_asset_names["default_inferred_data_connector_name"]) == 1
