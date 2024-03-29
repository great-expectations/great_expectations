from unittest import mock

import pytest

import great_expectations.exceptions.exceptions as gx_exceptions
from great_expectations.compatibility import google
from great_expectations.core.batch import BatchRequest, IDDict, LegacyBatchDefinition
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource.data_connector import InferredAssetGCSDataConnector
from great_expectations.execution_engine import PandasExecutionEngine

yaml = YAMLHandler()


if not google.storage:
    pytest.skip(
        'Could not import "storage" from google.cloud in configured_asset_gcs_data_connector.py',
        allow_module_level=True,
    )

# module level markers
pytestmark = pytest.mark.big


@pytest.fixture
def expected_config_dict():
    """Used to validate `self_check()` and `test_yaml_config()` outputs."""
    config = {
        "class_name": "InferredAssetGCSDataConnector",
        "data_asset_count": 2,
        "example_data_asset_names": ["directory", "path"],
        "data_assets": {
            "directory": {
                "batch_definition_count": 2,
                "example_data_references": ["directory/B-1.csv", "directory/B-2.csv"],
            },
            "path": {
                "batch_definition_count": 2,
                "example_data_references": ["path/A-100.csv", "path/A-101.csv"],
            },
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
    }
    return config


# noinspection PyUnusedLocal
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
    return_value=[
        "path/A-100.csv",
        "path/A-101.csv",
        "directory/B-1.csv",
        "directory/B-2.csv",
    ],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.google.storage.Client"
)
def test_instantiation_without_args(mock_gcs_conn, mock_list_keys, expected_config_dict):
    my_data_connector = InferredAssetGCSDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine=PandasExecutionEngine(),
        default_regex={
            "pattern": r"(.+)/(.+)-(\d+)\.csv",
            "group_names": ["data_asset_name", "letter", "number"],
        },
        bucket_or_name="test_bucket",
        prefix="",
    )

    my_data_connector._refresh_data_references_cache()
    assert my_data_connector.get_data_reference_count() == 4
    assert my_data_connector.get_unmatched_data_references() == []


# noinspection PyUnusedLocal
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
    return_value=[
        "path/A-100.csv",
        "path/A-101.csv",
        "directory/B-1.csv",
        "directory/B-2.csv",
    ],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.google.service_account.Credentials.from_service_account_file"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.google.storage.Client"
)
def test_instantiation_with_filename_arg(
    mock_gcs_conn, mock_auth_method, mock_list_keys, expected_config_dict
):
    my_data_connector = InferredAssetGCSDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine=PandasExecutionEngine(),
        gcs_options={
            "filename": "my_filename.json",
        },
        default_regex={
            "pattern": r"(.+)/(.+)-(\d+)\.csv",
            "group_names": ["data_asset_name", "letter", "number"],
        },
        bucket_or_name="test_bucket",
        prefix="",
    )

    my_data_connector._refresh_data_references_cache()
    assert my_data_connector.get_data_reference_count() == 4
    assert my_data_connector.get_unmatched_data_references() == []


# noinspection PyUnusedLocal
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
    return_value=[
        "path/A-100.csv",
        "path/A-101.csv",
        "directory/B-1.csv",
        "directory/B-2.csv",
    ],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.google.service_account.Credentials.from_service_account_info"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.google.storage.Client"
)
def test_instantiation_with_info_arg(
    mock_gcs_conn, mock_auth_method, mock_list_keys, expected_config_dict
):
    my_data_connector = InferredAssetGCSDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine=PandasExecutionEngine(),
        gcs_options={
            "info": "{ my_json: my_content }",
        },
        default_regex={
            "pattern": r"(.+)/(.+)-(\d+)\.csv",
            "group_names": ["data_asset_name", "letter", "number"],
        },
        bucket_or_name="test_bucket",
        prefix="",
    )

    my_data_connector._refresh_data_references_cache()
    assert my_data_connector.get_data_reference_count() == 4
    assert my_data_connector.get_unmatched_data_references() == []


@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
    return_value=[
        "path/A-100.csv",
        "path/A-101.csv",
        "directory/B-1.csv",
        "directory/B-2.csv",
    ],
)
def test_get_batch_definition_list_from_batch_request_with_nonexistent_datasource_name_raises_error(
    mock_gcs_conn, mock_list_keys, empty_data_context_stats_enabled
):
    my_data_connector = InferredAssetGCSDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine=PandasExecutionEngine(),
        default_regex={
            "pattern": r"(.+)/(.+)-(\d+)\.csv",
            "group_names": ["data_asset_name", "letter", "number"],
        },
        bucket_or_name="test_bucket",
        prefix="",
    )

    # Raises error in `DataConnector._validate_batch_request()` due to `datasource_name` in BatchRequest not matching DataConnector `datasource_name`  # noqa: E501
    with pytest.raises(ValueError):
        my_data_connector.get_batch_definition_list_from_batch_request(
            BatchRequest(
                datasource_name="something",
                data_connector_name="my_data_connector",
                data_asset_name="something",
            )
        )


@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
    return_value=[
        "2020/01/alpha-1001.csv",
        "2020/01/beta-1002.csv",
        "2020/02/alpha-1003.csv",
        "2020/02/beta-1004.csv",
        "2020/03/alpha-1005.csv",
        "2020/03/beta-1006.csv",
        "2020/04/beta-1007.csv",
    ],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.google.storage.Client"
)
def test_get_batch_definition_list_from_batch_request_with_unknown_data_connector_raises_error(
    mock_gcs_conn,
    mock_list_keys,
):
    my_data_connector: InferredAssetGCSDataConnector = InferredAssetGCSDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine=PandasExecutionEngine(),
        default_regex={
            "pattern": r"(\d{4})/(\d{2})/(.+)-\d+\.csv",
            "group_names": ["year_dir", "month_dir", "data_asset_name"],
        },
        bucket_or_name="test_bucket",
        prefix="",
    )

    my_data_connector._refresh_data_references_cache()

    # Raises error in `DataConnector._validate_batch_request()` due to `data-connector_name` in BatchRequest not matching DataConnector name  # noqa: E501
    with pytest.raises(ValueError):
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="FAKE_DATASOURCE_NAME",
                data_connector_name="non_existent_data_connector",
                data_asset_name="my_data_asset",
            )
        )


@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
    return_value=[
        "2020/01/alpha-1001.csv",
        "2020/01/beta-1002.csv",
        "2020/02/alpha-1003.csv",
        "2020/02/beta-1004.csv",
        "2020/03/alpha-1005.csv",
        "2020/03/beta-1006.csv",
        "2020/04/beta-1007.csv",
    ],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.google.storage.Client"
)
def test_complex_regex_example_with_implicit_data_asset_names(
    mock_gcs_conn,
    mock_list_keys,
):
    my_data_connector: InferredAssetGCSDataConnector = InferredAssetGCSDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine=PandasExecutionEngine(),
        default_regex={
            "pattern": r"(\d{4})/(\d{2})/(.+)-\d+\.csv",
            "group_names": ["year_dir", "month_dir", "data_asset_name"],
        },
        bucket_or_name="test_bucket",
        prefix="",
    )

    my_data_connector._refresh_data_references_cache()

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


@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
    return_value=[
        "some_bucket/2021/01/01/log_file-20210101.txt.gz",
        "some_bucket/2021/01/02/log_file-20210102.txt.gz",
        "some_bucket/2021/01/03/log_file-20210103.txt.gz",
        "some_bucket/2021/01/04/log_file-20210104.txt.gz",
        "some_bucket/2021/01/05/log_file-20210105.txt.gz",
        "some_bucket/2021/01/06/log_file-20210106.txt.gz",
        "some_bucket/2021/01/07/log_file-20210107.txt.gz",
    ],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.google.storage.Client"
)
def test_redundant_information_in_naming_convention_bucket_sorted(
    mock_gcs_conn,
    mock_list_keys,
):
    my_data_connector_yaml = yaml.load(
        """
          module_name: great_expectations.datasource.data_connector
          class_name: InferredAssetGCSDataConnector
          datasource_name: test_environment
          name: my_inferred_asset_filesystem_data_connector
          bucket_or_name: test_bucket
          prefix: ""
          default_regex:
              group_names:
                  - data_asset_name
                  - year
                  - month
                  - day
                  - full_date
              pattern: (\\w{11})/(\\d{4})/(\\d{2})/(\\d{2})/log_file-(.*)\\.txt\\.gz
          sorters:
              - orderby: desc
                class_name: DateTimeSorter
                name: full_date
          """,
    )

    my_data_connector: InferredAssetGCSDataConnector = instantiate_class_from_config(
        config=my_data_connector_yaml,
        runtime_environment={
            "name": "my_inferred_asset_filesystem_data_connector",
            "execution_engine": PandasExecutionEngine(),
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


@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
    return_value=[
        "some_bucket/2021/01/01/log_file-20210101.txt.gz",
        "some_bucket/2021/01/02/log_file-20210102.txt.gz",
        "some_bucket/2021/01/03/log_file-20210103.txt.gz",
        "some_bucket/2021/01/04/log_file-20210104.txt.gz",
        "some_bucket/2021/01/05/log_file-20210105.txt.gz",
        "some_bucket/2021/01/06/log_file-20210106.txt.gz",
        "some_bucket/2021/01/07/log_file-20210107.txt.gz",
    ],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.google.storage.Client"
)
def test_redundant_information_in_naming_convention_bucket_sorter_does_not_match_group(
    mock_gcs_conn,
    mock_list_keys,
):
    my_data_connector_yaml = yaml.load(
        """
          module_name: great_expectations.datasource.data_connector
          class_name: InferredAssetGCSDataConnector
          datasource_name: test_environment
          name: my_inferred_asset_filesystem_data_connector
          bucket_or_name: test_bucket
          prefix: ""
          default_regex:
              group_names:
                  - data_asset_name
                  - year
                  - month
                  - day
                  - full_date
              pattern: (\\w{11})/(\\d{4})/(\\d{2})/(\\d{2})/log_file-(.*)\\.txt\\.gz
          sorters:
              - orderby: desc
                class_name: DateTimeSorter
                name: not_matching_anything

          """,
    )

    with pytest.raises(gx_exceptions.DataConnectorError):
        instantiate_class_from_config(
            config=my_data_connector_yaml,
            runtime_environment={
                "name": "my_inferred_asset_filesystem_data_connector",
                "execution_engine": PandasExecutionEngine(),
            },
            config_defaults={"module_name": "great_expectations.datasource.data_connector"},
        )


@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
    return_value=[
        "some_bucket/2021/01/01/log_file-20210101.txt.gz",
        "some_bucket/2021/01/02/log_file-20210102.txt.gz",
        "some_bucket/2021/01/03/log_file-20210103.txt.gz",
        "some_bucket/2021/01/04/log_file-20210104.txt.gz",
        "some_bucket/2021/01/05/log_file-20210105.txt.gz",
        "some_bucket/2021/01/06/log_file-20210106.txt.gz",
        "some_bucket/2021/01/07/log_file-20210107.txt.gz",
    ],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.google.storage.Client"
)
def test_redundant_information_in_naming_convention_bucket_too_many_sorters(
    mock_gcs_conn,
    mock_list_keys,
):
    my_data_connector_yaml = yaml.load(
        """
        module_name: great_expectations.datasource.data_connector
        class_name: InferredAssetGCSDataConnector
        datasource_name: test_environment
        name: my_inferred_asset_filesystem_data_connector
        bucket_or_name: test_bucket
        prefix: ""
        default_regex:
            group_names:
                - data_asset_name
                - year
                - month
                - day
                - full_date
            pattern: (\\w{11})/(\\d{4})/(\\d{2})/(\\d{2})/log_file-(.*)\\.txt\\.gz
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
        instantiate_class_from_config(
            config=my_data_connector_yaml,
            runtime_environment={
                "name": "my_inferred_asset_filesystem_data_connector",
                "execution_engine": PandasExecutionEngine(),
            },
            config_defaults={"module_name": "great_expectations.datasource.data_connector"},
        )


# noinspection PyUnusedLocal
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.google.storage.Client"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
)
def test_get_full_file_path(mock_gcs_conn, mock_list_keys, empty_data_context_stats_enabled):
    yaml_string = """
class_name: InferredAssetGCSDataConnector
datasource_name: FAKE_DATASOURCE_NAME
bucket_or_name: my_bucket
prefix: my_base_directory/
default_regex:
   pattern: ^(.+)-(\\d{4})(\\d{2})\\.(csv|txt)$
   group_names:
       - data_asset_name
       - year_dir
       - month_dir
   """
    config = yaml.load(yaml_string)

    mock_list_keys.return_value = [
        "my_base_directory/alpha/files/go/here/alpha-202001.csv",
        "my_base_directory/alpha/files/go/here/alpha-202002.csv",
        "my_base_directory/alpha/files/go/here/alpha-202003.csv",
        "my_base_directory/beta_here/beta-202001.txt",
        "my_base_directory/beta_here/beta-202002.txt",
        "my_base_directory/beta_here/beta-202003.txt",
        "my_base_directory/beta_here/beta-202004.txt",
        "my_base_directory/gamma-202001.csv",
        "my_base_directory/gamma-202002.csv",
        "my_base_directory/gamma-202003.csv",
        "my_base_directory/gamma-202004.csv",
        "my_base_directory/gamma-202005.csv",
    ]

    my_data_connector: InferredAssetGCSDataConnector = instantiate_class_from_config(
        config,
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
        runtime_environment={
            "name": "my_data_connector",
            "execution_engine": PandasExecutionEngine(),
        },
    )

    assert (
        my_data_connector._get_full_file_path(
            "my_base_directory/alpha/files/go/here/alpha-202001.csv", "alpha"
        )
        == "gs://my_bucket/my_base_directory/alpha/files/go/here/alpha-202001.csv"
    )
    assert (
        my_data_connector._get_full_file_path("my_base_directory/beta_here/beta-202002.txt", "beta")
        == "gs://my_bucket/my_base_directory/beta_here/beta-202002.txt"
    )
    assert (
        my_data_connector._get_full_file_path("my_base_directory/gamma-202005.csv", "gamma")
        == "gs://my_bucket/my_base_directory/gamma-202005.csv"
    )
