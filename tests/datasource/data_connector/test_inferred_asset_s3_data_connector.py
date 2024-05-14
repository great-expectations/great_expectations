# noinspection PyPep8Naming
from contextlib import ExitStack as does_not_raise
from typing import List

import boto3
import pandas as pd
import pytest
from moto import mock_s3

import great_expectations.exceptions.exceptions as gx_exceptions
from great_expectations.core.batch import BatchRequest, IDDict, LegacyBatchDefinition
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource.data_connector import InferredAssetS3DataConnector

# noinspection PyProtectedMember
from great_expectations.datasource.data_connector.inferred_asset_s3_data_connector import (
    INVALID_S3_CHARS,
    _check_valid_s3_path,
)
from great_expectations.execution_engine import PandasExecutionEngine

yaml = YAMLHandler()

# module level markers
pytestmark = pytest.mark.big


@mock_s3
def test_basic_instantiation():
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "path/A-100.csv",
        "path/A-101.csv",
        "directory/B-1.csv",
        "directory/B-2.csv",
    ]
    for key in keys:
        client.put_object(Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key)

    my_data_connector: InferredAssetS3DataConnector = InferredAssetS3DataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine=PandasExecutionEngine(),
        default_regex={
            "pattern": r"(.+)/(.+)-(\d+)\.csv",
            "group_names": ["data_asset_name", "letter", "number"],
        },
        bucket=bucket,
        prefix="",
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


@mock_s3
def test_complex_regex_example_with_implicit_data_asset_names():
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "2020/01/alpha-1001.csv",
        "2020/01/beta-1002.csv",
        "2020/02/alpha-1003.csv",
        "2020/02/beta-1004.csv",
        "2020/03/alpha-1005.csv",
        "2020/03/beta-1006.csv",
        "2020/04/beta-1007.csv",
    ]
    for key in keys:
        client.put_object(Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key)

    my_data_connector: InferredAssetS3DataConnector = InferredAssetS3DataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine=PandasExecutionEngine(),
        default_regex={
            "pattern": r"(\d{4})/(\d{2})/(.+)-\d+\.csv",
            "group_names": ["year_dir", "month_dir", "data_asset_name"],
        },
        bucket=bucket,
        prefix="",
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


@mock_s3
def test_redundant_information_in_naming_convention_bucket_sorted():
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "some_bucket/2021/01/01/log_file-20210101.txt.gz",
        "some_bucket/2021/01/02/log_file-20210102.txt.gz",
        "some_bucket/2021/01/03/log_file-20210103.txt.gz",
        "some_bucket/2021/01/04/log_file-20210104.txt.gz",
        "some_bucket/2021/01/05/log_file-20210105.txt.gz",
        "some_bucket/2021/01/06/log_file-20210106.txt.gz",
        "some_bucket/2021/01/07/log_file-20210107.txt.gz",
    ]
    for key in keys:
        client.put_object(Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key)

    my_data_connector_yaml = yaml.load(
        f"""
          module_name: great_expectations.datasource.data_connector
          class_name: InferredAssetS3DataConnector
          datasource_name: test_environment
          name: my_inferred_asset_filesystem_data_connector
          bucket: {bucket}
          prefix: ""
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

    my_data_connector: InferredAssetS3DataConnector = instantiate_class_from_config(
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


@mock_s3
def test_redundant_information_in_naming_convention_bucket_sorter_does_not_match_group():
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "some_bucket/2021/01/01/log_file-20210101.txt.gz",
        "some_bucket/2021/01/02/log_file-20210102.txt.gz",
        "some_bucket/2021/01/03/log_file-20210103.txt.gz",
        "some_bucket/2021/01/04/log_file-20210104.txt.gz",
        "some_bucket/2021/01/05/log_file-20210105.txt.gz",
        "some_bucket/2021/01/06/log_file-20210106.txt.gz",
        "some_bucket/2021/01/07/log_file-20210107.txt.gz",
    ]
    for key in keys:
        client.put_object(Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key)

    my_data_connector_yaml = yaml.load(
        f"""
          module_name: great_expectations.datasource.data_connector
          class_name: InferredAssetS3DataConnector
          datasource_name: test_environment
          name: my_inferred_asset_filesystem_data_connector
          bucket: {bucket}
          prefix: ""
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
        my_data_connector: InferredAssetS3DataConnector = (  # noqa: F841
            instantiate_class_from_config(
                config=my_data_connector_yaml,
                runtime_environment={
                    "name": "my_inferred_asset_filesystem_data_connector",
                    "execution_engine": PandasExecutionEngine(),
                },
                config_defaults={"module_name": "great_expectations.datasource.data_connector"},
            )
        )


@mock_s3
def test_redundant_information_in_naming_convention_bucket_too_many_sorters():
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "some_bucket/2021/01/01/log_file-20210101.txt.gz",
        "some_bucket/2021/01/02/log_file-20210102.txt.gz",
        "some_bucket/2021/01/03/log_file-20210103.txt.gz",
        "some_bucket/2021/01/04/log_file-20210104.txt.gz",
        "some_bucket/2021/01/05/log_file-20210105.txt.gz",
        "some_bucket/2021/01/06/log_file-20210106.txt.gz",
        "some_bucket/2021/01/07/log_file-20210107.txt.gz",
    ]
    for key in keys:
        client.put_object(Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key)

    my_data_connector_yaml = yaml.load(
        f"""
        module_name: great_expectations.datasource.data_connector
        class_name: InferredAssetS3DataConnector
        datasource_name: test_environment
        name: my_inferred_asset_filesystem_data_connector
        bucket: {bucket}
        prefix: ""
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
        my_data_connector: InferredAssetS3DataConnector = (  # noqa: F841
            instantiate_class_from_config(
                config=my_data_connector_yaml,
                runtime_environment={
                    "name": "my_inferred_asset_filesystem_data_connector",
                    "execution_engine": PandasExecutionEngine(),
                },
                config_defaults={"module_name": "great_expectations.datasource.data_connector"},
            )
        )


# noinspection PyTypeChecker
@pytest.mark.parametrize(
    "path,expectation",
    [("BUCKET/DIR/FILE.CSV", does_not_raise())]
    + [
        (f"BUCKET/DIR/FILE{c}CSV", pytest.raises(gx_exceptions.ParserError))
        for c in INVALID_S3_CHARS
    ],
)
def test_bad_s3_regex_paths(path, expectation):
    with expectation:
        _check_valid_s3_path(path)
