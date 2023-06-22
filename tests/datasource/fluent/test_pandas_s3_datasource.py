from __future__ import annotations

import logging
import os
import re
from pprint import pformat as pf
from typing import TYPE_CHECKING, List, cast

import pandas as pd
import pydantic
import pytest
from moto import mock_s3
from pytest import param

import great_expectations.exceptions as ge_exceptions
from great_expectations.compatibility import aws
from great_expectations.core.util import S3Url
from great_expectations.datasource.fluent import PandasS3Datasource
from great_expectations.datasource.fluent.data_asset.data_connector import (
    S3DataConnector,
)
from great_expectations.datasource.fluent.dynamic_pandas import PANDAS_VERSION
from great_expectations.datasource.fluent.file_path_data_asset import (
    _FilePathDataAsset,
)
from great_expectations.datasource.fluent.interfaces import TestConnectionError
from great_expectations.datasource.fluent.pandas_file_path_datasource import (
    CSVAsset,
)

if TYPE_CHECKING:
    from botocore.client import BaseClient


logger = logging.getLogger(__file__)


# apply markers to entire test module
pytestmark = [
    pytest.mark.skipif(
        PANDAS_VERSION < 1.2, reason=f"Fluent pandas not supported on {PANDAS_VERSION}"
    ),
    pytest.mark.skipif(
        not aws.boto3,
        reason="Unable to load AWS connection object. Please install boto3 and botocore.",
    ),
]


@pytest.fixture()
def aws_region_name() -> str:
    return "us-east-1"


@pytest.fixture()
def aws_s3_bucket_name() -> str:
    return "test_bucket"


@pytest.fixture(scope="function")
def aws_credentials() -> None:
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.mark.skipif(not aws.boto3)
@pytest.fixture
def s3_mock(aws_credentials, aws_region_name: str) -> BaseClient:
    with mock_s3():
        client = aws.boto3.client("s3", region_name=aws_region_name)
        yield client


@pytest.fixture
def s3_bucket(s3_mock: BaseClient, aws_s3_bucket_name: str) -> str:
    bucket_name: str = aws_s3_bucket_name
    s3_mock.create_bucket(Bucket=bucket_name)
    return bucket_name


@pytest.fixture
def pandas_s3_datasource(
    empty_data_context, s3_mock, s3_bucket: str
) -> PandasS3Datasource:
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "alex_20200809_1000.csv",
        "eugene_20200809_1500.csv",
        "james_20200811_1009.csv",
        "abe_20200809_1040.csv",
        "will_20200809_1002.csv",
        "james_20200713_1567.csv",
        "eugene_20201129_1900.csv",
        "will_20200810_1001.csv",
        "james_20200810_1003.csv",
        "alex_20200819_1300.csv",
        "subfolder/for_recursive_search.csv",
    ]

    for key in keys:
        s3_mock.put_object(
            Bucket=s3_bucket,
            Body=test_df.to_csv(index=False).encode("utf-8"),
            Key=key,
        )

    pandas_s3_datasource = PandasS3Datasource(  # type: ignore[call-arg]
        name="pandas_s3_datasource",
        bucket=s3_bucket,
    )
    pandas_s3_datasource._data_context = empty_data_context

    return pandas_s3_datasource


@pytest.fixture
def csv_asset(pandas_s3_datasource: PandasS3Datasource) -> _FilePathDataAsset:
    asset = pandas_s3_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(?P<name>.+)_(?P<timestamp>.+)_(?P<price>\d{4})\.csv",
    )
    return asset


@pytest.fixture
def bad_regex_config(csv_asset: CSVAsset) -> tuple[re.Pattern, str]:
    regex = re.compile(
        r"(?P<name>.+)_(?P<ssn>\d{9})_(?P<timestamp>.+)_(?P<price>\d{4})\.csv"
    )
    data_connector: S3DataConnector = cast(S3DataConnector, csv_asset._data_connector)
    test_connection_error_message = f"""No file in bucket "{csv_asset.datasource.bucket}" with prefix "{data_connector._prefix}" matched regular expressions pattern "{regex.pattern}" using delimiter "{data_connector._delimiter}" for DataAsset "{csv_asset.name}"."""
    return regex, test_connection_error_message


@pytest.mark.integration
def test_construct_pandas_s3_datasource(pandas_s3_datasource: PandasS3Datasource):
    assert pandas_s3_datasource.name == "pandas_s3_datasource"


@pytest.mark.integration
def test_add_csv_asset_to_datasource(pandas_s3_datasource: PandasS3Datasource):
    asset = pandas_s3_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(.+)_(.+)_(\d{4})\.csv",
    )
    assert asset.name == "csv_asset"
    assert asset.batching_regex.match("random string") is None
    assert asset.batching_regex.match("alex_20200819_13D0.csv") is None
    m1 = asset.batching_regex.match("alex_20200819_1300.csv")
    assert m1 is not None


@pytest.mark.integration
def test_construct_csv_asset_directly():
    # noinspection PyTypeChecker
    asset = CSVAsset(
        name="csv_asset",
        batching_regex=r"(.+)_(.+)_(\d{4})\.csv",
    )
    assert asset.name == "csv_asset"
    assert asset.batching_regex.match("random string") is None
    assert asset.batching_regex.match("alex_20200819_13D0.csv") is None
    m1 = asset.batching_regex.match("alex_20200819_1300.csv")
    assert m1 is not None


@pytest.mark.unit
def test_invalid_connect_options(pandas_s3_datasource: PandasS3Datasource):
    with pytest.raises(pydantic.ValidationError) as exc_info:
        pandas_s3_datasource.add_csv_asset(  # type: ignore[call-arg]
            name="csv_asset",
            batching_regex=r"(.+)_(.+)_(\d{4})\.csv",
            extra_field="invalid",
        )

    error_dicts = exc_info.value.errors()
    print(pf(error_dicts))
    assert [
        {
            "loc": ("extra_field",),
            "msg": "extra fields not permitted",
            "type": "value_error.extra",
        }
    ] == error_dicts


@pytest.mark.unit
@pytest.mark.parametrize(
    ["connect_option_kwargs", "expected_error_dicts"],
    [
        param(
            {"my_prefix": "/"},
            [
                {
                    "loc": ("my_prefix",),
                    "msg": "extra fields not permitted",
                    "type": "value_error.extra",
                }
            ],
            id="extra_fields",
        ),
        param(
            {"s3_delimiter": ["/only", "/one_delimiter"]},
            [
                {
                    "loc": ("s3_delimiter",),
                    "msg": "str type expected",
                    "type": "type_error.str",
                },
            ],
            id="wrong_type",
        ),
    ],
)
def test_invalid_connect_options_value(
    pandas_s3_datasource: PandasS3Datasource,
    connect_option_kwargs: dict,
    expected_error_dicts: list[dict],
):
    with pytest.raises(pydantic.ValidationError) as exc_info:
        pandas_s3_datasource.add_csv_asset(
            name="csv_asset",
            batching_regex=r"(.+)_(.+)_(\d{4})\.csv",
            **connect_option_kwargs,
        )

    print(f"Exception raised:\n\t{repr(exc_info.value)}")
    error_dicts = exc_info.value.errors()
    print(pf(error_dicts))
    assert expected_error_dicts == error_dicts


@pytest.mark.unit
@pytest.mark.parametrize(
    "connect_options",
    [
        param({}, id="default connect options"),
        param({"s3_prefix": ""}, id="prefix ''"),
        param({"s3_delimiter": "/"}, id="s3_delimiter '/'"),
        # param({"s3_prefix": "non_default"}, id="s3_prefix 'non_default'"), # TODO: what prefix should I test?
        param(
            {"s3_prefix": "", "s3_delimiter": "/", "s3_max_keys": 20},
            id="all options",
        ),
    ],
)
def test_asset_connect_options_in_repr(
    pandas_s3_datasource: PandasS3Datasource, connect_options: dict
):
    print(f"connect_options\n{pf(connect_options)}\n")

    asset = pandas_s3_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(.+)_(.+)_(\d{4})\.csv",
        **connect_options,
    )

    print(f"__repr__\n{repr(asset)}\n")
    asset_as_str = str(asset)
    print(f"__str__\n{asset_as_str}\n")

    for option_name, option_value in connect_options.items():
        assert option_name in asset_as_str
        assert str(option_value) in asset_as_str
    if not connect_options:
        # if no connect options are provided the defaults should be used and should not
        # be part of any serialization. str(asset) == asset.yaml()
        assert "connect_options" not in asset_as_str


@pytest.mark.integration
def test_csv_asset_with_batching_regex_unnamed_parameters(
    pandas_s3_datasource: PandasS3Datasource,
):
    asset = pandas_s3_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(.+)_(.+)_(\d{4})\.csv",
    )
    options = asset.batch_request_options
    assert options == (
        "batch_request_param_1",
        "batch_request_param_2",
        "batch_request_param_3",
        "path",
    )


@pytest.mark.integration
def test_csv_asset_with_batching_regex_named_parameters(
    pandas_s3_datasource: PandasS3Datasource,
):
    asset = pandas_s3_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(?P<name>.+)_(?P<timestamp>.+)_(?P<price>\d{4})\.csv",
    )
    options = asset.batch_request_options
    assert options == (
        "name",
        "timestamp",
        "price",
        "path",
    )


@pytest.mark.integration
def test_csv_asset_with_some_batching_regex_named_parameters(
    pandas_s3_datasource: PandasS3Datasource,
):
    asset = pandas_s3_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(?P<name>.+)_(.+)_(?P<price>\d{4})\.csv",
    )
    options = asset.batch_request_options
    assert options == (
        "name",
        "batch_request_param_2",
        "price",
        "path",
    )


@pytest.mark.integration
def test_csv_asset_with_non_string_batching_regex_named_parameters(
    pandas_s3_datasource: PandasS3Datasource,
):
    asset = pandas_s3_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(.+)_(.+)_(?P<price>\d{4})\.csv",
    )
    with pytest.raises(ge_exceptions.InvalidBatchRequestError):
        # price is an int which will raise an error
        asset.build_batch_request(
            {"name": "alex", "timestamp": "1234567890", "price": 1300}
        )


@pytest.mark.integration
def test_get_batch_list_from_fully_specified_batch_request(
    pandas_s3_datasource: PandasS3Datasource,
):
    asset = pandas_s3_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"(?P<name>.+)_(?P<timestamp>.+)_(?P<price>\d{4})\.csv",
    )

    request = asset.build_batch_request(
        {"name": "alex", "timestamp": "20200819", "price": "1300"}
    )
    batches = asset.get_batch_list_from_batch_request(request)
    assert len(batches) == 1
    batch = batches[0]
    assert batch.batch_request.datasource_name == pandas_s3_datasource.name
    assert batch.batch_request.data_asset_name == asset.name
    assert batch.batch_request.options == {
        "path": "alex_20200819_1300.csv",
        "name": "alex",
        "timestamp": "20200819",
        "price": "1300",
    }
    assert batch.metadata == {
        "path": "alex_20200819_1300.csv",
        "name": "alex",
        "timestamp": "20200819",
        "price": "1300",
    }
    assert (
        batch.id
        == "pandas_s3_datasource-csv_asset-name_alex-timestamp_20200819-price_1300"
    )

    request = asset.build_batch_request({"name": "alex"})
    batches = asset.get_batch_list_from_batch_request(request)
    assert len(batches) == 2


@pytest.mark.integration
def test_test_connection_failures(
    s3_mock,
    pandas_s3_datasource: PandasS3Datasource,
    bad_regex_config: tuple[re.Pattern, str],
):
    regex, test_connection_error_message = bad_regex_config
    csv_asset = CSVAsset(  # type: ignore[call-arg]
        name="csv_asset",
        batching_regex=regex,
    )
    csv_asset._datasource = pandas_s3_datasource
    pandas_s3_datasource.assets = [
        csv_asset,
    ]
    csv_asset._data_connector = S3DataConnector(
        datasource_name=pandas_s3_datasource.name,
        data_asset_name=csv_asset.name,
        batching_regex=re.compile(regex),
        s3_client=s3_mock,
        bucket=pandas_s3_datasource.bucket,
        file_path_template_map_fn=S3Url.OBJECT_URL_TEMPLATE.format,
    )
    csv_asset._test_connection_error_message = test_connection_error_message

    with pytest.raises(TestConnectionError) as e:
        pandas_s3_datasource.test_connection()

    assert str(e.value) == str(test_connection_error_message)


@pytest.mark.integration
def test_add_csv_asset_with_recursive_file_discovery_to_datasource(
    pandas_s3_datasource: PandasS3Datasource,
):
    """
    Tests that files from the subfolder(s) is returned
    when the s3_recursive_file_discovery-flag is set to True

    This makes the list_keys-function search and return files also
    from sub-directories on S3, not just the files in the folder
    specified with the s3_name_starts_with-parameter
    """
    no_recursion_asset = pandas_s3_datasource.add_csv_asset(
        name="csv_asset_not_recursive",
        batching_regex=r".*",
        s3_recursive_file_discovery=False,
    )
    found_files_without_recursion = len(
        no_recursion_asset.get_batch_list_from_batch_request(
            no_recursion_asset.build_batch_request()
        )
    )
    recursion_asset = pandas_s3_datasource.add_csv_asset(
        name="csv_asset_recursive",
        batching_regex=r".*",
        s3_recursive_file_discovery=True,
    )
    found_files_with_recursion = len(
        recursion_asset.get_batch_list_from_batch_request(
            recursion_asset.build_batch_request()
        )
    )
    # Only 1 additional file was added to the subfolder
    assert found_files_without_recursion + 1 == found_files_with_recursion
    recursion_match = recursion_asset.batching_regex.match(".*/.*.csv")
    assert recursion_match is not None
