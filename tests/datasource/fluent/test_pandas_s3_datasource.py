from __future__ import annotations

import logging
from pprint import pformat as pf
from typing import TYPE_CHECKING, List

import pandas as pd
import pytest
from pytest import param

import great_expectations.exceptions as ge_exceptions
from great_expectations.compatibility import aws, pydantic
from great_expectations.datasource.fluent import PandasS3Datasource
from great_expectations.datasource.fluent.data_asset.path.pandas.generated_assets import CSVAsset
from great_expectations.datasource.fluent.data_asset.path.path_data_asset import (
    PathDataAsset,
)
from great_expectations.datasource.fluent.dynamic_pandas import PANDAS_VERSION

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
def aws_s3_bucket_name() -> str:
    return "test_bucket"


@pytest.fixture
def s3_bucket(s3_mock: BaseClient, aws_s3_bucket_name: str) -> str:
    bucket_name: str = aws_s3_bucket_name
    s3_mock.create_bucket(Bucket=bucket_name)
    return bucket_name


@pytest.fixture
def pandas_s3_datasource(empty_data_context, s3_mock, s3_bucket: str) -> PandasS3Datasource:
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "yellow_tripdata_sample_2024-01.csv",
        "yellow_tripdata_sample_2024-02.csv",
        "yellow_tripdata_sample_2024-03.csv",
        "yellow_tripdata_sample_2024-04.csv",
        "yellow_tripdata_sample_2024-05.csv",
        "yellow_tripdata_sample_2024-06.csv",
        "yellow_tripdata_sample_2024-07.csv",
        "yellow_tripdata_sample_2024-08.csv",
        "yellow_tripdata_sample_2024-09.csv",
        "yellow_tripdata_sample_2024-10.csv",
        "yellow_tripdata_sample_2024-11.csv",
        "yellow_tripdata_sample_2024-12.csv",
        "subfolder/for_recursive_search.csv",
    ]

    for key in keys:
        s3_mock.put_object(
            Bucket=s3_bucket,
            Body=test_df.to_csv(index=False).encode("utf-8"),
            Key=key,
        )

    pandas_s3_datasource = PandasS3Datasource(
        name="pandas_s3_datasource",
        bucket=s3_bucket,
    )
    pandas_s3_datasource._data_context = empty_data_context

    return pandas_s3_datasource


@pytest.fixture
def csv_asset(pandas_s3_datasource: PandasS3Datasource) -> PathDataAsset:
    asset = pandas_s3_datasource.add_csv_asset(
        name="csv_asset",
    )
    return asset


@pytest.mark.aws_deps
def test_construct_pandas_s3_datasource(pandas_s3_datasource: PandasS3Datasource, aws_credentials):
    assert pandas_s3_datasource.name == "pandas_s3_datasource"


@pytest.mark.aws_deps
def test_add_csv_asset_to_datasource(pandas_s3_datasource: PandasS3Datasource, aws_credentials):
    asset = pandas_s3_datasource.add_csv_asset(
        name="csv_asset",
    )
    assert asset.name == "csv_asset"


@pytest.mark.unit
def test_construct_csv_asset_directly():
    # noinspection PyTypeChecker
    asset = CSVAsset(
        name="csv_asset",
    )
    assert asset.name == "csv_asset"


@pytest.mark.aws_deps
def test_invalid_connect_options(pandas_s3_datasource: PandasS3Datasource, aws_credentials):
    with pytest.raises(pydantic.ValidationError) as exc_info:
        pandas_s3_datasource.add_csv_asset(  # type: ignore[call-arg]
            name="csv_asset",
            extra_field="invalid",
        )

    error_dicts = exc_info.value.errors()
    print(pf(error_dicts))
    assert error_dicts == [
        {
            "loc": ("extra_field",),
            "msg": "extra fields not permitted",
            "type": "value_error.extra",
        }
    ]


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
            **connect_option_kwargs,
        )

    print(f"Exception raised:\n\t{exc_info.value!r}")
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
        # param({"s3_prefix": "non_default"}, id="s3_prefix 'non_default'"), # TODO: what prefix should I test?  # noqa: E501
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
        **connect_options,
    )

    print(f"__repr__\n{asset!r}\n")
    asset_as_str = str(asset)
    print(f"__str__\n{asset_as_str}\n")

    for option_name, option_value in connect_options.items():
        assert option_name in asset_as_str
        assert str(option_value) in asset_as_str
    if not connect_options:
        # if no connect options are provided the defaults should be used and should not
        # be part of any serialization. str(asset) == asset.yaml()
        assert "connect_options" not in asset_as_str


@pytest.mark.aws_deps
def test_csv_asset_with_batching_regex_named_parameters(
    pandas_s3_datasource: PandasS3Datasource, aws_credentials
):
    asset = pandas_s3_datasource.add_csv_asset(
        name="csv_asset",
    )
    batching_regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
    batch_def = asset.add_batch_definition_monthly(name="batch def", regex=batching_regex)
    options = asset.get_batch_parameters_keys(partitioner=batch_def.partitioner)
    assert options == ("path", "year", "month")


@pytest.mark.aws_deps
def test_csv_asset_with_non_string_batching_regex_named_parameters(
    pandas_s3_datasource: PandasS3Datasource, aws_credentials
):
    asset = pandas_s3_datasource.add_csv_asset(
        name="csv_asset",
    )
    with pytest.raises(ge_exceptions.InvalidBatchRequestError):
        # price is an int which will raise an error
        asset.build_batch_request({"year": "2024", "month": 5})


@pytest.mark.aws_deps
def test_get_batch_list_from_fully_specified_batch_request(
    pandas_s3_datasource: PandasS3Datasource, aws_credentials
):
    asset = pandas_s3_datasource.add_csv_asset(
        name="csv_asset",
    )

    batching_regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
    batch_def = asset.add_batch_definition_monthly(name="batch def", regex=batching_regex)
    batch_parameters = {"year": "2024", "month": "05"}
    batch = batch_def.get_batch(batch_parameters=batch_parameters)

    assert batch.batch_request.datasource_name == pandas_s3_datasource.name
    assert batch.batch_request.data_asset_name == asset.name

    path = "yellow_tripdata_sample_2024-05.csv"
    assert batch.batch_request.options == {"path": path, "year": "2024", "month": "05"}
    assert batch.metadata == {"path": path, "year": "2024", "month": "05"}
    assert batch.id == "pandas_s3_datasource-csv_asset-year_2024-month_05"


@pytest.mark.aws_deps
def test_add_csv_asset_with_recursive_file_discovery_to_datasource(
    pandas_s3_datasource: PandasS3Datasource, aws_credentials
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
        s3_recursive_file_discovery=False,
    )
    found_files_without_recursion = len(
        no_recursion_asset.get_batch_identifiers_list(no_recursion_asset.build_batch_request())
    )
    recursion_asset = pandas_s3_datasource.add_csv_asset(
        name="csv_asset_recursive",
        s3_recursive_file_discovery=True,
    )
    found_files_with_recursion = len(
        recursion_asset.get_batch_identifiers_list(recursion_asset.build_batch_request())
    )
    # Only 1 additional file was added to the subfolder
    assert found_files_without_recursion + 1 == found_files_with_recursion
