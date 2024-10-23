import pathlib
import re
from typing import Final, List, Union

import pytest

from great_expectations.alias_types import PathStr
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.partitioners import (
    ColumnPartitionerDaily,
    ColumnPartitionerMonthly,
    ColumnPartitionerYearly,
    FileNamePartitionerDaily,
    FileNamePartitionerMonthly,
    FileNamePartitionerPath,
    FileNamePartitionerYearly,
)
from great_expectations.datasource.fluent import Datasource
from great_expectations.datasource.fluent.data_asset.path.file_asset import (
    AmbiguousPathError,
    PathNotFoundError,
    RegexMissingRequiredGroupsError,
    RegexUnknownGroupsError,
)
from great_expectations.datasource.fluent.data_asset.path.pandas.generated_assets import (
    CSVAsset as PandasCSVAsset,
)
from great_expectations.datasource.fluent.data_asset.path.pandas.generated_assets import (
    ExcelAsset,
    FWFAsset,
)
from great_expectations.datasource.fluent.data_asset.path.pandas.generated_assets import (
    JSONAsset as PandasJSONAsset,
)
from great_expectations.datasource.fluent.data_asset.path.pandas.generated_assets import (
    ORCAsset as PandasORCAsset,
)
from great_expectations.datasource.fluent.data_asset.path.pandas.generated_assets import (
    ParquetAsset as PandasParquetAsset,
)
from great_expectations.datasource.fluent.data_asset.path.path_data_asset import (
    PathDataAsset,
)
from great_expectations.datasource.fluent.data_asset.path.spark.csv_asset import (
    CSVAsset as SparkCSVAsset,
)
from great_expectations.datasource.fluent.data_asset.path.spark.csv_asset import (
    DirectoryCSVAsset,
)
from great_expectations.datasource.fluent.data_asset.path.spark.delta_asset import (
    DeltaAsset,
    DirectoryDeltaAsset,
)
from great_expectations.datasource.fluent.data_asset.path.spark.json_asset import (
    DirectoryJSONAsset,
)
from great_expectations.datasource.fluent.data_asset.path.spark.json_asset import (
    JSONAsset as SparkJSONAsset,
)
from great_expectations.datasource.fluent.data_asset.path.spark.orc_asset import (
    DirectoryORCAsset,
)
from great_expectations.datasource.fluent.data_asset.path.spark.orc_asset import (
    ORCAsset as SparkORCAsset,
)
from great_expectations.datasource.fluent.data_asset.path.spark.parquet_asset import (
    DirectoryParquetAsset,
)
from great_expectations.datasource.fluent.data_asset.path.spark.parquet_asset import (
    ParquetAsset as SparkParquetAsset,
)
from great_expectations.datasource.fluent.data_asset.path.spark.text_asset import (
    DirectoryTextAsset,
    TextAsset,
)
from great_expectations.datasource.fluent.data_connector import FilePathDataConnector
from great_expectations.datasource.fluent.pandas_filesystem_datasource import (
    PandasFilesystemDatasource,
)


@pytest.fixture
def validated_pandas_filesystem_datasource(
    pandas_filesystem_datasource: PandasFilesystemDatasource,
):
    years = ["2018", "2019", "2020"]
    all_files: List[str] = [
        file_name.stem
        for file_name in list(pathlib.Path(pandas_filesystem_datasource.base_directory).iterdir())
    ]
    # assert there are 12 files for each year
    for year in years:
        files_for_year = [
            file_name
            for file_name in all_files
            if file_name.find(f"yellow_tripdata_sample_{year}") == 0
        ]
        assert len(files_for_year) == 12
    return pandas_filesystem_datasource


@pytest.mark.filesystem
@pytest.mark.parametrize(
    "batching_regex",
    [
        pytest.param(r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv", id="String"),
        pytest.param(
            re.compile(r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"),
            id="re.Pattern",
        ),
    ],
)
def test_get_batch_identifiers_list__sort_ascending(
    validated_pandas_filesystem_datasource: PandasFilesystemDatasource,
    batching_regex: Union[str, re.Pattern],
):
    """Verify that get_batch_identifiers_list respects a partitioner's ascending sort order.

    NOTE: we just happen to be using pandas as the concrete class.
    """
    asset = validated_pandas_filesystem_datasource.add_csv_asset(
        name="csv_asset",
    )
    batch_definition = asset.add_batch_definition_monthly(
        name="foo", sort_ascending=True, regex=batching_regex
    )
    batch_request = batch_definition.build_batch_request()

    batch_identifiers_list = asset.get_batch_identifiers_list(batch_request)

    expected_years = ["2018"] * 12 + ["2019"] * 12 + ["2020"] * 12
    expected_months = [format(m, "02d") for m in range(1, 13)] * 3

    assert (len(batch_identifiers_list)) == 36
    for i, batch_identifiers in enumerate(batch_identifiers_list):
        assert batch_identifiers["year"] == str(expected_years[i])
        assert batch_identifiers["month"] == str(expected_months[i])


@pytest.mark.filesystem
@pytest.mark.parametrize(
    "batching_regex",
    [
        pytest.param(r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv", id="String"),
        pytest.param(
            re.compile(r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"),
            id="re.Pattern",
        ),
    ],
)
def test_get_batch_identifiers_list__sort_descending(
    validated_pandas_filesystem_datasource: PandasFilesystemDatasource,
    batching_regex: Union[str, re.Pattern],
):
    """Verify that get_batch_identifiers_list respects a partitioner's descending sort order.

    NOTE: we just happen to be using pandas as the concrete class.
    """
    asset = validated_pandas_filesystem_datasource.add_csv_asset(
        name="csv_asset",
    )
    batch_definition = asset.add_batch_definition_monthly(
        name="foo", regex=batching_regex, sort_ascending=False
    )
    batch_request = batch_definition.build_batch_request()

    batch_identifiers_list = asset.get_batch_identifiers_list(batch_request)

    expected_years = list(reversed(["2018"] * 12 + ["2019"] * 12 + ["2020"] * 12))
    expected_months = list(reversed([format(m, "02d") for m in range(1, 13)] * 3))

    assert (len(batch_identifiers_list)) == 36
    for i, batch_identifiers in enumerate(batch_identifiers_list):
        assert batch_identifiers["year"] == str(expected_years[i])
        assert batch_identifiers["month"] == str(expected_months[i])


@pytest.fixture
def datasource(mocker):
    # the API required by these tests is not specific to Spark or Pandas
    return mocker.Mock(spec=Datasource)


@pytest.fixture
def file_path_data_connector(mocker):
    return mocker.Mock(spec=FilePathDataConnector)


@pytest.fixture
def asset(request, datasource, file_path_data_connector) -> PathDataAsset:
    asset = request.param
    # since we're parametrizing these tests multiple ways, this object
    # will likely be reused, so we make sure the state we care about is reset:
    asset.batch_definitions = []
    asset._datasource = datasource  # same pattern Datasource uses to init Asset
    asset._data_connector = file_path_data_connector
    return asset


PATH_NAME = "data_2022-01.csv"


def _path_asset_parameters():
    return [
        # Spark Assets
        pytest.param(SparkCSVAsset(name="test_asset"), id="Spark CSV Asset"),
        pytest.param(SparkParquetAsset(name="test_asset"), id="Spark Parquet Asset"),
        pytest.param(SparkORCAsset(name="test_asset"), id="Spark ORC Asset"),
        pytest.param(SparkJSONAsset(name="test_asset"), id="Spark JSON Asset"),
        pytest.param(TextAsset(name="test_asset"), id="Spark Text Asset"),
        pytest.param(DeltaAsset(name="test_asset"), id="Spark Delta Asset"),
        # Pandas Assets
        pytest.param(PandasCSVAsset(name="test_asset"), id="Pandas CSV Asset"),
        pytest.param(ExcelAsset(name="test_asset"), id="Pandas Excel Asset"),
        pytest.param(PandasJSONAsset(name="test_asset"), id="Pandas JSON Asset"),
        pytest.param(PandasORCAsset(name="test_asset"), id="Pandas ORC Asset"),
        pytest.param(PandasParquetAsset(name="test_asset"), id="Pandas Parquet Asset"),
        pytest.param(FWFAsset(name="test_asset"), id="Pandas FWF Asset"),
    ]


@pytest.mark.unit
@pytest.mark.parametrize(
    "path",
    [
        pytest.param(PATH_NAME, id="String Path"),
        pytest.param(pathlib.Path(PATH_NAME), id="Pathlib Path"),
    ],
)
@pytest.mark.parametrize("asset", _path_asset_parameters(), indirect=["asset"])
def test_add_batch_definition_fluent_file_path__add_batch_definition_path_success(
    datasource,
    asset,
    path: PathStr,
    file_path_data_connector,
):
    # arrange
    name = "batch_def_name"
    expected_regex = re.compile(str(path))
    expected_batch_definition = BatchDefinition(
        name=name, partitioner=FileNamePartitionerPath(regex=expected_regex)
    )
    assert isinstance(expected_batch_definition.partitioner, FileNamePartitionerPath)
    datasource.add_batch_definition.return_value = expected_batch_definition
    file_path_data_connector.get_matched_data_references.return_value = [PATH_NAME]

    # act
    batch_definition = asset.add_batch_definition_path(name=name, path=path)

    # assert
    assert batch_definition == expected_batch_definition
    datasource.add_batch_definition.assert_called_once_with(expected_batch_definition)
    file_path_data_connector.get_matched_data_references.assert_called_once_with(
        regex=expected_regex
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "path",
    [
        pytest.param(PATH_NAME, id="String Path"),
        pytest.param(pathlib.Path(PATH_NAME), id="Pathlib Path"),
    ],
)
@pytest.mark.parametrize("asset", _path_asset_parameters(), indirect=["asset"])
def test_add_batch_definition_fluent_file_path__add_batch_definition_path_fails_if_no_file_is_found(
    datasource, asset, path: PathStr, file_path_data_connector
):
    # arrange
    name = "batch_def_name"
    expected_regex = re.compile(str(path))

    file_path_data_connector.get_matched_data_references.return_value = []

    # act
    with pytest.raises(PathNotFoundError) as error:
        asset.add_batch_definition_path(name=name, path=path)

        # assert -- we need to still be inside context manager to access this instance attribute
        assert error.path == path  # type: ignore[attr-defined]  # pytest.raises obscures type

    file_path_data_connector.get_matched_data_references.assert_called_once_with(
        regex=expected_regex
    )
    datasource.add_batch_definition.assert_not_called()


@pytest.mark.unit
@pytest.mark.parametrize(
    "path",
    [
        pytest.param(PATH_NAME, id="String Path"),
        pytest.param(pathlib.Path(PATH_NAME), id="Pathlib Path"),
    ],
)
@pytest.mark.parametrize("asset", _path_asset_parameters(), indirect=["asset"])
def test_add_batch_definition_fluent_file_path__add_batch_definition_path_fails_if_multiple_files_are_found(  # noqa: E501
    datasource, asset, path: PathStr, file_path_data_connector
):
    """This edge case occurs if a user doesn't actually provide a path, but
    instead a regex with multiple matches."""
    # arrange
    name = "batch_def_name"
    expected_regex = re.compile(str(path))
    file_path_data_connector.get_matched_data_references.return_value = [
        "data_reference_one",
        "data_reference_two",
    ]

    # act
    with pytest.raises(AmbiguousPathError) as error:
        asset.add_batch_definition_path(name=name, path=path)

        # assert -- we need to still be inside context manager to access this instance attribute
        assert error.path == path  # type: ignore[attr-defined]  # pytest.raises obscures type

    file_path_data_connector.get_matched_data_references.assert_called_once_with(
        regex=expected_regex
    )
    datasource.add_batch_definition.assert_not_called()


@pytest.mark.unit
@pytest.mark.parametrize("asset", _path_asset_parameters(), indirect=["asset"])
@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize(
    "batching_regex",
    [
        pytest.param(r"data_(?P<year>\d{4}).csv", id="String"),
        pytest.param(re.compile(r"data_(?P<year>\d{4}).csv"), id="re.Pattern"),
    ],
)
def test_add_batch_definition_fluent_file_path__add_batch_definition_yearly_success(
    datasource, asset, sort, batching_regex
):
    # arrange
    name = "batch_def_name"
    expected_batch_definition = BatchDefinition(
        name=name,
        partitioner=FileNamePartitionerYearly(regex=batching_regex, sort_ascending=sort),
    )
    assert isinstance(expected_batch_definition.partitioner, FileNamePartitionerYearly)
    datasource.add_batch_definition.return_value = expected_batch_definition

    # act
    batch_definition = asset.add_batch_definition_yearly(
        name=name, regex=batching_regex, sort_ascending=sort
    )

    # assert
    assert batch_definition == expected_batch_definition
    datasource.add_batch_definition.assert_called_once_with(expected_batch_definition)


@pytest.mark.unit
@pytest.mark.parametrize("asset", _path_asset_parameters(), indirect=["asset"])
@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize(
    "batching_regex",
    [
        pytest.param(r"data_2024.csv", id="String"),
        pytest.param(re.compile(r"data_2024.csv"), id="re.Pattern"),
    ],
)
def test_add_batch_definition_fluent_file_path__add_batch_definition_yearly_fails_if_required_group_is_missing(  # noqa: E501
    datasource, asset, sort, batching_regex
):
    # arrange
    name = "batch_def_name"

    # act
    with pytest.raises(RegexMissingRequiredGroupsError) as error:
        asset.add_batch_definition_yearly(name=name, regex=batching_regex, sort_ascending=sort)

        # assert -- we need to still be inside context manager to access this instance attribute
        assert error.missing_groups == {"year"}

    datasource.add_batch_definition.assert_not_called()


@pytest.mark.unit
@pytest.mark.parametrize("asset", _path_asset_parameters(), indirect=["asset"])
@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize(
    "batching_regex",
    [
        pytest.param(r"data_(?P<year>\d{4})-(?P<foo>\d{4}).csv", id="String"),
        pytest.param(re.compile(r"data_(?P<year>\d{4})-(?P<foo>\d{4}).csv"), id="re.Pattern"),
    ],
)
def test_add_batch_definition_fluent_file_path__add_batch_definition_yearly_fails_if_unknown_groups_are_found(  # noqa: E501
    datasource, asset, sort, batching_regex
):
    # arrange
    name = "batch_def_name"

    # act
    with pytest.raises(RegexUnknownGroupsError) as error:
        asset.add_batch_definition_yearly(name=name, regex=batching_regex, sort_ascending=sort)

        # assert -- we need to still be inside context manager to access this instance attribute
        assert error.unknown_groups == {"foo"}

    datasource.add_batch_definition.assert_not_called()


@pytest.mark.unit
@pytest.mark.parametrize("asset", _path_asset_parameters(), indirect=["asset"])
@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize(
    "batching_regex",
    [
        pytest.param(r"data_(?P<year>\d{4})-(?P<month>\d{2}).csv", id="String"),
        pytest.param(re.compile(r"data_(?P<year>\d{4})-(?P<month>\d{2}).csv"), id="re.Pattern"),
    ],
)
def test_add_batch_definition_fluent_file_path__add_batch_definition_monthly_success(
    datasource, asset, sort, batching_regex
):
    # arrange
    name = "batch_def_name"
    expected_batch_definition = BatchDefinition(
        name=name,
        partitioner=FileNamePartitionerMonthly(regex=batching_regex, sort_ascending=sort),
    )
    assert isinstance(expected_batch_definition.partitioner, FileNamePartitionerMonthly)

    datasource.add_batch_definition.return_value = expected_batch_definition

    # act
    batch_definition = asset.add_batch_definition_monthly(
        name=name, regex=batching_regex, sort_ascending=sort
    )

    # assert
    assert batch_definition == expected_batch_definition
    datasource.add_batch_definition.assert_called_once_with(expected_batch_definition)


@pytest.mark.unit
@pytest.mark.parametrize("asset", _path_asset_parameters(), indirect=["asset"])
@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize(
    "batching_regex",
    [
        pytest.param(r"data_(?P<year>\d{4})-(?P<month>\d{2}).csv", id="String"),
        pytest.param(re.compile(r"data_(?P<year>\d{4})-(?P<month>\d{2}).csv"), id="re.Pattern"),
    ],
)
def test_add_batch_definition_fluent_file_path__add_batch_definition_monthly_fails_if_required_group_is_missing(  # noqa: E501
    datasource, asset, sort, batching_regex
):
    # arrange
    name = "batch_def_name"
    batching_regex = re.compile(r"data_2024-01.csv")

    # act
    with pytest.raises(RegexMissingRequiredGroupsError) as error:
        asset.add_batch_definition_monthly(name=name, regex=batching_regex, sort_ascending=sort)

        # assert -- we need to still be inside context manager to access this instance attribute
        assert error.missing_groups == {"year", "month"}

    datasource.add_batch_definition.assert_not_called()


@pytest.mark.unit
@pytest.mark.parametrize("asset", _path_asset_parameters(), indirect=["asset"])
@pytest.mark.parametrize("sort", [True, False])
def test_add_batch_definition_fluent_file_path__add_batch_definition_monthly_fails_if_unknown_groups_are_found(  # noqa: E501
    datasource, asset, sort
):
    # arrange
    name = "batch_def_name"
    batching_regex = re.compile(r"data_(?P<year>\d{4})-(?P<month>\d{2})-(?P<foo>\d{4}).csv")

    # act
    with pytest.raises(RegexUnknownGroupsError) as error:
        asset.add_batch_definition_monthly(name=name, regex=batching_regex, sort_ascending=sort)

        # assert -- we need to still be inside context manager to access this instance attribute
        assert error.unknown_groups == {"foo"}

    datasource.add_batch_definition.assert_not_called()


@pytest.mark.unit
@pytest.mark.parametrize("asset", _path_asset_parameters(), indirect=["asset"])
@pytest.mark.parametrize("sort", [True, False])
def test_add_batch_definition_fluent_file_path__add_batch_definition_daily_success(
    datasource, asset, sort
):
    # arrange
    name = "batch_def_name"
    batching_regex = re.compile(r"data_(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2}).csv")
    expected_batch_definition = BatchDefinition(
        name=name,
        partitioner=FileNamePartitionerDaily(regex=batching_regex, sort_ascending=sort),
    )
    assert isinstance(expected_batch_definition.partitioner, FileNamePartitionerDaily)

    datasource.add_batch_definition.return_value = expected_batch_definition

    # act
    batch_definition = asset.add_batch_definition_daily(
        name=name, regex=batching_regex, sort_ascending=sort
    )

    # assert
    assert batch_definition == expected_batch_definition
    datasource.add_batch_definition.assert_called_once_with(expected_batch_definition)


@pytest.mark.unit
@pytest.mark.parametrize("asset", _path_asset_parameters(), indirect=["asset"])
@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize(
    "batching_regex",
    [
        pytest.param(r"data_2024.csv", id="String"),
        pytest.param(re.compile(r"data_2024.csv"), id="re.Pattern"),
    ],
)
def test_add_batch_definition_fluent_file_path__add_batch_definition_daily_fails_if_required_group_is_missing(  # noqa: E501
    datasource, asset, sort, batching_regex
):
    # arrange
    name = "batch_def_name"

    # act
    with pytest.raises(RegexMissingRequiredGroupsError) as error:
        asset.add_batch_definition_daily(name=name, regex=batching_regex, sort_ascending=sort)

        # assert -- we need to still be inside context manager to access this instance attribute
        assert error.missing_groups == {"year", "month", "day"}

    datasource.add_batch_definition.assert_not_called()


@pytest.mark.unit
@pytest.mark.parametrize("asset", _path_asset_parameters(), indirect=["asset"])
@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize(
    "batching_regex",
    [
        pytest.param(
            r"data_(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})-(?P<foo>\d{4}).csv", id="String"
        ),
        pytest.param(
            re.compile(r"data_(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})-(?P<foo>\d{4}).csv"),
            id="re.Pattern",
        ),
    ],
)
def test_add_batch_definition_fluent_file_path__add_batch_definition_daily_fails_if_unknown_groups_are_found(  # noqa: E501
    datasource, asset, sort, batching_regex
):
    # arrange
    name = "batch_def_name"
    batching_regex = re.compile(
        r"data_(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})-(?P<foo>\d{4}).csv"
    )

    # act
    with pytest.raises(RegexUnknownGroupsError) as error:
        asset.add_batch_definition_daily(name=name, regex=batching_regex, sort_ascending=sort)

        # assert -- we need to still be inside context manager to access this instance attribute
        assert error.unknown_groups == {"foo"}

    datasource.add_batch_definition.assert_not_called()


DATA_DIRECTORY: Final[pathlib.Path] = pathlib.Path("/data/my_directory")


def _directory_asset_parameters():
    return [
        pytest.param(
            DirectoryCSVAsset(name="test_asset", data_directory=DATA_DIRECTORY), id="CSV Asset"
        ),
        pytest.param(
            DirectoryParquetAsset(name="test_asset", data_directory=DATA_DIRECTORY),
            id="Parquet Asset",
        ),
        pytest.param(
            DirectoryORCAsset(name="test_asset", data_directory=DATA_DIRECTORY), id="ORC Asset"
        ),
        pytest.param(
            DirectoryJSONAsset(name="test_asset", data_directory=DATA_DIRECTORY), id="JSON Asset"
        ),
        pytest.param(
            DirectoryTextAsset(name="test_asset", data_directory=DATA_DIRECTORY), id="Text Asset"
        ),
        pytest.param(
            DirectoryDeltaAsset(name="test_asset", data_directory=DATA_DIRECTORY), id="Delta Asset"
        ),
    ]


@pytest.mark.unit
@pytest.mark.parametrize("asset", _directory_asset_parameters(), indirect=["asset"])
def test_add_batch_definition_whole_directory_success(
    datasource,
    asset,
):
    # arrange
    name = "batch_def_name"
    expected_batch_definition = BatchDefinition(name=name, partitioner=None)
    datasource.add_batch_definition.return_value = expected_batch_definition

    # act
    batch_definition = asset.add_batch_definition_whole_directory(name=name)

    # assert
    assert batch_definition == expected_batch_definition
    datasource.add_batch_definition.assert_called_once_with(expected_batch_definition)


@pytest.mark.unit
@pytest.mark.parametrize("asset", _directory_asset_parameters(), indirect=["asset"])
def test_add_batch_definition_daily_success(datasource, asset):
    # arrange
    name = "batch_def_name"
    column = "foo"
    expected_batch_definition = BatchDefinition(
        name=name,
        partitioner=ColumnPartitionerDaily(column_name=column),
    )
    datasource.add_batch_definition.return_value = expected_batch_definition

    # act
    batch_definition = asset.add_batch_definition_daily(name=name, column=column)

    # assert
    assert batch_definition == expected_batch_definition
    datasource.add_batch_definition.assert_called_once_with(expected_batch_definition)


@pytest.mark.unit
@pytest.mark.parametrize("asset", _directory_asset_parameters(), indirect=["asset"])
def test_add_batch_definition_monthly_success(
    datasource,
    asset,
):
    # arrange
    name = "batch_def_name"
    column = "foo"
    expected_batch_definition = BatchDefinition(
        name=name, partitioner=ColumnPartitionerMonthly(column_name=column)
    )
    datasource.add_batch_definition.return_value = expected_batch_definition

    # act
    batch_definition = asset.add_batch_definition_monthly(name=name, column=column)

    # assert
    assert batch_definition == expected_batch_definition
    datasource.add_batch_definition.assert_called_once_with(expected_batch_definition)


@pytest.mark.unit
@pytest.mark.parametrize("asset", _directory_asset_parameters(), indirect=["asset"])
def test_add_batch_definition_yearly_success(
    datasource,
    asset,
):
    # arrange
    name = "batch_def_name"
    column = "foo"
    expected_batch_definition = BatchDefinition(
        name=name, partitioner=ColumnPartitionerYearly(column_name=column)
    )
    datasource.add_batch_definition.return_value = expected_batch_definition

    # act
    batch_definition = asset.add_batch_definition_yearly(name=name, column=column)

    # assert
    assert batch_definition == expected_batch_definition
    datasource.add_batch_definition.assert_called_once_with(expected_batch_definition)
