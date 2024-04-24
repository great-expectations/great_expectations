import pathlib
import re
from typing import List

import pytest

from great_expectations.alias_types import PathStr
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.partitioners import PartitionerYearAndMonth
from great_expectations.datasource.fluent.data_asset.data_connector import FilePathDataConnector
from great_expectations.datasource.fluent.file_path_data_asset import (
    AmbiguousPathError,
    PathNotFoundError,
    RegexMissingRequiredGroupsError,
    RegexUnknownGroupsError,
    _FilePathDataAsset,
)
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
def test_get_batch_list_from_batch_request__sort_ascending(
    validated_pandas_filesystem_datasource: PandasFilesystemDatasource,
):
    """Verify that get_batch_list_from_batch_request respects a partitioner's ascending sort order.

    NOTE: we just happen to be using pandas as the concrete class.
    """
    asset = validated_pandas_filesystem_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
    )
    batch_definition = asset.add_batch_definition(
        "foo",
        partitioner=PartitionerYearAndMonth(
            column_name="TODO: delete column from this partitioner", sort_ascending=True
        ),
    )
    batch_request = batch_definition.build_batch_request()

    batches = asset.get_batch_list_from_batch_request(batch_request)

    expected_years = ["2018"] * 12 + ["2019"] * 12 + ["2020"] * 12
    expected_months = [format(m, "02d") for m in range(1, 13)] * 3

    assert (len(batches)) == 36
    for i, batch in enumerate(batches):
        assert batch.metadata["year"] == str(expected_years[i])
        assert batch.metadata["month"] == str(expected_months[i])


@pytest.mark.xfail(
    strict=True,
    reason="This test will pass with a few adjustments when RegexPartitioners are implemented.",
)
@pytest.mark.filesystem
def test_get_batch_list_from_batch_request__sort_descending(
    validated_pandas_filesystem_datasource: PandasFilesystemDatasource,
):
    """Verify that get_batch_list_from_batch_request respects a partitioner's descending sort order.

    NOTE: we just happen to be using pandas as the concrete class.
    """
    asset = validated_pandas_filesystem_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
    )
    batch_definition = asset.add_batch_definition(
        "foo",
        partitioner=PartitionerYearAndMonth(
            column_name="TODO: delete column from this partitioner", sort_ascending=False
        ),
    )
    batch_request = batch_definition.build_batch_request()

    batches = asset.get_batch_list_from_batch_request(batch_request)

    expected_years = list(reversed(["2018"] * 12 + ["2019"] * 12 + ["2020"] * 12))
    expected_months = list(reversed([format(m, "02d") for m in range(1, 13)] * 3))

    assert (len(batches)) == 36
    for i, batch in enumerate(batches):
        assert batch.metadata["year"] == str(expected_years[i])
        assert batch.metadata["month"] == str(expected_months[i])


@pytest.fixture
def datasource(mocker):
    return mocker.Mock(spec=PandasFilesystemDatasource)


@pytest.fixture
def file_path_data_connector(mocker):
    return mocker.Mock(spec=FilePathDataConnector)


@pytest.fixture
def asset(datasource, file_path_data_connector) -> _FilePathDataAsset:
    asset = _FilePathDataAsset(name="test_asset", type="_sql_asset")
    asset._datasource = datasource  # same pattern Datasource uses to init Asset
    asset._data_connector = file_path_data_connector
    return asset


PATH_NAME = "data_2022-01.csv"


@pytest.mark.unit
@pytest.mark.parametrize(
    "path",
    [
        pytest.param(PATH_NAME, id="String Path"),
        pytest.param(pathlib.Path(PATH_NAME), id="Pathlib Path"),
    ],
)
def test_add_batch_definition_fluent_file_path__add_batch_definition_path_success(
    datasource, asset, path: PathStr, file_path_data_connector
):
    # arrange
    name = "batch_def_name"
    expected_regex = re.compile(str(path))
    expected_batch_definition = BatchDefinition(
        name=name, partitioner=None, batching_regex=expected_regex
    )
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
def test_add_batch_definition_fluent_file_path__add_batch_definition_path_fails_if_no_file_is_found(
    datasource, asset, path: PathStr, file_path_data_connector
):
    # arrange
    name = "batch_def_name"
    expected_regex = re.compile(str(path))
    expected_batch_definition = BatchDefinition(
        name=name, partitioner=None, batching_regex=expected_regex
    )
    datasource.add_batch_definition.return_value = expected_batch_definition
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
def test_add_batch_definition_fluent_file_path__add_batch_definition_path_fails_if_multiple_files_are_found(  # noqa: E501
    datasource, asset, path: PathStr, file_path_data_connector
):
    """This edge case occurs if a user doesn't actually provide a path, but
    instead a regex with multiple matches."""
    # arrange
    name = "batch_def_name"
    expected_regex = re.compile(str(path))
    expected_batch_definition = BatchDefinition(
        name=name, partitioner=None, batching_regex=expected_regex
    )
    datasource.add_batch_definition.return_value = expected_batch_definition
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
def test_add_batch_definition_fluent_file_path__add_batch_definition_yearly_success(
    datasource, asset
):
    # arrange
    name = "batch_def_name"
    batching_regex = re.compile(r"data_(?P<year>\d{4}).csv")
    expected_batch_definition = BatchDefinition(
        name=name,
        partitioner=None,
        batching_regex=batching_regex,
    )
    datasource.add_batch_definition.return_value = expected_batch_definition

    # act
    batch_definition = asset.add_batch_definition_yearly(name=name, regex=batching_regex)

    # assert
    assert batch_definition == expected_batch_definition
    datasource.add_batch_definition.assert_called_once_with(expected_batch_definition)


@pytest.mark.unit
def test_add_batch_definition_fluent_file_path__add_batch_definition_yearly_fails_if_required_group_is_missing(  # noqa: E501
    datasource, asset
):
    # arrange
    name = "batch_def_name"
    batching_regex = re.compile(r"data_2024.csv")
    expected_batch_definition = BatchDefinition(
        name=name,
        partitioner=None,
        batching_regex=batching_regex,
    )
    datasource.add_batch_definition.return_value = expected_batch_definition

    # act
    with pytest.raises(RegexMissingRequiredGroupsError) as error:
        asset.add_batch_definition_yearly(name=name, regex=batching_regex)

        # assert -- we need to still be inside context manager to access this instance attribute
        assert error.missing_groups == {"year"}

    datasource.add_batch_definition.assert_not_called()


@pytest.mark.unit
def test_add_batch_definition_fluent_file_path__add_batch_definition_yearly_fails_if_unknown_groups_are_found(  # noqa: E501
    datasource, asset
):
    # arrange
    name = "batch_def_name"
    batching_regex = re.compile(r"data_(?P<year>\d{4})-(?P<foo>\d{4}).csv")
    expected_batch_definition = BatchDefinition(
        name=name,
        partitioner=None,
        batching_regex=batching_regex,
    )
    datasource.add_batch_definition.return_value = expected_batch_definition

    # act
    with pytest.raises(RegexUnknownGroupsError) as error:
        asset.add_batch_definition_yearly(name=name, regex=batching_regex)

        # assert -- we need to still be inside context manager to access this instance attribute
        assert error.unknown_groups == {"foo"}

    datasource.add_batch_definition.assert_not_called()


@pytest.mark.unit
def test_add_batch_definition_fluent_file_path__add_batch_definition_monthly_success(
    datasource, asset
):
    # arrange
    name = "batch_def_name"
    batching_regex = re.compile(r"data_(?P<year>\d{4})-(?P<month>\d{2}).csv")
    expected_batch_definition = BatchDefinition(
        name=name,
        partitioner=None,
        batching_regex=batching_regex,
    )
    datasource.add_batch_definition.return_value = expected_batch_definition

    # act
    batch_definition = asset.add_batch_definition_monthly(name=name, regex=batching_regex)

    # assert
    assert batch_definition == expected_batch_definition
    datasource.add_batch_definition.assert_called_once_with(expected_batch_definition)


@pytest.mark.unit
def test_add_batch_definition_fluent_file_path__add_batch_definition_monthly_fails_if_required_group_is_missing(  # noqa: E501
    datasource, asset
):
    # arrange
    name = "batch_def_name"
    batching_regex = re.compile(r"data_2024-01.csv")
    expected_batch_definition = BatchDefinition(
        name=name,
        partitioner=None,
        batching_regex=batching_regex,
    )
    datasource.add_batch_definition.return_value = expected_batch_definition

    # act
    with pytest.raises(RegexMissingRequiredGroupsError) as error:
        asset.add_batch_definition_monthly(name=name, regex=batching_regex)

        # assert -- we need to still be inside context manager to access this instance attribute
        assert error.missing_groups == {"year", "month"}

    datasource.add_batch_definition.assert_not_called()


@pytest.mark.unit
def test_add_batch_definition_fluent_file_path__add_batch_definition_monthly_fails_if_unknown_groups_are_found(  # noqa: E501
    datasource, asset
):
    # arrange
    name = "batch_def_name"
    batching_regex = re.compile(r"data_(?P<year>\d{4})-(?P<month>\d{2})-(?P<foo>\d{4}).csv")
    expected_batch_definition = BatchDefinition(
        name=name,
        partitioner=None,
        batching_regex=batching_regex,
    )
    datasource.add_batch_definition.return_value = expected_batch_definition

    # act
    with pytest.raises(RegexUnknownGroupsError) as error:
        asset.add_batch_definition_monthly(name=name, regex=batching_regex)

        # assert -- we need to still be inside context manager to access this instance attribute
        assert error.unknown_groups == {"foo"}

    datasource.add_batch_definition.assert_not_called()


@pytest.mark.unit
def test_add_batch_definition_fluent_file_path__add_batch_definition_daily_success(
    datasource, asset
):
    # arrange
    name = "batch_def_name"
    batching_regex = re.compile(r"data_(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2}).csv")
    expected_batch_definition = BatchDefinition(
        name=name,
        partitioner=None,
        batching_regex=batching_regex,
    )
    datasource.add_batch_definition.return_value = expected_batch_definition

    # act
    batch_definition = asset.add_batch_definition_daily(name=name, regex=batching_regex)

    # assert
    assert batch_definition == expected_batch_definition
    datasource.add_batch_definition.assert_called_once_with(expected_batch_definition)


@pytest.mark.unit
def test_add_batch_definition_fluent_file_path__add_batch_definition_daily_fails_if_required_group_is_missing(  # noqa: E501
    datasource, asset
):
    # arrange
    name = "batch_def_name"
    batching_regex = re.compile(r"data_2024-01-01.csv")
    expected_batch_definition = BatchDefinition(
        name=name,
        partitioner=None,
        batching_regex=batching_regex,
    )
    datasource.add_batch_definition.return_value = expected_batch_definition

    # act
    with pytest.raises(RegexMissingRequiredGroupsError) as error:
        asset.add_batch_definition_daily(name=name, regex=batching_regex)

        # assert -- we need to still be inside context manager to access this instance attribute
        assert error.missing_groups == {"year", "month", "day"}

    datasource.add_batch_definition.assert_not_called()


@pytest.mark.unit
def test_add_batch_definition_fluent_file_path__add_batch_definition_daily_fails_if_unknown_groups_are_found(  # noqa: E501
    datasource, asset
):
    # arrange
    name = "batch_def_name"
    batching_regex = re.compile(
        r"data_(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})-(?P<foo>\d{4}).csv"
    )
    expected_batch_definition = BatchDefinition(
        name=name,
        partitioner=None,
        batching_regex=batching_regex,
    )
    datasource.add_batch_definition.return_value = expected_batch_definition

    # act
    with pytest.raises(RegexUnknownGroupsError) as error:
        asset.add_batch_definition_daily(name=name, regex=batching_regex)

        # assert -- we need to still be inside context manager to access this instance attribute
        assert error.unknown_groups == {"foo"}

    datasource.add_batch_definition.assert_not_called()
