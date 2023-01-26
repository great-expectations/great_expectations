from __future__ import annotations

import inspect
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pydantic
import pytest
from pytest import param

import great_expectations.exceptions as ge_exceptions
from great_expectations.alias_types import PathStr
from great_expectations.data_context.util import file_relative_path
from great_expectations.experimental.datasources.interfaces import (
    BatchSortersDefinition,
)
from great_expectations.experimental.datasources.pandas_datasource import (
    CSVAsset,
    PandasDatasource,
)

# TODO: use this to parametrize `test_data_asset_defined_for_io_read_method`
PANDAS_IO_METHODS: list[str] = [
    member_tuple[0]
    for member_tuple in inspect.getmembers(pd, predicate=inspect.isfunction)
    if member_tuple[0].startswith("read_")
]


@pytest.mark.unit
class TestDynamicPandasAssets:
    def test_asset_types_and_asset_annotations_match(self):
        asset_class_names: set[str] = {t.__name__ for t in PandasDatasource.asset_types}
        assert asset_class_names

        assets_field: pydantic.fields.ModelField = PandasDatasource.__dict__[
            "__fields__"
        ]["assets"]
        asset_field_union_members: set[str] = {
            t.__name__
            for t in assets_field.type_.__args__  # accessing the `Union` members with `__args__`
        }

        assert asset_class_names == asset_field_union_members

    @pytest.mark.parametrize(
        "method_name",
        [
            param("read_clipboard", marks=pytest.mark.xfail),
            "read_csv",
            "read_excel",
            param("read_feather", marks=pytest.mark.xfail),
            param("read_fwf", marks=pytest.mark.xfail),
            param("read_gbq", marks=pytest.mark.xfail),
            param("read_hdf", marks=pytest.mark.xfail),
            param("read_html", marks=pytest.mark.xfail),
            "read_json",
            param("read_orc", marks=pytest.mark.xfail),
            "read_parquet",
            param("read_pickle", marks=pytest.mark.xfail),
            param("read_sas", marks=pytest.mark.xfail),
            param("read_spss", marks=pytest.mark.xfail),
            param("read_sql", marks=pytest.mark.xfail),
            param("read_sql_query", marks=pytest.mark.xfail),
            param("read_sql_table", marks=pytest.mark.xfail),
            param("read_stata", marks=pytest.mark.xfail),
            param("read_table", marks=pytest.mark.xfail),
            param("read_xml", marks=pytest.mark.xfail),
        ],
    )
    def test_data_asset_defined_for_io_read_method(self, method_name: str):
        _, type_name = method_name.split("read_")
        assert type_name

        asset_class_names: set[str] = {
            t.__name__.lower().split("asset")[0] for t in PandasDatasource.asset_types
        }
        print(asset_class_names)

        assert type_name in asset_class_names

    def test_data_asset_reader_options_passthrough(self):
        raise NotImplementedError("create this test")


@pytest.fixture
def pandas_datasource() -> PandasDatasource:
    return PandasDatasource(name="pandas_datasource")


@pytest.fixture
def csv_path() -> pathlib.Path:
    return pathlib.Path(
        file_relative_path(
            __file__,
            pathlib.Path("..", "..", "test_sets", "taxi_yellow_tripdata_samples"),
        )
    )


@pytest.mark.unit
def test_construct_pandas_datasource(pandas_datasource: PandasDatasource):
    assert pandas_datasource.name == "pandas_datasource"


@pytest.mark.unit
def test_add_csv_asset_to_datasource(
    pandas_datasource: PandasDatasource, csv_path: pathlib.Path
):
    asset = pandas_datasource.add_csv_asset(
        name="csv_asset",
        data_path=csv_path,
        regex=r"yellow_tripdata_sample_(\d{4})-(\d{2}).csv",
    )
    assert asset.name == "csv_asset"
    assert asset.path == csv_path
    assert asset.regex.match("random string") is None
    assert asset.regex.match("yellow_tripdata_sample_11D1-22.csv") is None
    m1 = asset.regex.match("yellow_tripdata_sample_1111-22.csv")
    assert m1 is not None


@pytest.mark.unit
def test_construct_csv_asset_directly(csv_path: pathlib.Path):
    # noinspection PyTypeChecker
    asset = CSVAsset(
        name="csv_asset",
        path=csv_path,
        regex=r"yellow_tripdata_sample_(\d{4})-(\d{2}).csv",  # Ignoring IDE warning (type declarations are consistent).
    )
    assert asset.name == "csv_asset"
    assert asset.path == csv_path
    assert asset.regex.match("random string") is None
    assert asset.regex.match("yellow_tripdata_sample_11D1-22.csv") is None
    m1 = asset.regex.match("yellow_tripdata_sample_1111-22.csv")
    assert m1 is not None


@pytest.mark.unit
def test_csv_asset_with_regex_unnamed_parameters(
    pandas_datasource: PandasDatasource, csv_path: pathlib.Path
):
    asset = pandas_datasource.add_csv_asset(
        name="csv_asset",
        data_path=csv_path,
        regex=r"yellow_tripdata_sample_(\d{4})-(\d{2}).csv",
    )
    options = asset.batch_request_options_template()
    assert options == {"batch_request_param_1": None, "batch_request_param_2": None}


@pytest.mark.unit
def test_csv_asset_with_regex_named_parameters(
    pandas_datasource: PandasDatasource, csv_path: pathlib.Path
):
    asset = pandas_datasource.add_csv_asset(
        name="csv_asset",
        data_path=csv_path,
        regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).csv",
    )
    options = asset.batch_request_options_template()
    assert options == {"year": None, "month": None}


@pytest.mark.unit
def test_csv_asset_with_some_regex_named_parameters(
    pandas_datasource: PandasDatasource, csv_path: pathlib.Path
):
    asset = pandas_datasource.add_csv_asset(
        name="csv_asset",
        data_path=csv_path,
        regex=r"yellow_tripdata_sample_(\d{4})-(?P<month>\d{2}).csv",
    )
    options = asset.batch_request_options_template()
    assert options == {"batch_request_param_1": None, "month": None}


@pytest.mark.unit
def test_csv_asset_with_non_string_regex_named_parameters(
    pandas_datasource: PandasDatasource, csv_path: pathlib.Path
):
    asset = pandas_datasource.add_csv_asset(
        name="csv_asset",
        data_path=csv_path,
        regex=r"yellow_tripdata_sample_(\d{4})-(?P<month>\d{2}).csv",
    )
    with pytest.raises(ge_exceptions.InvalidBatchRequestError):
        # year is an int which will raise an error
        asset.get_batch_request({"year": 2018, "month": "04"})


@pytest.mark.unit
def test_get_batch_list_from_fully_specified_batch_request(
    pandas_datasource: PandasDatasource, csv_path: pathlib.Path
):
    asset = pandas_datasource.add_csv_asset(
        name="csv_asset",
        data_path=csv_path,
        regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).csv",
    )
    request = asset.get_batch_request({"year": "2018", "month": "04"})
    batches = asset.get_batch_list_from_batch_request(request)
    assert len(batches) == 1
    batch = batches[0]
    assert batch.batch_request.datasource_name == pandas_datasource.name
    assert batch.batch_request.data_asset_name == asset.name
    assert batch.batch_request.options == {"year": "2018", "month": "04"}
    assert batch.metadata == {
        "year": "2018",
        "month": "04",
        "path": asset.path / "yellow_tripdata_sample_2018-04.csv",
    }
    assert batch.id == "pandas_datasource-csv_asset-year_2018-month_04"


@pytest.mark.unit
def test_get_batch_list_from_partially_specified_batch_request(
    pandas_datasource, csv_path: Path
):
    # Verify test directory has files that don't match what we will query for
    all_files = list(csv_path.iterdir())
    # assert there are files that are not csv files
    assert any([file.suffix != "csv" for file in all_files])
    # assert there are 12 files from 2018
    files_for_2018 = [file for file in all_files if "2018" in file.name]
    assert len(files_for_2018) == 12

    asset = pandas_datasource.add_csv_asset(
        name="csv_asset",
        data_path=csv_path,
        regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).csv",
    )
    request = asset.get_batch_request({"year": "2018"})
    batches = asset.get_batch_list_from_batch_request(request)
    assert (len(batches)) == 12
    batch_filenames = [Path(batch.metadata["path"]) for batch in batches]
    assert set(files_for_2018) == set(batch_filenames)

    @dataclass(frozen=True)
    class YearMonth:
        year: str
        month: str

    expected_year_month = {
        YearMonth(year="2018", month=format(m, "02d")) for m in range(1, 13)
    }
    batch_year_month = {
        YearMonth(year=batch.metadata["year"], month=batch.metadata["month"])
        for batch in batches
    }
    assert expected_year_month == batch_year_month


@pytest.mark.unit
@pytest.mark.parametrize(
    "order_by",
    [
        ["+year", "month"],
        ["+year", "+month"],
        ["+year", "-month"],
        ["year", "month"],
        ["year", "+month"],
        ["year", "-month"],
        ["-year", "month"],
        ["-year", "+month"],
        ["-year", "-month"],
        ["month", "+year"],
        ["+month", "+year"],
        ["-month", "+year"],
        ["month", "year"],
        ["+month", "year"],
        ["-month", "year"],
        ["month", "-year"],
        ["+month", "-year"],
        ["-month", "-year"],
    ],
)
def test_pandas_sorter(pandas_datasource, csv_path: Path, order_by):
    # Verify test directory has files we expect
    years = ["2018", "2019", "2020"]
    months = [format(m, "02d") for m in range(1, 13)]
    all_files = [p.name for p in csv_path.iterdir()]
    # assert there are 12 files for each year
    for year in years:
        files_for_year = [
            file_name
            for file_name in all_files
            if file_name.find(f"yellow_tripdata_sample_{year}") == 0
        ]
        assert len(files_for_year) == 12

    asset = pandas_datasource.add_csv_asset(
        name="csv_asset",
        data_path=csv_path,
        regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).csv",
        order_by=order_by,
    )
    batches = asset.get_batch_list_from_batch_request(asset.get_batch_request())
    assert (len(batches)) == 36

    @dataclass(frozen=True)
    class TimeRange:
        key: str
        range: list[int]

    ordered_years = reversed(years) if "-year" in order_by else years
    ordered_months = reversed(months) if "-month" in order_by else months
    if "year" in order_by[0]:
        ordered = [
            TimeRange(key="year", range=ordered_years),
            TimeRange(key="month", range=ordered_months),
        ]
    else:
        ordered = [
            TimeRange(key="month", range=ordered_months),
            TimeRange(key="year", range=ordered_years),
        ]

    batch_index = -1
    for range1 in ordered[0].range:
        key1 = ordered[0].key
        for range2 in ordered[1].range:
            key2 = ordered[1].key
            batch_index += 1
            metadata = batches[batch_index].metadata
            assert metadata[key1] == range1
            assert metadata[key2] == range2
