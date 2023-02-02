from __future__ import annotations

import logging
import pathlib
import re
from dataclasses import dataclass
from pprint import pformat as pf
from typing import TYPE_CHECKING, Any, Type

import pydantic
import pytest
from pytest import MonkeyPatch, param

import great_expectations.exceptions as ge_exceptions
import great_expectations.execution_engine.pandas_execution_engine
from great_expectations.experimental.datasources.pandas_datasource import (
    CSVAsset,
    JSONAsset,
    PandasDatasource,
    _DataFrameAsset,
)

if TYPE_CHECKING:
    from great_expectations.alias_types import PathStr
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.experimental.datasources.interfaces import (
        BatchSortersDefinition,
    )

logger = logging.getLogger(__file__)


@pytest.fixture
def pandas_datasource() -> PandasDatasource:
    return PandasDatasource(name="pandas_datasource")


@pytest.fixture
def csv_path() -> pathlib.Path:
    relative_path = pathlib.Path(
        "..", "..", "test_sets", "taxi_yellow_tripdata_samples"
    )
    abs_csv_path = (
        pathlib.Path(__file__).parent.joinpath(relative_path).resolve(strict=True)
    )
    return abs_csv_path


class SpyInterrupt(RuntimeError):
    """
    Exception that may be raised to interrupt the control flow of the program
    when a spy has already captured everything needed.
    """


@pytest.fixture
def capture_reader_fn_params(monkeypatch: MonkeyPatch):
    """
    Capture the `reader_options` arguments being passed to the `PandasExecutionEngine`.

    Note this fixture is heavily reliant on the implementation details of `PandasExecutionEngine`,
    should this change this fixture will need to change.
    """
    captured_args: list[list] = []
    captured_kwargs: list[dict[str, Any]] = []

    def reader_fn_spy(*args, **kwargs):
        logging.info(f"reader_fn_spy() called with...\n{args}\n{kwargs}")
        captured_args.append(args)
        captured_kwargs.append(kwargs)
        raise SpyInterrupt("Reader options have been captured")

    monkeypatch.setattr(
        great_expectations.execution_engine.pandas_execution_engine.PandasExecutionEngine,
        "_get_reader_fn",
        lambda *_: reader_fn_spy,
        raising=True,
    )

    yield captured_args, captured_kwargs


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

        assert asset_class_names.issubset(asset_field_union_members)

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

    @pytest.mark.parametrize(
        ["asset_model", "extra_kwargs"],
        [
            (CSVAsset, {"sep": "|", "names": ["col1", "col2", "col3"]}),
            (JSONAsset, {"orient": "records", "convert_dates": True}),
        ],
    )
    def test_data_asset_defaults(
        self,
        asset_model: Type[_DataFrameAsset],
        extra_kwargs: dict,
    ):
        """
        Test that an asset dictionary can be dumped with only the original passed keys
        present.
        """
        kwargs: dict[str, Any] = {
            "name": "test",
            "base_directory": pathlib.Path(__file__),
            "regex": re.compile(r"yellow_tripdata_sample_(\d{4})-(\d{2})"),
        }
        kwargs.update(extra_kwargs)
        print(f"extra_kwargs\n{pf(extra_kwargs)}")
        asset_instance = asset_model(**kwargs)
        assert asset_instance.dict() == kwargs

    @pytest.mark.parametrize(
        "extra_kwargs",
        [
            {"sep": "|", "decimal": ","},
            {"usecols": [0, 1, 2], "names": ["foo", "bar"]},
            {"dtype": {"col_1": "Int64"}},
        ],
    )
    def test_data_asset_reader_options_passthrough(
        self,
        empty_data_context: AbstractDataContext,
        csv_path: pathlib.Path,
        capture_reader_fn_params: tuple[list[list], list[dict]],
        extra_kwargs: dict,
    ):
        batch_request = (
            empty_data_context.sources.add_pandas("my_pandas")
            .add_csv_asset(
                "my_csv",
                base_directory=csv_path,
                regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).csv",
                **extra_kwargs,
            )
            .get_batch_request({"year": "2018"})
        )
        with pytest.raises(SpyInterrupt):
            empty_data_context.get_validator(batch_request=batch_request)

        captured_args, captured_kwargs = capture_reader_fn_params
        print(f"positional args:\n{pf(captured_args[-1])}\n")
        print(f"keyword args:\n{pf(captured_kwargs[-1])}")

        assert captured_kwargs[-1] == extra_kwargs


@pytest.mark.unit
def test_construct_pandas_datasource(pandas_datasource: PandasDatasource):
    assert pandas_datasource.name == "pandas_datasource"


@pytest.mark.unit
def test_add_csv_asset_to_datasource(
    pandas_datasource: PandasDatasource, csv_path: pathlib.Path
):
    asset = pandas_datasource.add_csv_asset(
        name="csv_asset",
        base_directory=csv_path,
        regex=r"yellow_tripdata_sample_(\d{4})-(\d{2}).csv",
    )
    assert asset.name == "csv_asset"
    assert asset.base_directory == csv_path
    assert asset.regex.match("random string") is None
    assert asset.regex.match("yellow_tripdata_sample_11D1-22.csv") is None
    m1 = asset.regex.match("yellow_tripdata_sample_1111-22.csv")
    assert m1 is not None


@pytest.mark.unit
def test_construct_csv_asset_directly(csv_path: pathlib.Path):
    # noinspection PyTypeChecker
    asset = CSVAsset(
        name="csv_asset",
        base_directory=csv_path,
        regex=r"yellow_tripdata_sample_(\d{4})-(\d{2}).csv",  # Ignoring IDE warning (type declarations are consistent).
    )
    assert asset.name == "csv_asset"
    assert asset.base_directory == csv_path
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
        base_directory=csv_path,
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
        base_directory=csv_path,
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
        base_directory=csv_path,
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
        base_directory=csv_path,
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
        base_directory=csv_path,
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
        "base_directory": asset.base_directory / "yellow_tripdata_sample_2018-04.csv",
    }
    assert batch.id == "pandas_datasource-csv_asset-year_2018-month_04"


@pytest.mark.unit
def test_get_batch_list_from_partially_specified_batch_request(
    pandas_datasource: PandasDatasource, csv_path: pathlib.Path
):
    # Verify test directory has files that don't match what we will query for
    file_name: PathStr
    all_files: list[str] = [
        file_name.stem for file_name in list(pathlib.Path(csv_path).iterdir())
    ]
    # assert there are files that are not csv files
    assert any([not file_name.endswith("csv") for file_name in all_files])
    # assert there are 12 files from 2018
    files_for_2018 = [
        file_name for file_name in all_files if file_name.find("2018") >= 0
    ]
    assert len(files_for_2018) == 12

    asset = pandas_datasource.add_csv_asset(
        name="csv_asset",
        base_directory=csv_path,
        regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).csv",
    )
    request = asset.get_batch_request({"year": "2018"})
    batches = asset.get_batch_list_from_batch_request(request)
    assert (len(batches)) == 12
    batch_filenames = [
        pathlib.Path(batch.metadata["base_directory"]).stem for batch in batches
    ]
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
def test_pandas_sorter(
    pandas_datasource: PandasDatasource,
    csv_path: pathlib.Path,
    order_by: BatchSortersDefinition,
):
    # Verify test directory has files we expect
    years = ["2018", "2019", "2020"]
    months = [format(m, "02d") for m in range(1, 13)]
    file_name: PathStr
    all_files: list[str] = [
        file_name.stem for file_name in list(pathlib.Path(csv_path).iterdir())
    ]
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
        base_directory=csv_path,
        regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).csv",
        order_by=order_by,
    )
    batches = asset.get_batch_list_from_batch_request(asset.get_batch_request())
    assert (len(batches)) == 36

    @dataclass(frozen=True)
    class TimeRange:
        key: str
        range: list[str]

    ordered_years = reversed(years) if "-year" in order_by else years
    ordered_months = reversed(months) if "-month" in order_by else months
    if "year" in order_by[0]:  # type: ignore[operator]
        ordered = [
            TimeRange(key="year", range=ordered_years),  # type: ignore[arg-type]
            TimeRange(key="month", range=ordered_months),  # type: ignore[arg-type]
        ]
    else:
        ordered = [
            TimeRange(key="month", range=ordered_months),  # type: ignore[arg-type]
            TimeRange(key="year", range=ordered_years),  # type: ignore[arg-type]
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
