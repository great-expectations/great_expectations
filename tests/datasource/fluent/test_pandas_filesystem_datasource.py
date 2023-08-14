from __future__ import annotations

import copy
import inspect
import logging
import pathlib
import re
from dataclasses import dataclass
from pprint import pformat as pf
from typing import TYPE_CHECKING, Any, Optional, Type

import pydantic
import pytest
from pytest import MonkeyPatch, param

import great_expectations.exceptions as ge_exceptions
import great_expectations.execution_engine.pandas_execution_engine
from great_expectations.datasource.fluent import PandasFilesystemDatasource
from great_expectations.datasource.fluent.data_asset.data_connector import (
    FilesystemDataConnector,
)
from great_expectations.datasource.fluent.dynamic_pandas import PANDAS_VERSION
from great_expectations.datasource.fluent.interfaces import TestConnectionError
from great_expectations.datasource.fluent.pandas_file_path_datasource import (  # type: ignore[attr-defined] # is defined but private
    CSVAsset,
    JSONAsset,
    _FilePathDataAsset,
)
from great_expectations.datasource.fluent.sources import _get_field_details

if TYPE_CHECKING:
    from great_expectations.alias_types import PathStr
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.datasource.fluent.interfaces import (
        BatchMetadata,
        BatchSlice,
        SortersDefinition,
    )

logger = logging.getLogger(__file__)

# apply markers to entire test module
pytestmark = [
    pytest.mark.skipif(
        PANDAS_VERSION < 1.2, reason=f"Fluent pandas not supported on {PANDAS_VERSION}"
    )
]


@pytest.fixture
def pandas_filesystem_datasource(empty_data_context) -> PandasFilesystemDatasource:
    base_directory_rel_path = pathlib.Path(
        "..", "..", "test_sets", "taxi_yellow_tripdata_samples"
    )
    base_directory_abs_path = (
        pathlib.Path(__file__)
        .parent.joinpath(base_directory_rel_path)
        .resolve(strict=True)
    )
    pandas_filesystem_datasource = PandasFilesystemDatasource(  # type: ignore[call-arg]
        name="pandas_filesystem_datasource",
        base_directory=base_directory_abs_path,
    )
    pandas_filesystem_datasource._data_context = empty_data_context
    return pandas_filesystem_datasource


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
    @pytest.mark.parametrize(
        "method_name",
        [
            param("read_clipboard", marks=pytest.mark.xfail(reason="not path based")),
            param("read_csv"),
            param("read_excel"),
            param("read_feather"),
            param("read_fwf"),
            param("read_gbq", marks=pytest.mark.xfail(reason="not path based")),
            param("read_hdf"),
            param("read_html"),
            param("read_json"),
            param("read_orc"),
            param("read_parquet"),
            param("read_pickle"),
            param("read_sas"),
            param("read_spss"),
            param(
                "read_sql",
                marks=pytest.mark.xfail(reason="name conflict & not path based"),
            ),
            param(
                "read_sql_query",
                marks=pytest.mark.xfail(
                    reason="type name logic expects 'sqltable' & not path based"
                ),
            ),
            param(
                "read_sql_table",
                marks=pytest.mark.xfail(
                    reason="type name logic expects 'sqltable' & not path based"
                ),
            ),
            param("read_stata"),
            param(
                "read_table",
                marks=pytest.mark.xfail(reason="name conflict & not path based"),
            ),
            param(
                "read_xml",
                marks=pytest.mark.skipif(
                    PANDAS_VERSION < 1.3,
                    reason=f"read_xml does not exist on {PANDAS_VERSION} ",
                ),
            ),
        ],
    )
    def test_data_asset_defined_for_io_read_method(self, method_name: str):
        _, type_name = method_name.split("read_")
        assert type_name

        asset_class_names: set[str] = {
            t.__name__.lower().split("asset")[0]
            for t in PandasFilesystemDatasource.asset_types
        }
        print(asset_class_names)

        assert type_name in asset_class_names

    @pytest.mark.parametrize("asset_class", PandasFilesystemDatasource.asset_types)
    def test_add_asset_method_exists_and_is_functional(
        self, asset_class: Type[_FilePathDataAsset]
    ):
        type_name: str = _get_field_details(asset_class, "type").default_value
        method_name: str = f"add_{type_name}_asset"

        print(f"{method_name}() -> {asset_class.__name__}")

        assert method_name in PandasFilesystemDatasource.__dict__

        ds = PandasFilesystemDatasource(  # type: ignore[call-arg]
            name="ds_for_testing_add_asset_methods",
            base_directory=pathlib.Path.cwd(),
        )
        method = getattr(ds, method_name)

        with pytest.raises(pydantic.ValidationError) as exc_info:
            method(
                f"{asset_class.__name__}_add_asset_test",
                batching_regex="great_expectations",
                _invalid_key="foobar",
            )
        # importantly check that the method creates (or attempts to create) the intended asset
        assert exc_info.value.model == asset_class

    @pytest.mark.parametrize("asset_class", PandasFilesystemDatasource.asset_types)
    def test_add_asset_method_signature(self, asset_class: Type[_FilePathDataAsset]):
        type_name: str = _get_field_details(asset_class, "type").default_value
        method_name: str = f"add_{type_name}_asset"

        ds = PandasFilesystemDatasource(  # type: ignore[call-arg]
            name="ds_for_testing_add_asset_methods",
            base_directory=pathlib.Path.cwd(),
        )
        method = getattr(ds, method_name)

        add_asset_method_sig: inspect.Signature = inspect.signature(method)
        print(f"\t{method_name}()\n{add_asset_method_sig}\n")

        asset_class_init_sig: inspect.Signature = inspect.signature(asset_class)
        print(f"\t{asset_class.__name__}\n{asset_class_init_sig}\n")

        for i, param_name in enumerate(asset_class_init_sig.parameters):
            print(f"{i} {param_name} ", end="")

            if param_name == "type":
                assert (
                    param_name not in add_asset_method_sig.parameters
                ), "type should not be part of the `add_<TYPE>_asset` method"
                print("⏩")
                continue

            assert param_name in add_asset_method_sig.parameters
            print("✅")

    @pytest.mark.parametrize("asset_class", PandasFilesystemDatasource.asset_types)
    def test_minimal_validation(self, asset_class: Type[_FilePathDataAsset]):
        """
        These parametrized tests ensures that every `PandasFilesystemDatasource` asset model does some minimal
        validation, and doesn't accept arbitrary keyword arguments.
        This is also a proxy for testing that the dynamic pydantic model creation was successful.
        """
        with pytest.raises(pydantic.ValidationError) as exc_info:
            asset_class(
                name="test",
                base_directory=pathlib.Path(__file__),
                batching_regex=re.compile(r"yellow_tripdata_sample_(\d{4})-(\d{2})"),
                invalid_keyword_arg="bad",
            )

        errors_dict = exc_info.value.errors()
        assert {
            "loc": ("invalid_keyword_arg",),
            "msg": "extra fields not permitted",
            "type": "value_error.extra",
        } == errors_dict[  # the extra keyword error will always be the last error
            -1  # we don't care about any other errors for this test
        ]

    @pytest.mark.parametrize(
        ["asset_model", "extra_kwargs"],
        [
            (CSVAsset, {"sep": "|", "names": ["col1", "col2", "col3"]}),
            (JSONAsset, {"orient": "records", "convert_dates": True}),
        ],
    )
    def test_data_asset_defaults(
        self,
        asset_model: Type[_FilePathDataAsset],
        extra_kwargs: dict,
    ):
        """
        Test that an asset dictionary can be dumped with only the original passed keys
        present.
        """
        kwargs: dict[str, Any] = {
            "name": "test",
            "batching_regex": re.compile(r"yellow_tripdata_sample_(\d{4})-(\d{2})"),
        }
        kwargs.update(extra_kwargs)
        print(f"extra_kwargs\n{pf(extra_kwargs)}")
        asset_instance = asset_model(**kwargs)
        assert asset_instance.dict(exclude={"type"}) == kwargs

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
            empty_data_context.sources.add_pandas_filesystem(  # .build_batch_request
                "my_pandas",
                base_directory=csv_path,
            )
            .add_csv_asset(
                "my_csv",
                batching_regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
                **extra_kwargs,
            )
            .build_batch_request({"year": "2018"})
        )
        with pytest.raises(SpyInterrupt):
            empty_data_context.get_validator(batch_request=batch_request)

        captured_args, captured_kwargs = capture_reader_fn_params
        print(f"positional args:\n{pf(captured_args[-1])}\n")
        print(f"keyword args:\n{pf(captured_kwargs[-1])}")

        assert captured_kwargs[-1] == extra_kwargs


@pytest.mark.unit
def test_construct_pandas_filesystem_datasource(
    pandas_filesystem_datasource: PandasFilesystemDatasource,
):
    assert pandas_filesystem_datasource.name == "pandas_filesystem_datasource"


@pytest.mark.unit
def test_add_csv_asset_to_datasource(
    pandas_filesystem_datasource: PandasFilesystemDatasource,
):
    asset = pandas_filesystem_datasource.add_csv_asset(
        name="csv_asset",
    )
    assert asset.name == "csv_asset"
    m1 = asset.batching_regex.match("this_can_be_named_anything.csv")
    assert m1 is not None


@pytest.mark.unit
def test_add_csv_asset_with_batching_regex_to_datasource(
    pandas_filesystem_datasource: PandasFilesystemDatasource,
):
    asset = pandas_filesystem_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"yellow_tripdata_sample_(\d{4})-(\d{2})\.csv",
    )
    assert asset.name == "csv_asset"
    assert asset.batching_regex.match("random string") is None
    assert asset.batching_regex.match("yellow_tripdata_sample_11D1-22.csv") is None
    m1 = asset.batching_regex.match("yellow_tripdata_sample_1111-22.csv")
    assert m1 is not None


@pytest.mark.unit
def test_construct_csv_asset_directly():
    # noinspection PyTypeChecker
    asset = CSVAsset(
        name="csv_asset",
        batching_regex=r"yellow_tripdata_sample_(\d{4})-(\d{2})\.csv",
    )
    assert asset.name == "csv_asset"
    assert asset.batching_regex.match("random string") is None
    assert asset.batching_regex.match("yellow_tripdata_sample_11D1-22.csv") is None
    m1 = asset.batching_regex.match("yellow_tripdata_sample_1111-22.csv")
    assert m1 is not None


@pytest.mark.unit
def test_invalid_connect_options(
    pandas_filesystem_datasource: PandasFilesystemDatasource,
):
    with pytest.raises(pydantic.ValidationError) as exc_info:
        pandas_filesystem_datasource.add_csv_asset(  # type: ignore[call-arg]
            name="csv_asset",
            batching_regex=r"yellow_tripdata_sample_(\d{4})-(\d{2})\.csv",
            glob_foobar="invalid",
        )

    error_dicts = exc_info.value.errors()
    print(pf(error_dicts))
    assert [
        {
            "loc": ("glob_foobar",),
            "msg": "extra fields not permitted",
            "type": "value_error.extra",
        }
    ] == error_dicts


@pytest.mark.unit
@pytest.mark.parametrize(
    ["glob_directive", "expected_error"],
    [
        ({"invalid", "type"}, pydantic.ValidationError),
        ("not_a_dir/*.csv", TestConnectionError),
    ],
)
def test_invalid_connect_options_value(
    pandas_filesystem_datasource: PandasFilesystemDatasource,
    glob_directive,
    expected_error: Type[Exception],
):
    with pytest.raises(expected_error) as exc_info:
        pandas_filesystem_datasource.add_csv_asset(
            name="csv_asset",
            batching_regex=r"yellow_tripdata_sample_(\d{4})-(\d{2})\.csv",
            glob_directive=glob_directive,
        )

    print(f"Exception raised:\n\t{repr(exc_info.value)}")

    if isinstance(exc_info.value, pydantic.ValidationError):
        error_dicts = exc_info.value.errors()
        print(pf(error_dicts))
        assert [
            {
                "loc": ("glob_directive",),
                "msg": "str type expected",
                "type": "type_error.str",
            }
        ] == error_dicts


@pytest.mark.unit
@pytest.mark.parametrize(
    "connect_options",
    [
        param({"glob_directive": "**/*"}, id="glob **/*"),
        param({"glob_directive": "**/*.csv"}, id="glob **/*.csv"),
        param({}, id="default connect options"),
    ],
)
def test_asset_connect_options_in_repr(
    pandas_filesystem_datasource: PandasFilesystemDatasource, connect_options: dict
):
    asset = pandas_filesystem_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"yellow_tripdata_sample_(\d{4})-(\d{2})\.csv",
        **connect_options,
    )
    asset_repr = repr(asset)
    print(asset_repr)

    if connect_options:
        assert "glob_directive" in asset_repr
        assert connect_options["glob_directive"] in asset_repr
    else:
        # if no connect options are provided the defaults should be used and should not
        # be part of any serialization. repr == asset.yaml()
        assert "glob_directive" not in asset_repr


@pytest.mark.unit
def test_csv_asset_with_batching_regex_unnamed_parameters(
    pandas_filesystem_datasource: PandasFilesystemDatasource,
):
    asset = pandas_filesystem_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"yellow_tripdata_sample_(\d{4})-(\d{2})\.csv",
    )
    options = asset.batch_request_options
    assert options == (
        "batch_request_param_1",
        "batch_request_param_2",
        "path",
    )


@pytest.mark.unit
def test_csv_asset_with_batching_regex_named_parameters(
    pandas_filesystem_datasource: PandasFilesystemDatasource,
):
    asset = pandas_filesystem_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
    )
    options = asset.batch_request_options
    assert options == ("year", "month", "path")


@pytest.mark.unit
def test_csv_asset_with_some_batching_regex_named_parameters(
    pandas_filesystem_datasource: PandasFilesystemDatasource,
):
    asset = pandas_filesystem_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"yellow_tripdata_sample_(\d{4})-(?P<month>\d{2})\.csv",
    )
    options = asset.batch_request_options
    assert options == ("batch_request_param_1", "month", "path")


@pytest.mark.unit
def test_csv_asset_with_non_string_batching_regex_named_parameters(
    pandas_filesystem_datasource: PandasFilesystemDatasource,
):
    asset = pandas_filesystem_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"yellow_tripdata_sample_(\d{4})-(?P<month>\d{2})\.csv",
    )
    with pytest.raises(ge_exceptions.InvalidBatchRequestError):
        # year is an int which will raise an error
        asset.build_batch_request({"year": 2018, "month": "04"})


@pytest.mark.unit
def test_get_batch_list_from_fully_specified_batch_request(
    pandas_filesystem_datasource: PandasFilesystemDatasource,
):
    asset = pandas_filesystem_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
    )
    request = asset.build_batch_request({"year": "2018", "month": "04"})
    batches = asset.get_batch_list_from_batch_request(request)
    assert len(batches) == 1
    batch = batches[0]
    assert batch.batch_request.datasource_name == pandas_filesystem_datasource.name
    assert batch.batch_request.data_asset_name == asset.name

    path = "yellow_tripdata_sample_2018-04.csv"
    assert batch.batch_request.options == {"path": path, "year": "2018", "month": "04"}
    assert batch.metadata == {"path": path, "year": "2018", "month": "04"}

    assert batch.id == "pandas_filesystem_datasource-csv_asset-year_2018-month_04"


@pytest.mark.unit
@pytest.mark.parametrize(
    "year,month,path,batch_count",
    [
        ("2018", "04", "yellow_tripdata_sample_2018-04.csv", 1),
        ("2018", None, None, 12),
        (None, "04", None, 3),
        (None, "03", "yellow_tripdata_sample_2018-04.csv", 0),
    ],
)
def test_get_batch_list_batch_count(
    year: Optional[str],
    month: Optional[str],
    path: Optional[str],
    batch_count: int,
    pandas_filesystem_datasource: PandasFilesystemDatasource,
):
    asset = pandas_filesystem_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
    )
    request = asset.build_batch_request({"year": year, "month": month, "path": path})
    batches = asset.get_batch_list_from_batch_request(request)
    assert len(batches) == batch_count


@pytest.mark.unit
def test_get_batch_list_from_partially_specified_batch_request(
    pandas_filesystem_datasource: PandasFilesystemDatasource,
):
    # Verify test directory has files that don't match what we will query for
    file_name: PathStr
    all_files: list[str] = [
        file_name.stem
        for file_name in list(
            pathlib.Path(pandas_filesystem_datasource.base_directory).iterdir()
        )
    ]
    # assert there are files that are not csv files
    assert any(not file_name.endswith("csv") for file_name in all_files)
    # assert there are 12 files from 2018
    files_for_2018 = [
        file_name for file_name in all_files if file_name.find("2018") >= 0
    ]
    assert len(files_for_2018) == 12

    asset = pandas_filesystem_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
    )
    request = asset.build_batch_request({"year": "2018"})
    batches = asset.get_batch_list_from_batch_request(request)
    assert (len(batches)) == 12
    batch_filenames = [pathlib.Path(batch.metadata["path"]).stem for batch in batches]
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


@pytest.mark.timeout(
    6.0  # this test can take longer than the default timeout, try to reduce it
)
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
    pandas_filesystem_datasource: PandasFilesystemDatasource,
    order_by: SortersDefinition,
):
    # Verify test directory has files we expect
    years = ["2018", "2019", "2020"]
    months = [format(m, "02d") for m in range(1, 13)]
    file_name: PathStr
    all_files: list[str] = [
        file_name.stem
        for file_name in list(
            pathlib.Path(pandas_filesystem_datasource.base_directory).iterdir()
        )
    ]
    # assert there are 12 files for each year
    for year in years:
        files_for_year = [
            file_name
            for file_name in all_files
            if file_name.find(f"yellow_tripdata_sample_{year}") == 0
        ]
        assert len(files_for_year) == 12

    asset = pandas_filesystem_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
        order_by=order_by,
    )
    batches = asset.get_batch_list_from_batch_request(asset.build_batch_request())
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


@pytest.mark.unit
@pytest.mark.parametrize(
    "batch_slice, expected_batch_count",
    [
        ("[-3:]", 3),
        ("[5:9]", 4),
        ("[:10:2]", 5),
        (slice(-3, None), 3),
        (slice(5, 9), 4),
        (slice(0, 10, 2), 5),
        ("-5", 1),
        ("-1", 1),
        (11, 1),
        (0, 1),
        ([3], 1),
        (None, 12),
        ("", 12),
    ],
)
def test_pandas_slice_batch_count(
    pandas_filesystem_datasource: PandasFilesystemDatasource,
    batch_slice: BatchSlice,
    expected_batch_count: int,
) -> None:
    asset = pandas_filesystem_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
    )
    batch_request = asset.build_batch_request(
        options={"year": "2019"},
        batch_slice=batch_slice,
    )
    batches = asset.get_batch_list_from_batch_request(batch_request=batch_request)
    assert len(batches) == expected_batch_count


def bad_batching_regex_config(
    csv_path: pathlib.Path,
) -> tuple[re.Pattern, TestConnectionError]:
    batching_regex = re.compile(
        r"green_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
    )
    test_connection_error = TestConnectionError(
        "No file at base_directory path "
        f'"{csv_path.resolve()}" matched regular expressions pattern '
        f'"{batching_regex.pattern}" and/or glob_directive "**/*" for '
        'DataAsset "csv_asset".'
    )
    return batching_regex, test_connection_error


@pytest.fixture(params=[bad_batching_regex_config])
def datasource_test_connection_error_messages(
    csv_path: pathlib.Path,
    pandas_filesystem_datasource: PandasFilesystemDatasource,
    request,
) -> tuple[PandasFilesystemDatasource, TestConnectionError]:
    batching_regex, test_connection_error = request.param(csv_path=csv_path)
    csv_asset = CSVAsset(  # type: ignore[call-arg]
        name="csv_asset",
        batching_regex=batching_regex,
    )
    csv_asset._datasource = pandas_filesystem_datasource
    pandas_filesystem_datasource.assets = [
        csv_asset,
    ]
    csv_asset._data_connector = FilesystemDataConnector(
        datasource_name=pandas_filesystem_datasource.name,
        data_asset_name=csv_asset.name,
        batching_regex=batching_regex,
        base_directory=pandas_filesystem_datasource.base_directory,
        data_context_root_directory=pandas_filesystem_datasource.data_context_root_directory,
    )
    csv_asset._test_connection_error_message = test_connection_error
    return pandas_filesystem_datasource, test_connection_error


@pytest.mark.unit
def test_test_connection_failures(
    datasource_test_connection_error_messages: tuple[
        PandasFilesystemDatasource, TestConnectionError
    ]
):
    (
        pandas_filesystem_datasource,
        test_connection_error,
    ) = datasource_test_connection_error_messages

    with pytest.raises(type(test_connection_error)) as e:
        pandas_filesystem_datasource.test_connection()
    assert str(e.value) == str(test_connection_error)


@pytest.mark.timeout(
    5,  # deepcopy operation can be slow. Try to eliminate it in the future.
)
@pytest.mark.unit
def test_csv_asset_batch_metadata(
    pandas_filesystem_datasource: PandasFilesystemDatasource,
):
    my_config_variables = {"pipeline_filename": __file__}
    pandas_filesystem_datasource._data_context.config_variables.update(  # type: ignore[union-attr] # `_data_context`
        my_config_variables
    )

    asset_specified_metadata = {
        "pipeline_name": "my_pipeline",
        "no_curly_pipeline_filename": "$pipeline_filename",
        "curly_pipeline_filename": "${pipeline_filename}",
    }

    asset = pandas_filesystem_datasource.add_csv_asset(
        name="csv_asset",
        batching_regex=r"yellow_tripdata_sample_\d{4}-(?P<month>\d{2})\.csv",
        batch_metadata=asset_specified_metadata,
    )
    assert asset.batch_metadata == asset_specified_metadata

    batch_request = asset.build_batch_request()

    batches = pandas_filesystem_datasource.get_batch_list_from_batch_request(
        batch_request
    )

    substituted_batch_metadata: BatchMetadata = copy.deepcopy(asset_specified_metadata)
    substituted_batch_metadata.update(
        {
            "no_curly_pipeline_filename": __file__,
            "curly_pipeline_filename": __file__,
        }
    )

    months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]

    for i, month in enumerate(months):
        substituted_batch_metadata["month"] = month
        actual_metadata = copy.deepcopy(batches[i].metadata)
        # not testing path for the purposes of this test
        actual_metadata.pop("path")
        assert actual_metadata == substituted_batch_metadata
