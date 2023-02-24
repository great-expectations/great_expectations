from __future__ import annotations

import inspect
import logging
import pathlib
from pprint import pformat as pf
from typing import TYPE_CHECKING, Any, Type

import pydantic
import pytest
from pytest import MonkeyPatch, param

import great_expectations.execution_engine.pandas_execution_engine
from great_expectations.experimental.datasources import PandasDatasource
from great_expectations.experimental.datasources.dynamic_pandas import PANDAS_VERSION
from great_expectations.experimental.datasources.pandas_datasource import (
    PandasCSVAsset,
    PandasTableAsset,
    _PandasDataAsset,
)
from great_expectations.experimental.datasources.sources import (
    DEFAULT_PANDAS_DATASOURCE_NAMES,
    DefaultPandasDatasourceError,
    _get_field_details,
)

if TYPE_CHECKING:
    from great_expectations.data_context import AbstractDataContext


logger = logging.getLogger(__file__)

# apply markers to entire test module
pytestmark = [
    pytest.mark.skipif(
        PANDAS_VERSION < 1.2, reason=f"ZEP pandas not supported on {PANDAS_VERSION}"
    )
]


@pytest.fixture
def pandas_datasource() -> PandasDatasource:
    return PandasDatasource(
        name="pandas_datasource",
    )


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
    @pytest.mark.parametrize(
        "method_name",
        [
            param("read_clipboard"),
            param("read_csv"),
            param("read_excel"),
            param("read_feather"),
            param(
                "read_fwf", marks=pytest.mark.xfail(reason="unhandled type annotation")
            ),
            param("read_gbq"),
            param("read_hdf"),
            param("read_html"),
            param("read_json"),
            param("read_orc"),
            param("read_parquet"),
            param("read_pickle"),
            param("read_sas"),
            param("read_spss"),
            param("read_sql"),
            param("read_sql_query"),
            param("read_sql_table"),
            param("read_stata"),
            param("read_table"),
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
            t.__name__.lower()[6:-5] for t in PandasDatasource.asset_types
        }
        print(asset_class_names)

        assert type_name.replace("_", "") in asset_class_names

    @pytest.mark.parametrize("asset_class", PandasDatasource.asset_types)
    def test_add_asset_method_exists_and_is_functional(
        self, asset_class: Type[_PandasDataAsset]
    ):
        type_name: str = _get_field_details(asset_class, "type").default_value
        method_name: str = f"add_{type_name}_asset"

        print(f"{method_name}() -> {asset_class.__name__}")

        assert method_name in PandasDatasource.__dict__

        ds = PandasDatasource(
            name="ds_for_testing_add_asset_methods",
        )
        method = getattr(ds, method_name)

        with pytest.raises(pydantic.ValidationError) as exc_info:
            method(
                f"{asset_class.__name__}_add_asset_test",
                regex="great_expectations",
                _invalid_key="foobar",
            )
        # importantly check that the method creates (or attempts to create) the intended asset
        assert exc_info.value.model == asset_class

    @pytest.mark.parametrize("asset_class", PandasDatasource.asset_types)
    def test_add_asset_method_signature(self, asset_class: Type[_PandasDataAsset]):
        type_name: str = _get_field_details(asset_class, "type").default_value
        method_name: str = f"add_{type_name}_asset"

        ds = PandasDatasource(
            name="ds_for_testing_add_asset_methods",
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

    @pytest.mark.parametrize("asset_class", PandasDatasource.asset_types)
    def test_minimal_validation(self, asset_class: Type[_PandasDataAsset]):
        """
        These parametrized tests ensures that every `PandasDatasource` asset model does some minimal
        validation, and doesn't accept arbitrary keyword arguments.
        This is also a proxy for testing that the dynamic pydantic model creation was successful.
        """
        with pytest.raises(pydantic.ValidationError) as exc_info:
            asset_class(  # type: ignore[call-arg]
                name="test",
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
            (PandasCSVAsset, {"sep": "|", "names": ["col1", "col2", "col3"]}),
            (
                PandasTableAsset,
                {
                    "sep": "|",
                    "names": ["col1", "col2", "col3", "col4"],
                    "skiprows": [2, 4, 5],
                },
            ),
        ],
    )
    def test_data_asset_defaults(
        self,
        csv_path: pathlib.Path,
        asset_model: Type[_PandasDataAsset],
        extra_kwargs: dict,
    ):
        """
        Test that an asset dictionary can be dumped with only the original passed keys
        present.
        """
        kwargs: dict[str, Any] = {
            "name": "test",
            "filepath_or_buffer": csv_path / "yellow_tripdata_sample_2018-04.csv",
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
        extra_kwargs.update(
            {"filepath_or_buffer": csv_path / "yellow_tripdata_sample_2018-04.csv"}
        )
        batch_request = (
            empty_data_context.sources.add_pandas(
                "my_pandas",
            )
            .add_pandas_csv_asset(
                "my_csv",
                **extra_kwargs,
            )
            .build_batch_request()
        )
        with pytest.raises(SpyInterrupt):
            empty_data_context.get_validator(batch_request=batch_request)

        captured_args, captured_kwargs = capture_reader_fn_params
        print(f"positional args:\n{pf(captured_args[-1])}\n")
        print(f"keyword args:\n{pf(captured_kwargs[-1])}")

        assert captured_kwargs[-1] == extra_kwargs

    def test_default_pandas_datasource_get_and_set(
        self, empty_data_context: AbstractDataContext, csv_path: pathlib.Path
    ):
        pandas_datasource = empty_data_context.sources.pandas_default
        assert isinstance(pandas_datasource, PandasDatasource)
        assert pandas_datasource.name == "default_pandas_datasource"
        assert len(pandas_datasource.assets) == 0

        # TODO: Update the following 3 lines after registry namespace change to:
        #       - pandas_csv_asset_X -> csv_asset_X
        #       - read_pandas_csv -> read_csv
        expected_csv_data_asset_name_1 = "pandas_csv_asset_1"
        expected_csv_data_asset_name_2 = "pandas_csv_asset_2"
        csv_data_asset_1 = pandas_datasource.read_pandas_csv(  # type: ignore[attr-defined]
            filepath_or_buffer=csv_path / "yellow_tripdata_sample_2018-04.csv",
        )
        assert isinstance(csv_data_asset_1, _PandasDataAsset)
        assert csv_data_asset_1.name == expected_csv_data_asset_name_1
        assert len(pandas_datasource.assets) == 1

        # ensure we get the same datasource when we call pandas_default again
        pandas_datasource = empty_data_context.sources.pandas_default
        assert pandas_datasource.name == "default_pandas_datasource"
        assert len(pandas_datasource.assets) == 1
        assert pandas_datasource.assets[expected_csv_data_asset_name_1]

        csv_data_asset_2 = pandas_datasource.read_pandas_csv(  # type: ignore[attr-defined]
            filepath_or_buffer=csv_path / "yellow_tripdata_sample_2018-03.csv"
        )
        assert csv_data_asset_2.name == expected_csv_data_asset_name_2
        assert len(pandas_datasource.assets) == 2

    def test_default_pandas_datasource_name_conflict(
        self, empty_data_context: AbstractDataContext
    ):
        (
            default_pandas_datasource_name_1,
            default_pandas_datasource_name_2,
        ) = DEFAULT_PANDAS_DATASOURCE_NAMES

        # These add_datasource calls will create legacy PandasDatasources
        empty_data_context.add_datasource(
            name=default_pandas_datasource_name_1, class_name="PandasDatasource"
        )
        empty_data_context.add_datasource(
            name=default_pandas_datasource_name_2, class_name="PandasDatasource"
        )

        # both datasource names are taken by legacy datasources
        with pytest.raises(DefaultPandasDatasourceError):
            pandas_datasource = empty_data_context.sources.pandas_default

        # only datasource name 1 is taken by legacy datasources
        empty_data_context.datasources.pop(default_pandas_datasource_name_2)
        pandas_datasource = empty_data_context.sources.pandas_default
        assert isinstance(pandas_datasource, PandasDatasource)
        assert pandas_datasource.name == default_pandas_datasource_name_2

        # both datasource names are available
        empty_data_context.datasources.pop(default_pandas_datasource_name_1)
        empty_data_context.datasources.pop(default_pandas_datasource_name_2)
        pandas_datasource = empty_data_context.sources.pandas_default
        assert isinstance(pandas_datasource, PandasDatasource)
        assert pandas_datasource.name == default_pandas_datasource_name_1
