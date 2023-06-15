from __future__ import annotations

import copy
import inspect
import logging
import pathlib
from pprint import pformat as pf
from typing import TYPE_CHECKING, Any, Callable, Type

import pydantic
import pytest
from pytest import MonkeyPatch, param

import great_expectations.execution_engine.pandas_execution_engine
from great_expectations.datasource.fluent import PandasDatasource
from great_expectations.datasource.fluent.dynamic_pandas import PANDAS_VERSION
from great_expectations.datasource.fluent.pandas_datasource import (
    _DYNAMIC_ASSET_TYPES,
    CSVAsset,
    DataFrameAsset,
    TableAsset,
    _PandasDataAsset,
)
from great_expectations.datasource.fluent.sources import (
    DEFAULT_PANDAS_DATA_ASSET_NAME,
    DEFAULT_PANDAS_DATASOURCE_NAME,
    DefaultPandasDatasourceError,
    _get_field_details,
)
from great_expectations.util import camel_to_snake
from great_expectations.validator.validator import Validator

if TYPE_CHECKING:
    import pandas as pd

    from great_expectations.data_context import AbstractDataContext


logger = logging.getLogger(__file__)

# apply markers to entire test module
pytestmark = [
    pytest.mark.skipif(
        PANDAS_VERSION < 1.2, reason=f"Fluent pandas not supported on {PANDAS_VERSION}"
    )
]


@pytest.fixture
def pandas_datasource() -> PandasDatasource:
    return PandasDatasource(  # type: ignore[call-arg] # type field not required
        name="pandas_datasource",
    )


@pytest.fixture
def valid_file_path(csv_path: pathlib.Path) -> pathlib.Path:
    return csv_path / "yellow_tripdata_sample_2018-03.csv"


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
            param("read_fwf"),
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
            camel_to_snake(t.__name__).split("_asset")[0]
            for t in PandasDatasource.asset_types
        }
        print(asset_class_names)

        assert type_name in PandasDatasource._type_lookup
        assert type_name in asset_class_names

    @pytest.mark.parametrize("asset_class", _DYNAMIC_ASSET_TYPES)
    def test_add_asset_method_exists_and_is_functional(
        self, asset_class: Type[_PandasDataAsset]
    ):
        type_name: str = _get_field_details(asset_class, "type").default_value
        method_name: str = f"add_{type_name}_asset"

        print(f"{method_name}() -> {asset_class.__name__}")

        assert method_name in PandasDatasource.__dict__

        ds = PandasDatasource(  # type: ignore[call-arg] # type field not required
            name="ds_for_testing_add_asset_methods",
        )
        method = getattr(ds, method_name)

        with pytest.raises(pydantic.ValidationError) as exc_info:
            positional_arg_string = "foo"
            positional_args: list[str] = []
            while len(positional_args) < 3:
                try:
                    method(
                        f"{asset_class.__name__}_add_asset_test",
                        *positional_args,
                        _invalid_key="bar",
                    )
                    break
                except TypeError:
                    positional_args.append(positional_arg_string)
        # importantly check that the method creates (or attempts to create) the intended asset
        assert exc_info.value.model == asset_class

    @pytest.mark.parametrize("asset_class", _DYNAMIC_ASSET_TYPES)
    def test_add_asset_method_signature(self, asset_class: Type[_PandasDataAsset]):
        type_name: str = _get_field_details(asset_class, "type").default_value
        method_name: str = f"add_{type_name}_asset"

        ds = PandasDatasource(  # type: ignore[call-arg] # type field not required
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

    @pytest.mark.parametrize("asset_class", _DYNAMIC_ASSET_TYPES)
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
            (CSVAsset, {"sep": "|", "names": ["col1", "col2", "col3"]}),
            (
                TableAsset,
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
            .add_csv_asset(
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

    @pytest.mark.parametrize(
        "read_method_name,positional_args",
        [
            param("read_clipboard", {}),
            param("read_csv", {"filepath_or_buffer": "valid_file_path"}),
            param("read_excel", {"io": "valid_file_path"}),
            param("read_feather", {"path": "valid_file_path"}),
            param("read_fwf", {"filepath_or_buffer": "valid_file_path"}),
            param("read_gbq", {"query": "SELECT * FROM my_table"}),
            param("read_hdf", {"path_or_buf": "valid_file_path"}),
            param("read_html", {"io": "valid_file_path"}),
            param("read_json", {"path_or_buf": "valid_file_path"}),
            param("read_orc", {"path": "valid_file_path"}),
            param("read_parquet", {"path": "valid_file_path"}),
            param("read_pickle", {"filepath_or_buffer": "valid_file_path"}),
            param("read_sas", {"filepath_or_buffer": "valid_file_path"}),
            param("read_spss", {"path": "valid_file_path"}),
            param("read_sql", {"sql": "SELECT * FROM my_table", "con": "sqlite://"}),
            param(
                "read_sql_query", {"sql": "SELECT * FROM my_table", "con": "sqlite://"}
            ),
            param("read_sql_table", {"table_name": "my_table", "con": "sqlite://"}),
            param("read_stata", {"filepath_or_buffer": "valid_file_path"}),
            param("read_table", {"filepath_or_buffer": "valid_file_path"}),
            param(
                "read_xml",
                {"path_or_buffer": "valid_file_path"},
                marks=pytest.mark.skipif(
                    PANDAS_VERSION < 1.3,
                    reason=f"read_xml does not exist on {PANDAS_VERSION} ",
                ),
            ),
        ],
    )
    def test_positional_arguments(
        self,
        mocker,
        empty_data_context: AbstractDataContext,
        read_method_name: str,
        positional_args: dict[str, str | pathlib.Path],
        request,
    ):
        if "valid_file_path" in positional_args.values():
            positional_args = {
                positional_arg_name: request.getfixturevalue("valid_file_path")
                for positional_arg_name, positional_arg in positional_args.items()
                if positional_arg == "valid_file_path"
            }

        add_method_name = "add_" + read_method_name.split("read_")[1] + "_asset"
        add_method: Callable = getattr(
            empty_data_context.sources.pandas_default, add_method_name
        )

        asset: _PandasDataAsset = add_method(
            "my_asset",
            *positional_args.values(),
        )
        for positional_arg_name, positional_arg in positional_args.items():
            assert getattr(asset, positional_arg_name) == positional_arg

        read_method: Callable = getattr(
            empty_data_context.sources.pandas_default, read_method_name
        )
        mocker.patch(
            "great_expectations.data_context.data_context.abstract_data_context.AbstractDataContext.get_validator"
        )
        _ = read_method(*positional_args.values())
        # read_* returns a validator, but we just want to inspect the asset
        asset = empty_data_context.sources.pandas_default.get_asset(
            asset_name=DEFAULT_PANDAS_DATA_ASSET_NAME
        )
        for positional_arg_name, positional_arg in positional_args.items():
            assert getattr(asset, positional_arg_name) == positional_arg


def test_default_pandas_datasource_get_and_set(
    empty_data_context: AbstractDataContext, valid_file_path: pathlib.Path
):
    pandas_datasource = empty_data_context.sources.pandas_default
    assert isinstance(pandas_datasource, PandasDatasource)
    assert pandas_datasource.name == DEFAULT_PANDAS_DATASOURCE_NAME
    assert len(pandas_datasource.assets) == 0

    validator = pandas_datasource.read_csv(
        filepath_or_buffer=valid_file_path,
    )
    assert isinstance(validator, Validator)
    csv_data_asset_1 = pandas_datasource.get_asset(
        asset_name=DEFAULT_PANDAS_DATA_ASSET_NAME
    )
    assert isinstance(csv_data_asset_1, _PandasDataAsset)
    assert csv_data_asset_1.name == DEFAULT_PANDAS_DATA_ASSET_NAME
    assert len(pandas_datasource.assets) == 1

    # ensure we get the same datasource when we call pandas_default again
    pandas_datasource = empty_data_context.sources.pandas_default
    assert pandas_datasource.name == DEFAULT_PANDAS_DATASOURCE_NAME
    assert len(pandas_datasource.assets) == 1
    assert pandas_datasource.get_asset(asset_name=DEFAULT_PANDAS_DATA_ASSET_NAME)

    # ensure we overwrite the ephemeral data asset if no name is passed
    _ = pandas_datasource.read_csv(filepath_or_buffer=valid_file_path)
    assert csv_data_asset_1.name == DEFAULT_PANDAS_DATA_ASSET_NAME
    assert len(pandas_datasource.assets) == 1

    # ensure we get an additional named asset when one is passed
    expected_csv_data_asset_name = "my_csv_asset"
    _ = pandas_datasource.read_csv(
        asset_name=expected_csv_data_asset_name,
        filepath_or_buffer=valid_file_path,
    )
    csv_data_asset_2 = pandas_datasource.get_asset(
        asset_name=expected_csv_data_asset_name
    )
    assert csv_data_asset_2.name == expected_csv_data_asset_name
    assert len(pandas_datasource.assets) == 2

    # ensure ephemeral data assets are not serialized
    config_as_dict = empty_data_context.fluent_config.dict()["fluent_datasources"]
    print(f"{pf(config_as_dict)}")
    for ds in config_as_dict:
        for asset in ds.get("assets", []):
            assert asset["name"] != DEFAULT_PANDAS_DATA_ASSET_NAME


def test_default_pandas_datasource_name_conflict(
    empty_data_context: AbstractDataContext,
):
    # the datasource name is taken by legacy
    empty_data_context.add_datasource(
        name=DEFAULT_PANDAS_DATASOURCE_NAME, class_name="PandasDatasource"
    )
    with pytest.raises(DefaultPandasDatasourceError):
        _ = empty_data_context.sources.pandas_default

    # the datasource name is available
    empty_data_context.datasources.pop(DEFAULT_PANDAS_DATASOURCE_NAME)
    pandas_datasource = empty_data_context.sources.pandas_default
    assert isinstance(pandas_datasource, PandasDatasource)
    assert pandas_datasource.name == DEFAULT_PANDAS_DATASOURCE_NAME


def test_dataframe_asset(
    empty_data_context: AbstractDataContext, test_df_pandas: pd.DataFrame
):
    # validates that a dataframe object is passed
    with pytest.raises(ValueError) as exc_info:
        _ = empty_data_context.sources.pandas_default.read_dataframe(dataframe={})

    assert (
        'Cannot execute "PandasDatasource.read_dataframe()" without a valid "dataframe" argument.'
        in str(exc_info.value)
    )

    # correct working behavior with read method
    validator = empty_data_context.sources.pandas_default.read_dataframe(
        dataframe=test_df_pandas
    )
    assert isinstance(validator, Validator)
    assert isinstance(
        empty_data_context.sources.pandas_default.get_asset(
            asset_name=DEFAULT_PANDAS_DATA_ASSET_NAME
        ),
        DataFrameAsset,
    )

    # correct working behavior with add method
    dataframe_asset_name = "my_dataframe_asset"
    dataframe_asset = empty_data_context.sources.pandas_default.add_dataframe_asset(
        name=dataframe_asset_name
    )
    assert isinstance(dataframe_asset, DataFrameAsset)
    assert dataframe_asset.name == "my_dataframe_asset"
    assert len(empty_data_context.sources.pandas_default.assets) == 2
    _ = dataframe_asset.build_batch_request(dataframe=test_df_pandas)
    assert all(
        asset.dataframe.equals(test_df_pandas)  # type: ignore[attr-defined]
        for asset in empty_data_context.sources.pandas_default.assets
    )


def test_pandas_data_asset_batch_metadata(
    empty_data_context: AbstractDataContext, valid_file_path: pathlib.Path
):
    my_config_variables = {"pipeline_filename": __file__}
    empty_data_context.config_variables.update(my_config_variables)

    pandas_datasource = empty_data_context.sources.pandas_default

    batch_metadata = {
        "no_curly_pipeline_filename": "$pipeline_filename",
        "curly_pipeline_filename": "${pipeline_filename}",
        "pipeline_step": "transform_3",
    }

    csv_asset = pandas_datasource.add_csv_asset(
        name="my_csv_asset",
        filepath_or_buffer=valid_file_path,
        batch_metadata=batch_metadata,
    )
    assert csv_asset.batch_metadata == batch_metadata

    batch_list = csv_asset.get_batch_list_from_batch_request(
        csv_asset.build_batch_request()
    )
    assert len(batch_list) == 1

    # allow mutation of this attribute
    batch_list[0].metadata["also_this_one"] = "other_batch-level_value"

    substituted_batch_metadata = copy.deepcopy(batch_metadata)
    substituted_batch_metadata.update(
        {
            "no_curly_pipeline_filename": __file__,
            "curly_pipeline_filename": __file__,
            "also_this_one": "other_batch-level_value",
        }
    )
    assert batch_list[0].metadata == substituted_batch_metadata


def test_build_batch_request_raises_if_missing_dataframe(
    empty_data_context: AbstractDataContext,
):
    dataframe_asset = empty_data_context.sources.add_or_update_pandas(
        name="fluent_pandas_datasource"
    ).add_dataframe_asset(name="my_df_asset")

    with pytest.raises(ValueError) as e:
        dataframe_asset.build_batch_request()

    assert "Cannot build batch request for dataframe asset without a dataframe" in str(
        e.value
    )
