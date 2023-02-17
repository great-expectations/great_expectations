import functools
import json
import pathlib
import re
from pprint import pformat as pf
from typing import Callable, List

import pydantic
import pytest

from great_expectations.data_context import FileDataContext
from great_expectations.experimental.datasources.config import GxConfig
from great_expectations.experimental.datasources.interfaces import Datasource
from great_expectations.experimental.datasources.sources import _SourceFactories
from great_expectations.experimental.datasources.sql_datasource import (
    ColumnSplitter,
    TableAsset,
)

try:
    from devtools import debug as pp
except ImportError:
    from pprint import pprint as pp

p = pytest.param

EXPERIMENTAL_DATASOURCE_TEST_DIR = pathlib.Path(__file__).parent

PG_CONFIG_YAML_FILE = EXPERIMENTAL_DATASOURCE_TEST_DIR / FileDataContext.GX_YML
PG_CONFIG_YAML_STR = PG_CONFIG_YAML_FILE.read_text()

# TODO: create PG_CONFIG_YAML_FILE/STR from this dict
PG_COMPLEX_CONFIG_DICT = {
    "xdatasources": {
        "my_pg_ds": {
            "connection_string": "postgresql://userName:@hostname/dbName",
            "name": "my_pg_ds",
            "type": "postgres",
            "assets": {
                "my_table_asset_wo_splitters": {
                    "name": "my_table_asset_wo_splitters",
                    "table_name": "my_table",
                    "type": "table",
                },
                "with_splitter": {
                    "column_splitter": {
                        "column_name": "my_column",
                        "method_name": "split_on_year_and_month",
                        "name": "y_m_splitter",
                        "param_names": ["year", "month"],
                    },
                    "name": "with_splitter",
                    "table_name": "another_table",
                    "type": "table",
                },
                "with_sorters": {
                    "order_by": [
                        {"key": "year"},
                        {"key": "month", "reverse": True},
                    ],
                    "name": "with_sorters",
                    "table_name": "yet_another_table",
                    "type": "table",
                },
                "with_dslish_sorters": {
                    "order_by": ["year", "-month"],
                    "name": "with_sorters",
                    "table_name": "yet_another_table",
                    "type": "table",
                },
            },
        },
        "my_pandas_ds": {
            "type": "pandas_filesystem",
            "name": "my_pandas_ds",
            "base_directory": __file__,
            "assets": {
                "my_csv_asset": {
                    "name": "my_csv_asset",
                    "type": "csv",
                    "regex": r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).csv",
                    "sep": "|",
                    "names": ["col1", "col2"],
                },
                "my_json_asset": {
                    "name": "my_json_asset",
                    "type": "json",
                    "regex": r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).json",
                    "orient": "records",
                },
            },
        },
    }
}
PG_COMPLEX_CONFIG_JSON = json.dumps(PG_COMPLEX_CONFIG_DICT)

SIMPLE_DS_DICT = {
    "xdatasources": {
        "my_ds": {
            "name": "my_ds",
            "type": "sql",
            "connection_string": "sqlite://",
        }
    }
}

COMBINED_ZEP_AND_OLD_STYLE_CFG_DICT = {
    "xdatasources": {
        "my_ds": {
            "name": "my_ds",
            "type": "sql",
            "connection_string": "sqlite://",
        }
    },
    "name": "getting_started_datasource",
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "PandasExecutionEngine",
    },
    "data_connectors": {
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetFilesystemDataConnector",
            "base_directory": "../data/",
            "default_regex": {
                "group_names": ["data_asset_name"],
                "pattern": "(.*)",
            },
        },
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "assets": {
                "my_runtime_asset_name": {
                    "batch_identifiers": ["runtime_batch_identifier_name"]
                }
            },
        },
    },
}


@pytest.mark.parametrize(
    "asset_dict", [{"type": "json", "orient": "records"}, {"type": "csv", "sep": "|"}]
)
class TestExcludeUnsetAssetFields:
    """
    Ensure that DataAsset fields are excluded from serialization if they have not be explicitly set.

    We are trying to ensure that our configs aren't filled with default values from DataAssets that
    users never set.
    """

    def test_from_datasource(self, asset_dict: dict):
        ds_mapping = {"csv": "pandas_filesystem", "json": "pandas_filesystem"}

        ds_type_: str = ds_mapping[asset_dict["type"]]
        ds_class = _SourceFactories.type_lookup[ds_type_]

        # fill in required args
        asset_dict.update(
            {
                "name": "my_asset",
                "regex": re.compile(r"sample_(?P<year>\d{4})-(?P<month>\d{2}).csv"),
            }
        )
        asset_name = asset_dict["name"]
        ds_dict = {
            "name": "my_ds",
            "base_directory": pathlib.Path(__file__),
            "assets": {asset_name: asset_dict},
        }
        datasource: Datasource = ds_class.parse_obj(ds_dict)
        assert asset_dict == datasource.dict()["assets"][asset_name]

    def test_from_gx_config(self, asset_dict: dict):
        """
        Ensure that unset fields are excluded even when being parsed by the the top-level `GxConfig` class.
        """
        # fill in required args
        asset_dict.update(
            {
                "name": "my_asset",
                "regex": re.compile(r"sample_(?P<year>\d{4})-(?P<month>\d{2}).csv"),
            }
        )
        asset_name = asset_dict["name"]
        ds_dict = {
            "name": "my_ds",
            "type": "pandas_filesystem",
            "base_directory": pathlib.Path(__file__),
            "assets": {asset_name: asset_dict},
        }
        gx_config = GxConfig.parse_obj({"xdatasources": {"my_ds": ds_dict}})

        gx_config_dict = gx_config.dict()
        print(f"gx_config_dict\n{pf(gx_config_dict)}")
        assert (
            asset_dict == gx_config_dict["xdatasources"]["my_ds"]["assets"][asset_name]
        )


@pytest.mark.parametrize(
    ["load_method", "input_"],
    [
        p(GxConfig.parse_obj, SIMPLE_DS_DICT, id="simple pg config dict"),
        p(
            GxConfig.parse_obj,
            COMBINED_ZEP_AND_OLD_STYLE_CFG_DICT,
            id="zep + old style config",
        ),
        p(GxConfig.parse_raw, json.dumps(SIMPLE_DS_DICT), id="simple pg json"),
        p(GxConfig.parse_obj, PG_COMPLEX_CONFIG_DICT, id="pg complex dict"),
        p(GxConfig.parse_raw, PG_COMPLEX_CONFIG_JSON, id="pg complex json"),
        p(GxConfig.parse_yaml, PG_CONFIG_YAML_FILE, id="pg_config.yaml file"),
        p(GxConfig.parse_yaml, PG_CONFIG_YAML_STR, id="pg_config yaml string"),
    ],
)
def test_load_config(inject_engine_lookup_double, load_method: Callable, input_):
    loaded: GxConfig = load_method(input_)
    pp(loaded)
    assert loaded

    assert loaded.datasources
    for datasource in loaded.datasources.values():
        assert isinstance(datasource, Datasource)


@pytest.mark.unit
@pytest.mark.parametrize(
    ["config", "expected_error_loc", "expected_msg"],
    [
        p({}, ("xdatasources",), "field required", id="no datasources"),
        p(
            {
                "xdatasources": {
                    "my_bad_ds_missing_type": {
                        "name": "my_bad_ds_missing_type",
                    }
                }
            },
            ("xdatasources",),
            "'my_bad_ds_missing_type' is missing a 'type' entry",
            id="missing 'type' field",
        ),
    ],
)
def test_catch_bad_top_level_config(
    inject_engine_lookup_double,
    config: dict,
    expected_error_loc: tuple,
    expected_msg: str,
):
    print(f"  config\n{pf(config)}\n")
    with pytest.raises(pydantic.ValidationError) as exc_info:
        loaded = GxConfig.parse_obj(config)
        print(f"Erroneously loaded config\n{loaded}\n")

    print(f"\n{exc_info.typename}:{exc_info.value}")
    all_errors = exc_info.value.errors()
    print(f"\nErrors dict\n{pf(all_errors)}")

    assert len(all_errors) == 1, "Expected 1 error"
    assert expected_error_loc == all_errors[0]["loc"]
    assert expected_msg == all_errors[0]["msg"]


@pytest.mark.unit
@pytest.mark.parametrize(
    ["bad_asset_config", "expected_error_loc", "expected_msg"],
    [
        p(
            {"name": "missing `table_name`", "type": "table"},
            ("xdatasources", "assets", "missing `table_name`", "table_name"),
            "field required",
            id="missing `table_name`",
        ),
        p(
            {
                "name": "unknown splitter",
                "type": "table",
                "table_name": "pool",
                "column_splitter": {
                    "method_name": "not_a_valid_method_name",
                    "column_name": "foo",
                },
            },
            (
                "xdatasources",
                "assets",
                "unknown splitter",
                "column_splitter",
                "method_name",
            ),
            "unexpected value; permitted:",
            id="unknown splitter method",
        ),
    ],
)
def test_catch_bad_asset_configs(
    inject_engine_lookup_double,
    bad_asset_config: dict,
    expected_error_loc: tuple,
    expected_msg: str,
):
    config: dict = {
        "my_test_ds": {
            "type": "postgres",
            "name": "my_test_ds",
            "connection_string": "postgres://userName:@hostname/dbName",
            "assets": {bad_asset_config["name"]: bad_asset_config},
        }
    }
    print(f"  Config\n{pf(config)}\n")

    with pytest.raises(pydantic.ValidationError) as exc_info:
        GxConfig.parse_obj({"xdatasources": config})

    print(f"\n{exc_info.typename}:{exc_info.value}")

    all_errors = exc_info.value.errors()
    assert len(all_errors) >= 1, "Expected at least 1 error"
    test_msg = ""
    for error in all_errors:
        if expected_error_loc == all_errors[0]["loc"]:
            test_msg = error["msg"]
            break
    assert test_msg.startswith(expected_msg)


@pytest.mark.unit
@pytest.mark.parametrize(
    ["bad_column_kwargs", "expected_error_type", "expected_msg"],
    [
        (
            {
                "column_name": "flavor",
                "method_name": "NOT_VALID",
                "param_names": ["cherry", "strawberry"],
            },
            "value_error",
            "unexpected value; permitted:",
        )
    ],
)
def test_general_column_splitter_errors(
    inject_engine_lookup_double,
    bad_column_kwargs: dict,
    expected_error_type: str,
    expected_msg: str,
):

    with pytest.raises(pydantic.ValidationError) as exc_info:
        ColumnSplitter(**bad_column_kwargs)

    print(f"\n{exc_info.typename}:{exc_info.value}")

    all_errors = exc_info.value.errors()
    assert len(all_errors) == 1, "Expected 1 error"
    assert expected_error_type == all_errors[0]["type"]
    assert all_errors[0]["msg"].startswith(expected_msg)


@pytest.fixture
@functools.lru_cache(maxsize=1)
def from_dict_gx_config() -> GxConfig:
    gx_config = GxConfig.parse_obj(PG_COMPLEX_CONFIG_DICT)
    assert gx_config
    return gx_config


@pytest.fixture
@functools.lru_cache(maxsize=1)
def from_json_gx_config() -> GxConfig:
    gx_config = GxConfig.parse_raw(PG_COMPLEX_CONFIG_JSON)
    assert gx_config
    return gx_config


@pytest.fixture
@functools.lru_cache(maxsize=1)
def from_yaml_gx_config() -> GxConfig:
    gx_config = GxConfig.parse_yaml(PG_CONFIG_YAML_STR)
    assert gx_config
    return gx_config


def test_dict_config_round_trip(
    inject_engine_lookup_double, from_dict_gx_config: GxConfig
):
    dumped: dict = from_dict_gx_config.dict()
    print(f"  Dumped Dict ->\n\n{pf(dumped)}\n")

    re_loaded: GxConfig = GxConfig.parse_obj(dumped)
    pp(re_loaded)
    assert re_loaded

    assert from_dict_gx_config == re_loaded


def test_json_config_round_trip(
    inject_engine_lookup_double, from_json_gx_config: GxConfig
):
    dumped: str = from_json_gx_config.json(indent=2)
    print(f"  Dumped JSON ->\n\n{dumped}\n")

    re_loaded: GxConfig = GxConfig.parse_raw(dumped)
    pp(re_loaded)
    assert re_loaded

    assert from_json_gx_config.dict() == re_loaded.dict()


def test_yaml_config_round_trip(
    inject_engine_lookup_double, from_yaml_gx_config: GxConfig
):
    dumped: str = from_yaml_gx_config.yaml()
    print(f"  Dumped YAML ->\n\n{dumped}\n")

    re_loaded: GxConfig = GxConfig.parse_yaml(dumped)
    pp(re_loaded)
    assert re_loaded

    assert from_yaml_gx_config.dict() == re_loaded.dict()
    assert dumped == re_loaded.yaml()


def test_yaml_file_config_round_trip(
    inject_engine_lookup_double, tmp_path: pathlib.Path, from_yaml_gx_config: GxConfig
):
    yaml_file = tmp_path / "test.yaml"
    assert not yaml_file.exists()

    result_path = from_yaml_gx_config.yaml(yaml_file)
    assert yaml_file.exists()
    assert result_path == yaml_file

    print(f"  yaml_file -> \n\n{yaml_file.read_text()}")

    re_loaded: GxConfig = GxConfig.parse_yaml(yaml_file)
    pp(re_loaded)
    assert re_loaded

    assert from_yaml_gx_config == re_loaded


def test_splitters_deserialization(
    inject_engine_lookup_double, from_json_gx_config: GxConfig
):
    table_asset: TableAsset = from_json_gx_config.datasources["my_pg_ds"].assets[
        "with_splitter"
    ]
    assert isinstance(table_asset.column_splitter, ColumnSplitter)
    assert table_asset.column_splitter.method_name == "split_on_year_and_month"


# TDD Tests for future work


@pytest.mark.xfail(reason="Key Ordering needs to be implemented")
def test_yaml_config_round_trip_ordering(
    inject_engine_lookup_double, from_yaml_gx_config: GxConfig
):
    dumped: str = from_yaml_gx_config.yaml()

    assert PG_CONFIG_YAML_STR == dumped


@pytest.mark.xfail(
    reason="Custom BatchSorter serialization logic needs to be implemented"
)
def test_custom_sorter_serialization(
    inject_engine_lookup_double, from_json_gx_config: GxConfig
):
    dumped: str = from_json_gx_config.json(indent=2)
    print(f"  Dumped JSON ->\n\n{dumped}\n")

    expected_sorter_strings: List[str] = PG_COMPLEX_CONFIG_DICT["xdatasources"][  # type: ignore[index]
        "my_pg_ds"
    ][
        "assets"
    ][
        "with_dslish_sorters"
    ][
        "order_by"
    ]

    assert '"reverse": True' not in dumped
    assert '{"key":' not in dumped

    for sorter_str in expected_sorter_strings:
        assert sorter_str in dumped, f"`{sorter_str}` not found in dumped json"
