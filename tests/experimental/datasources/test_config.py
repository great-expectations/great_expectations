import functools
import json
import pathlib
from typing import Callable

import pytest

from great_expectations.experimental.datasources.config import GxConfig
from great_expectations.experimental.datasources.interfaces import Datasource

try:
    from devtools import PrettyFormat as pf
    from devtools import debug as pp
except ImportError:
    from pprint import pformat as pf  # type: ignore[assignment]
    from pprint import pprint as pp  # type: ignore[assignment]

p = pytest.param

EXPERIMENTAL_DATASOURCE_TEST_DIR = pathlib.Path(__file__).parent

PG_CONFIG_YAML_FILE = EXPERIMENTAL_DATASOURCE_TEST_DIR / "config.yaml"
PG_CONFIG_YAML_STR = PG_CONFIG_YAML_FILE.read_text()

# TODO: create PG_CONFIG_YAML_FILE/STR from this dict
PG_COMPLEX_CONFIG_DICT = {
    "datasources": {
        "my_pg_ds": {
            "connection_string": "postgres://foo.bar",
            "name": "my_pg_ds",
            "type": "postgres",
            "assets": {
                "my_table_asset_wo_splitters": {
                    "name": "my_table_asset_wo_splitters",
                    "table_name": "my_table",
                    "type": "table",
                },
                "with_splitters": {
                    "column_splitter": {
                        "column_name": "my_column",
                        "method_name": "foobar_it",
                        "name": "my_splitter",
                        "param_names": ["alpha", "bravo"],
                    },
                    "name": "with_splitters",
                    "table_name": "another_table",
                    "type": "table",
                },
            },
        }
    }
}
PG_COMPLEX_CONFIG_JSON = json.dumps(PG_COMPLEX_CONFIG_DICT)

SIMPLE_DS_DICT = {
    "datasources": {
        "my_ds": {
            "name": "my_ds",
            "type": "postgres",
            "connection_string": "postgres",
        }
    }
}


@pytest.mark.parametrize(
    ["load_method", "input_"],
    [
        p(GxConfig.parse_obj, SIMPLE_DS_DICT, id="simple pg config dict"),
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
    print(f"  Dumped Dict ->\n\n{pf(dumped)}")

    re_loaded: GxConfig = GxConfig.parse_obj(dumped)
    pp(re_loaded)
    assert re_loaded

    assert from_dict_gx_config == re_loaded


def test_json_config_round_trip(
    inject_engine_lookup_double, from_json_gx_config: GxConfig
):
    dumped: str = from_json_gx_config.json()
    print(f"  Dumped JSON ->\n\n{dumped}")

    re_loaded: GxConfig = GxConfig.parse_raw(dumped)
    pp(re_loaded)
    assert re_loaded

    assert from_json_gx_config == re_loaded


def test_yaml_config_round_trip(
    inject_engine_lookup_double, from_yaml_gx_config: GxConfig
):
    dumped: str = from_yaml_gx_config.yaml()
    print(f"  Dumped YAML ->\n\n{dumped}")

    re_loaded: GxConfig = GxConfig.parse_yaml(dumped)
    pp(re_loaded)
    assert re_loaded

    assert from_yaml_gx_config == re_loaded


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


@pytest.mark.xfail(reason="Key Ordering needs to be implemented")
def test_yaml_config_round_trip_ordering(
    inject_engine_lookup_double, from_yaml_gx_config: GxConfig
):
    dumped: str = from_yaml_gx_config.yaml()

    assert PG_CONFIG_YAML_STR == dumped
