import json
import pathlib
from typing import Callable

import pytest

from great_expectations.experimental.datasources.config import GxConfig
from great_expectations.experimental.datasources.interfaces import Datasource

try:
    from devtools import debug as pp
except ImportError:
    from pprint import pprint as pp  # type: ignore[assignment]

p = pytest.param

EXPERIMENTAL_DATASOURCE_TEST_DIR = pathlib.Path(__file__).parent

PG_CONFIG_YAML_FILE = EXPERIMENTAL_DATASOURCE_TEST_DIR / "config.yaml"
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
        p(GxConfig.parse_yaml, PG_CONFIG_YAML_FILE, id="pg_config.yaml file"),
        p(
            GxConfig.parse_yaml,
            PG_CONFIG_YAML_FILE.read_text(),
            id="pg_config yaml string",
        ),
    ],
)
def test_load_config(inject_engine_lookup_double, load_method: Callable, input_):
    loaded: GxConfig = load_method(input_)
    pp(loaded)
    assert loaded

    assert loaded.datasources
    for datasource in loaded.datasources.values():
        assert isinstance(datasource, Datasource)
