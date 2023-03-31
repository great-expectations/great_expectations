from __future__ import annotations

import copy
import functools
import json
import logging
import pathlib
import re
import uuid
from pprint import pformat as pf
from pprint import pprint as pp
from typing import TYPE_CHECKING, Callable, List

import pydantic
import pytest
from typing_extensions import Final

from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context import FileDataContext
from great_expectations.datasource.fluent.config import GxConfig
from great_expectations.datasource.fluent.constants import _ASSETS_KEY
from great_expectations.datasource.fluent.interfaces import Datasource
from great_expectations.datasource.fluent.sources import (
    DEFAULT_PANDAS_DATA_ASSET_NAME,
    DEFAULT_PANDAS_DATASOURCE_NAME,
    _SourceFactories,
)
from great_expectations.datasource.fluent.sql_datasource import (
    SplitterYearAndMonth,
    TableAsset,
)

if TYPE_CHECKING:
    from pytest import FixtureRequest

    from great_expectations.datasource.fluent import SqliteDatasource

yaml = YAMLHandler()
LOGGER = logging.getLogger(__file__)

p = pytest.param

EXPERIMENTAL_DATASOURCE_TEST_DIR = pathlib.Path(__file__).parent
CSV_PATH = EXPERIMENTAL_DATASOURCE_TEST_DIR.joinpath(
    pathlib.Path("..", "..", "test_sets", "taxi_yellow_tripdata_samples")
)

PG_CONFIG_YAML_FILE = EXPERIMENTAL_DATASOURCE_TEST_DIR / FileDataContext.GX_YML
PG_CONFIG_YAML_STR: Final[str] = PG_CONFIG_YAML_FILE.read_text()

# TODO: create PG_CONFIG_YAML_FILE/STR from this dict
COMPLEX_CONFIG_DICT: Final[dict] = {
    "fluent_datasources": {
        "my_pg_ds": {
            "connection_string": "postgresql://userName:@hostname/dbName",
            "kwargs": {"echo": True},
            "name": "my_pg_ds",
            "type": "postgres",
            "assets": {
                "my_table_asset_wo_splitters": {
                    "name": "my_table_asset_wo_splitters",
                    "table_name": "my_table",
                    "type": "table",
                },
                "with_splitter": {
                    "splitter": {
                        "column_name": "my_column",
                        "method_name": "split_on_year_and_month",
                    },
                    "table_name": "another_table",
                    "name": "with_splitter",
                    "type": "table",
                },
                "with_sorters": {
                    "order_by": [
                        {"key": "year"},
                        {"key": "month", "reverse": True},
                    ],
                    "table_name": "yet_another_table",
                    "name": "with_sorters",
                    "type": "table",
                },
                "with_dslish_sorters": {
                    "order_by": ["year", "-month"],
                    "table_name": "yet_another_table",
                    "name": "with_dslish_sorters",
                    "type": "table",
                },
            },
        },
        "my_pandas_filesystem_ds": {
            "type": "pandas_filesystem",
            "name": "my_pandas_filesystem_ds",
            "base_directory": __file__,
            "assets": {
                "my_csv_asset": {
                    "type": "csv",
                    "name": "my_csv_asset",
                    "batching_regex": r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).csv",
                    "sep": "|",
                    "names": ["col1", "col2"],
                },
                "my_json_asset": {
                    "type": "json",
                    "name": "my_json_asset",
                    "batching_regex": r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).json",
                    "connect_options": {"glob_directive": "**/*.json"},
                    "orient": "records",
                },
            },
        },
    }
}
COMPLEX_CONFIG_JSON: Final[str] = json.dumps(COMPLEX_CONFIG_DICT)

SIMPLE_DS_DICT: Final[dict] = {
    "fluent_datasources": {
        "my_ds": {
            "type": "sql",
            "name": "my_ds",
            "connection_string": "sqlite://",
        }
    }
}

SIMPLE_DS_DICT_WITH_DS_NAME_ERROR = {
    "fluent_datasources": {
        "my_ds": {
            "type": "sql",
            "name": "my_incorrect_ds",
            "connection_string": "sqlite://",
        }
    }
}

SIMPLE_DS_DICT_WITH_ASSET_NAME_ERROR = {
    "fluent_datasources": {
        "my_ds": {
            "type": "sql",
            "name": "my_ds",
            "connection_string": "sqlite://",
            "assets": {
                "my_csv_asset": {
                    "type": "csv",
                    "name": "my_incorrect_csv_asset",
                    "batching_regex": r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).csv",
                },
            },
        }
    }
}

COMBINED_FLUENT_AND_OLD_STYLE_CFG_DICT = {
    "fluent_datasources": {
        "my_ds": {
            "type": "sql",
            "name": "my_ds",
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

DEFAULT_PANDAS_DATASOURCE_AND_DATA_ASSET_CONFIG_DICT: Final[dict] = {
    "fluent_datasources": {
        DEFAULT_PANDAS_DATASOURCE_NAME: {
            "type": "pandas",
            "name": DEFAULT_PANDAS_DATASOURCE_NAME,
            "assets": {
                DEFAULT_PANDAS_DATA_ASSET_NAME: {
                    "name": DEFAULT_PANDAS_DATA_ASSET_NAME,
                    "type": "csv",
                    "filepath_or_buffer": CSV_PATH
                    / "yellow_tripdata_sample_2018-04.csv",
                    "sep": "|",
                    "names": ["col1", "col2"],
                },
                "my_csv_asset": {
                    "name": "my_csv_asset",
                    "type": "csv",
                    "filepath_or_buffer": CSV_PATH
                    / "yellow_tripdata_sample_2018-04.csv",
                    "sep": "|",
                    "names": ["col1", "col2"],
                },
            },
        },
    }
}


@pytest.fixture
def ds_dict_config() -> dict:
    return copy.deepcopy(COMPLEX_CONFIG_DICT)


@pytest.fixture
def sqlite_database_path() -> pathlib.Path:
    relative_path = pathlib.Path(
        "..",
        "..",
        "test_sets",
        "taxi_yellow_tripdata_samples",
        "sqlite",
        "yellow_tripdata.db",
    )
    return pathlib.Path(__file__).parent.joinpath(relative_path).resolve(strict=True)


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
        asset_dict_config = copy.deepcopy(asset_dict)

        ds_mapping = {"csv": "pandas_filesystem", "json": "pandas_filesystem"}

        ds_type_: str = ds_mapping[asset_dict_config["type"]]
        ds_class = _SourceFactories.type_lookup[ds_type_]

        # fill in required args
        asset_dict_config.update(
            {
                "name": "my_asset",
                "batching_regex": re.compile(
                    r"sample_(?P<year>\d{4})-(?P<month>\d{2}).csv"
                ),
            }
        )
        asset_name = asset_dict_config["name"]
        ds_dict = {
            "name": "my_ds",
            "base_directory": pathlib.Path(__file__),
            "assets": {asset_name: asset_dict_config},
        }
        datasource: Datasource = ds_class.parse_obj(ds_dict)
        assert asset_dict_config == datasource.dict()["assets"][asset_name]

    def test_from_gx_config(self, asset_dict: dict):
        """
        Ensure that unset fields are excluded even when being parsed by the the top-level `GxConfig` class.
        """
        # fill in required args
        asset_dict.update(
            {
                "name": "my_asset",
                "batching_regex": re.compile(
                    r"sample_(?P<year>\d{4})-(?P<month>\d{2}).csv"
                ),
            }
        )
        asset_dict_config = copy.deepcopy(asset_dict)

        ds_dict = {
            "type": "pandas_filesystem",
            "base_directory": pathlib.Path(__file__),
            "assets": {"my_asset": asset_dict_config},
        }
        gx_config = GxConfig.parse_obj({"fluent_datasources": {"my_ds": ds_dict}})

        gx_config_dict = gx_config.dict()
        print(f"gx_config_dict\n{pf(gx_config_dict)}")
        assert (
            asset_dict
            == gx_config_dict["fluent_datasources"]["my_ds"]["assets"]["my_asset"]
        )


def test_id_only_serialized_if_present(ds_dict_config: dict):
    print(f"\tInput:\n\n{pf(ds_dict_config, depth=3)}")
    all_ids: list[str] = []
    with_ids: dict = {}
    no_ids: dict = {}

    # remove or add ids
    for ds_name, ds in ds_dict_config["fluent_datasources"].items():

        with_ids[ds_name] = copy.deepcopy(ds)
        no_ids[ds_name] = copy.deepcopy(ds)

        ds_id = uuid.uuid4()
        all_ids.append(str(ds_id))
        with_ids[ds_name]["id"] = ds_id

        no_ids[ds_name].pop("id", None)

        for asset_name in ds["assets"].keys():

            asset_id = uuid.uuid4()
            all_ids.append(str(asset_id))
            with_ids[ds_name]["assets"][asset_name]["id"] = asset_id

            no_ids[ds_name]["assets"][asset_name].pop("id", None)

    gx_config_no_ids = GxConfig.parse_obj({"fluent_datasources": no_ids})
    gx_config_with_ids = GxConfig.parse_obj({"fluent_datasources": with_ids})

    assert "id" not in str(gx_config_no_ids.dict())
    assert "id" not in gx_config_no_ids.json()
    assert "id" not in gx_config_no_ids.yaml()

    for serialized_str in [
        str(gx_config_with_ids.dict()),
        gx_config_with_ids.json(),
        gx_config_with_ids.yaml(),
    ]:
        for id_ in all_ids:
            assert id_ in serialized_str


@pytest.mark.parametrize(
    ["load_method", "input_"],
    [
        p(GxConfig.parse_obj, SIMPLE_DS_DICT, id="simple pg config dict"),
        p(
            GxConfig.parse_obj,
            COMBINED_FLUENT_AND_OLD_STYLE_CFG_DICT,
            id="fluent + old style config",
        ),
        p(GxConfig.parse_raw, json.dumps(SIMPLE_DS_DICT), id="simple pg json"),
        p(GxConfig.parse_obj, COMPLEX_CONFIG_DICT, id="complex dict"),
        p(GxConfig.parse_raw, COMPLEX_CONFIG_JSON, id="complex json"),
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


@pytest.mark.parametrize(
    ["load_method", "input_"],
    [
        p(
            GxConfig.parse_obj,
            SIMPLE_DS_DICT_WITH_DS_NAME_ERROR,
            id="simple pg config dict with ds name error",
        ),
        p(
            GxConfig.parse_raw,
            json.dumps(SIMPLE_DS_DICT_WITH_DS_NAME_ERROR),
            id="simple pg json with ds name error",
        ),
    ],
)
def test_load_incorrect_ds_config_raises_error(
    inject_engine_lookup_double, load_method: Callable, input_
):
    with pytest.raises(pydantic.ValidationError) as exc_info:
        _ = load_method(input_)

    assert (
        str(exc_info.value)
        == '1 validation error for GxConfig\nfluent_datasources\n  Datasource key "my_ds" is different from name "my_incorrect_ds" in its configuration. (type=value_error)'
    )


@pytest.mark.parametrize(
    ["load_method", "input_"],
    [
        p(
            GxConfig.parse_obj,
            SIMPLE_DS_DICT_WITH_ASSET_NAME_ERROR,
            id="simple pg config dict with asset name error",
        ),
        p(
            GxConfig.parse_raw,
            json.dumps(SIMPLE_DS_DICT_WITH_ASSET_NAME_ERROR),
            id="simple pg json with asset name error",
        ),
    ],
)
def test_load_incorrect_asset_config_raises_error(
    inject_engine_lookup_double, load_method: Callable, input_
):
    with pytest.raises(pydantic.ValidationError) as exc_info:
        _ = load_method(input_)

    assert (
        str(exc_info.value)
        == '1 validation error for GxConfig\nfluent_datasources\n  DataAsset key "my_csv_asset" is different from name "my_incorrect_csv_asset" in its configuration. (type=value_error)'
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    ["config", "expected_error_loc", "expected_msg"],
    [
        p({}, ("fluent_datasources",), "field required", id="no datasources"),
        p(
            {
                "fluent_datasources": {
                    "my_bad_ds_missing_type": {
                        "name": "my_bad_ds_missing_type",
                    }
                }
            },
            ("fluent_datasources",),
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
            ("fluent_datasources", "assets", "missing `table_name`", "table_name"),
            "field required",
            id="missing `table_name`",
        ),
        p(
            {
                "name": "unknown splitter",
                "type": "table",
                "table_name": "pool",
                "splitter": {
                    "method_name": "not_a_valid_method_name",
                    "column_name": "foo",
                },
            },
            (
                "fluent_datasources",
                "assets",
                "unknown splitter",
                "splitter",
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
        GxConfig.parse_obj({"fluent_datasources": config})

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
            },
            "value_error.const",
            "unexpected value; permitted:",
        )
    ],
)
def test_general_splitter_errors(
    inject_engine_lookup_double,
    bad_column_kwargs: dict,
    expected_error_type: str,
    expected_msg: str,
):
    with pytest.raises(pydantic.ValidationError) as exc_info:
        SplitterYearAndMonth(**bad_column_kwargs)

    print(f"\n{exc_info.typename}:{exc_info.value}")

    all_errors = exc_info.value.errors()
    assert len(all_errors) == 1, "Expected 1 error"
    assert expected_error_type == all_errors[0]["type"]
    assert all_errors[0]["msg"].startswith(expected_msg)


@pytest.fixture
@functools.lru_cache(maxsize=1)
def from_dict_gx_config() -> GxConfig:
    gx_config = GxConfig.parse_obj(COMPLEX_CONFIG_DICT)
    assert gx_config
    return gx_config


@pytest.fixture
@functools.lru_cache(maxsize=1)
def from_json_gx_config() -> GxConfig:
    gx_config = GxConfig.parse_raw(COMPLEX_CONFIG_JSON)
    return gx_config


@pytest.fixture
@functools.lru_cache(maxsize=1)
def from_yaml_gx_config() -> GxConfig:
    gx_config = GxConfig.parse_yaml(PG_CONFIG_YAML_STR)
    return gx_config


@pytest.fixture(params=[from_dict_gx_config, from_json_gx_config, from_yaml_gx_config])
def from_all_config(request: FixtureRequest) -> GxConfig:
    """
    This fixture parametrizes all our config fixtures.
    This will in-turn parametrize any test that uses it, creating a test case for each
    `from_*_config` fixture
    """
    fixture_name = request.param.__name__
    return request.getfixturevalue(fixture_name)


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


def test_assets_key_presence(
    inject_engine_lookup_double, from_yaml_gx_config: GxConfig
):
    ds_wo_assets = None
    ds_with_assets = None
    for ds in from_yaml_gx_config.datasources.values():
        if ds.assets:
            ds_with_assets = ds
        else:
            ds_wo_assets = ds
    assert ds_with_assets, "Need at least one Datasource with assets for this test"
    assert ds_wo_assets, "Need at least one Datasource without assets for this test"

    dumped_as_dict: dict = yaml.load(from_yaml_gx_config.yaml())
    print(
        f"  dict from dumped yaml ->\n\n{pf(dumped_as_dict['fluent_datasources'], depth=2)}"
    )

    assert _ASSETS_KEY in dumped_as_dict["fluent_datasources"][ds_with_assets.name]
    assert _ASSETS_KEY not in dumped_as_dict["fluent_datasources"][ds_wo_assets.name]


def test_splitters_deserialization(
    inject_engine_lookup_double, from_all_config: GxConfig
):
    table_asset: TableAsset = from_all_config.datasources["my_pg_ds"].assets[
        "with_splitter"
    ]
    assert isinstance(table_asset.splitter, SplitterYearAndMonth)
    assert table_asset.splitter.method_name == "split_on_year_and_month"


# TDD Tests for future work


@pytest.mark.xfail(reason="Key Ordering needs to be implemented")
def test_yaml_config_round_trip_ordering(
    inject_engine_lookup_double, from_yaml_gx_config: GxConfig
):
    dumped: str = from_yaml_gx_config.yaml()

    assert PG_CONFIG_YAML_STR == dumped


@pytest.mark.xfail(reason="Custom Sorter serialization logic needs to be implemented")
def test_custom_sorter_serialization(
    inject_engine_lookup_double, from_json_gx_config: GxConfig
):
    dumped: str = from_json_gx_config.json(indent=2)
    print(f"  Dumped JSON ->\n\n{dumped}\n")

    expected_sorter_strings: List[str] = COMPLEX_CONFIG_DICT["fluent_datasources"][
        "my_pg_ds"
    ]["assets"]["with_dslish_sorters"]["order_by"]

    assert '"reverse": True' not in dumped
    assert '{"key":' not in dumped

    for sorter_str in expected_sorter_strings:
        assert sorter_str in dumped, f"`{sorter_str}` not found in dumped json"


def test_dict_default_pandas_config_round_trip(inject_engine_lookup_double):
    # the default data asset should be dropped, but one named asset should remain
    datasource_without_default_pandas_data_asset_config_dict = copy.deepcopy(
        DEFAULT_PANDAS_DATASOURCE_AND_DATA_ASSET_CONFIG_DICT
    )

    from_dict_default_pandas_config = GxConfig.parse_obj(
        DEFAULT_PANDAS_DATASOURCE_AND_DATA_ASSET_CONFIG_DICT
    )
    assert (
        DEFAULT_PANDAS_DATA_ASSET_NAME
        not in from_dict_default_pandas_config.fluent_datasources[
            DEFAULT_PANDAS_DATASOURCE_NAME
        ].assets
    )

    dumped: dict = from_dict_default_pandas_config.dict()
    print(f"  Dumped Dict ->\n\n{pf(dumped)}\n")

    datasource_without_default_pandas_data_asset_config_dict["fluent_datasources"][
        DEFAULT_PANDAS_DATASOURCE_NAME
    ]["assets"].pop(DEFAULT_PANDAS_DATA_ASSET_NAME)
    assert datasource_without_default_pandas_data_asset_config_dict == dumped

    re_loaded: GxConfig = GxConfig.parse_obj(dumped)
    pp(re_loaded)
    assert re_loaded

    assert from_dict_default_pandas_config == re_loaded

    # removing just the named asset results in nothing being serialized
    # since all we are left with is the default datasource and default data asset
    only_default_pandas_datasource_and_data_asset_config_dict = copy.deepcopy(
        DEFAULT_PANDAS_DATASOURCE_AND_DATA_ASSET_CONFIG_DICT
    )
    only_default_pandas_datasource_and_data_asset_config_dict["fluent_datasources"][
        DEFAULT_PANDAS_DATASOURCE_NAME
    ]["assets"].pop("my_csv_asset")

    from_dict_only_default_pandas_config = GxConfig.parse_obj(
        only_default_pandas_datasource_and_data_asset_config_dict
    )
    assert from_dict_only_default_pandas_config.fluent_datasources == {}


@pytest.fixture
def file_dc_config_dir_init(tmp_path: pathlib.Path) -> pathlib.Path:
    """
    Initialize an regular/old-style FileDataContext project config directory.
    Removed on teardown.
    """
    gx_yml = tmp_path / FileDataContext.GX_DIR / FileDataContext.GX_YML
    assert gx_yml.exists() is False
    FileDataContext.create(tmp_path)
    assert gx_yml.exists()

    tmp_gx_dir = gx_yml.parent.absolute()
    LOGGER.info(f"tmp_gx_dir -> {tmp_gx_dir}")
    return tmp_gx_dir


@pytest.fixture
def file_dc_config_file_with_substitutions(
    file_dc_config_dir_init: pathlib.Path,
) -> pathlib.Path:
    config_file = file_dc_config_dir_init / FileDataContext.GX_YML
    assert config_file.exists()
    with open(config_file, mode="a") as file_append:
        file_append.write(PG_CONFIG_YAML_STR)

    print(config_file.read_text())
    return config_file


@pytest.mark.integration
def test_config_substitution_retains_original_value_on_save(
    monkeypatch: pytest.MonkeyPatch,
    file_dc_config_file_with_substitutions: pathlib.Path,
    sqlite_database_path: pathlib.Path,
):
    original: dict = yaml.load(file_dc_config_file_with_substitutions.read_text())[
        "fluent_datasources"
    ]["my_sqlite_ds_w_subs"]

    from great_expectations import get_context

    context = get_context(
        context_root_dir=file_dc_config_file_with_substitutions.parent
    )

    print(context.fluent_config)

    # inject env variable
    my_conn_str = f"sqlite:///{sqlite_database_path}"
    monkeypatch.setenv("MY_CONN_STR", my_conn_str)

    ds_w_subs: SqliteDatasource = context.fluent_config.datasources[  # type: ignore[assignment]
        "my_sqlite_ds_w_subs"
    ]

    assert str(ds_w_subs.connection_string) == r"${MY_CONN_STR}"
    assert (
        ds_w_subs.connection_string.get_config_value(  # type: ignore[union-attr] # might not be ConfigStr
            context.config_provider
        )
        == my_conn_str
    )

    context._save_project_config()

    round_tripped = yaml.load(file_dc_config_file_with_substitutions.read_text())[
        "fluent_datasources"
    ]["my_sqlite_ds_w_subs"]

    # FIXME: serialized items should not have name
    round_tripped.pop("name")

    assert round_tripped == original


@pytest.mark.integration
def test_config_substitution_retains_original_value_on_save_w_run_time_mods(
    monkeypatch: pytest.MonkeyPatch,
    sqlite_database_path: pathlib.Path,
    file_dc_config_file_with_substitutions: pathlib.Path,
):
    # inject env variable
    my_conn_str = f"sqlite:///{sqlite_database_path}"
    monkeypatch.setenv("MY_CONN_STR", my_conn_str)

    original: dict = yaml.load(file_dc_config_file_with_substitutions.read_text())[
        "fluent_datasources"
    ]
    assert original.get("my_sqlite_ds_w_subs")  # will be modified
    assert original.get("my_pg_ds")  # will be deleted
    assert not original.get("my_sqlite")  # will be added

    from great_expectations import get_context

    context = get_context(
        context_root_dir=file_dc_config_file_with_substitutions.parent
    )

    datasources = context.fluent_datasources

    assert (
        str(datasources["my_sqlite_ds_w_subs"].connection_string)  # type: ignore[attr-defined]
        == r"${MY_CONN_STR}"
    )

    # add a new datasource
    context.sources.add_sqlite("my_new_one", connection_string="sqlite://")

    # add a new asset to an existing data
    sqlite_ds_w_subs: SqliteDatasource = context.get_datasource(  # type: ignore[assignment]
        "my_sqlite_ds_w_subs"
    )
    sqlite_ds_w_subs.add_table_asset(
        "new_asset", table_name="yellow_tripdata_sample_2019_01"
    )

    context._save_project_config()

    round_tripped_datasources = yaml.load(
        file_dc_config_file_with_substitutions.read_text()
    )["fluent_datasources"]

    assert round_tripped_datasources["my_new_one"]
    assert round_tripped_datasources["my_sqlite_ds_w_subs"]["assets"]["new_asset"]
