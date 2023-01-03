import functools
import logging
import pathlib
from pprint import pformat as pf

import pytest

# apply markers to entire test module
pytestmark = [pytest.mark.integration]

from great_expectations import get_context
from great_expectations.data_context import FileDataContext
from great_expectations.experimental.datasources.config import GxConfig

LOGGER = logging.getLogger(__file__)


@pytest.fixture
def db_file() -> pathlib.Path:
    db = pathlib.Path(
        __file__,
        "..",
        "..",
        "..",
        "test_sets",
        "taxi_yellow_tripdata_samples",
        "sqlite",
        "yellow_tripdata.db",
    ).resolve()
    assert db.exists()
    return db


@pytest.fixture
def zep_config_dict(db_file) -> dict:
    return {
        "xdatasources": {
            "my_sql_ds": {
                "connection_string": f"sqlite:///{db_file}",
                "name": "my_sql_ds",
                "type": "sql",
                "assets": {
                    "my_asset": {
                        "name": "my_asset",
                        "table_name": "yellow_tripdata_sample_2019_01",
                        "type": "table",
                        "column_splitter": {
                            "column_name": "pickup_datetime",
                            "method_name": "split_on_year_and_month",
                            "name": "y_m_splitter",
                            "param_names": ["year", "month"],
                        },
                        "order_by": [
                            {"metadata_key": "year"},
                            {"metadata_key": "month"},
                        ],
                    },
                },
            }
        }
    }


@pytest.fixture
def zep_only_config(zep_config_dict: dict) -> GxConfig:
    """Creates a ZEP `GxConfig` object and ensures it contains at least one `Datasource`"""
    zep_config = GxConfig.parse_obj(zep_config_dict)
    assert zep_config.datasources
    return zep_config


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
def zep_yaml_config_file(
    file_dc_config_dir_init: pathlib.Path, zep_only_config: GxConfig
) -> pathlib.Path:
    """
    Dump the provided GxConfig to a temporary path. File is removed during test teardown.

    Append ZEP config to default config file
    """
    config_file_path = file_dc_config_dir_init / FileDataContext.GX_YML

    assert config_file_path.exists() is True

    with open(config_file_path, mode="a") as f_append:
        yaml_string = "\n# ZEP\n" + zep_only_config.yaml()
        f_append.write(yaml_string)

    for ds_name in zep_only_config.datasources.keys():
        assert ds_name in yaml_string

    print(f"  Config File Text\n-----------\n{config_file_path.read_text()}")
    return config_file_path


def test_load_an_existing_config(
    zep_yaml_config_file: pathlib.Path, zep_only_config: GxConfig
):
    context = get_context(
        context_root_dir=zep_yaml_config_file.parent, cloud_mode=False
    )

    assert context.zep_config == zep_only_config


@pytest.fixture
@functools.lru_cache(maxsize=1)
def zep_file_context(zep_yaml_config_file: pathlib.Path) -> FileDataContext:
    context = get_context(
        context_root_dir=zep_yaml_config_file.parent, cloud_mode=False
    )
    assert isinstance(context, FileDataContext)
    return context


def test_serialize_zep_config(zep_file_context: FileDataContext):
    dumped_yaml: str = zep_file_context.zep_config.yaml()
    print(f"  Dumped Config\n\n{dumped_yaml}\n")

    assert zep_file_context.zep_config.datasources

    for ds_name, datasource in zep_file_context.zep_config.datasources.items():
        assert ds_name in dumped_yaml

        for asset_name in datasource.assets.keys():
            assert asset_name in dumped_yaml


def test_zep_simple_validate_workflow(zep_file_context: FileDataContext):
    batch_request = (
        zep_file_context.get_datasource("my_sql_ds")
        .get_asset("my_asset")
        .get_batch_request({"year": 2019, "month": 1})
    )

    validator = zep_file_context.get_validator(batch_request=batch_request)
    result = validator.expect_column_max_to_be_between(
        column="passenger_count", min_value=1, max_value=12
    )
    print(f"  results ->\n{pf(result)}")
    assert result["success"] is True


@pytest.mark.xfail(reason="TypeError: cannot pickle 'module' object")
def test_save_datacontext(zep_file_context: FileDataContext):
    zep_file_context.variables.save_config()


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
