import functools
import pathlib

import pytest
from pytest import TempPathFactory

# apply markers to entire test module
pytestmark = [pytest.mark.integration]

from great_expectations import get_context
from great_expectations.data_context import FileDataContext
from great_expectations.experimental.datasources.config import GxConfig


@pytest.fixture
def zep_config_dict() -> dict:
    return {
        "datasources": {
            "my_sql_ds": {
                "connection_string": "sqlite:///:memory:",
                "name": "my_sql_ds",
                "type": "sql",
                "assets": {
                    "my_table_asset": {
                        "name": "my_table_asset",
                        "table_name": "my_table",
                        "type": "table",
                        "column_splitter": {
                            "column_name": "my_column",
                            "method_name": "split_on_year_and_month",
                            "name": "y_m_splitter",
                            "param_names": ["year", "month"],
                        },
                        "order_by": [
                            {"metadata_key": "year"},
                            {"metadata_key": "month", "reverse": True},
                        ],
                    },
                },
            }
        }
    }


@pytest.fixture
def zep_only_config(zep_config_dict: dict) -> GxConfig:
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
    return gx_yml.parent


@pytest.fixture
def zep_yaml_config_file(
    file_dc_config_dir_init: pathlib.Path, zep_only_config: GxConfig
) -> pathlib.Path:
    """
    Dump the provided GxConfig to a temporary path. File is removed during test teardown.
    """
    config_file_path = file_dc_config_dir_init / "zep_config.yml"
    # config_file_path.parent.mkdir()

    assert config_file_path.exists() is False

    zep_only_config.yaml(config_file_path)
    assert config_file_path.exists() is True
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


def test_zep_simple_validate(zep_file_context: FileDataContext):
    # see `test_run_checkpoint_and_data_doc`
    # sans data_doc
    pass


def test_save_datacontext(zep_file_context: FileDataContext):
    zep_file_context.variables.save_config()


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
