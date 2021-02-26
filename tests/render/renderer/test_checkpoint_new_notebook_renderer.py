import pytest

from great_expectations.data_context import BaseDataContext
from great_expectations.render.renderer.checkpoint_new_notebook_renderer import (
    CheckpointNewNotebookRenderer,
)


@pytest.fixture
def assetless_dataconnector_context(
    titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates,
):
    context = titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates
    root_directory = context.root_directory
    assert len(context.list_datasources()) == 1

    # mangle the datasource in a specific way
    config = context.get_config_with_variables_substituted()
    config.datasources["my_datasource"]["data_connectors"].pop(
        "my_basic_data_connector"
    )
    config.datasources["my_datasource"]["data_connectors"].pop(
        "my_special_data_connector"
    )
    config.datasources["my_datasource"]["data_connectors"].pop(
        "my_other_data_connector"
    )
    return BaseDataContext(project_config=config, context_root_dir=root_directory)


def test_find_datasource_with_asset_on_context_with_no_datasources(
    empty_data_context,
):
    context = empty_data_context
    assert len(context.list_datasources()) == 0

    renderer = CheckpointNewNotebookRenderer(context, "foo")
    obs = renderer._find_datasource_with_asset()
    assert obs is None


def test_find_datasource_with_asset_on_context_with_a_datasource_with_no_dataconnectors(
    titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates,
):
    context = titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates
    context.delete_datasource("my_datasource")
    assert len(context.list_datasources()) == 0
    context.add_datasource("aaa_datasource", class_name="PandasDatasource")
    assert len(context.list_datasources()) == 1

    renderer = CheckpointNewNotebookRenderer(context, "foo")
    obs = renderer._find_datasource_with_asset()
    assert obs is None


def test_find_datasource_with_asset_on_context_with_a_datasource_with_a_dataconnector_that_has_no_assets(
    assetless_dataconnector_context,
):
    context = assetless_dataconnector_context
    assert list(context.get_datasource("my_datasource").data_connectors.keys()) == [
        "my_runtime_data_connector"
    ]

    renderer = CheckpointNewNotebookRenderer(context, "foo")
    obs = renderer._find_datasource_with_asset()
    assert obs is None


def test_find_datasource_with_asset_on_happy_path_context(
    titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates,
):
    context = titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates
    assert len(context.list_datasources()) == 1

    renderer = CheckpointNewNotebookRenderer(context, "foo")
    obs = renderer._find_datasource_with_asset()
    assert obs == {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_special_data_connector",
        "asset_name": "users",
    }


def test_find_datasource_with_asset_on_context_with_a_full_datasource_and_one_with_no_dataconnectors(
    titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates,
):
    context = titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates
    assert len(context.list_datasources()) == 1
    context.add_datasource("aaa_datasource", class_name="PandasDatasource")
    assert len(context.list_datasources()) == 2

    renderer = CheckpointNewNotebookRenderer(context, "foo")
    obs = renderer._find_datasource_with_asset()
    assert obs == {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_special_data_connector",
        "asset_name": "users",
    }
