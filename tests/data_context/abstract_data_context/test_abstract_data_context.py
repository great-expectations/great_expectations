import pytest


@pytest.mark.parametrize(
    "context_fixture_name",
    [
        pytest.param("ephemeral_context_with_defaults", marks=[pytest.mark.unit]),
        pytest.param("empty_data_context", marks=[pytest.mark.filesystem]),
        pytest.param("empty_cloud_context_fluent", marks=[pytest.mark.cloud]),
    ],
)
def test_fluent_datasources_show_when_printed(
    context_fixture_name: str,
    request,
) -> None:
    """Verify that fluent datasources show up when printed.
    This ends up testing both add and update (since adding assets and batch defs
    are implemented as updates on the datasource)
    """
    context = request.getfixturevalue(context_fixture_name)

    ds_name = "my-datasource"
    asset_name = "my-asset"
    batch_definition_name = "my-batch_definition"
    (
        context.data_sources.add_pandas(name=ds_name)
        .add_dataframe_asset(name=asset_name)
        .add_batch_definition_whole_dataframe(name=batch_definition_name)
    )

    str_output = context.__str__()
    repr_output = context.__repr__()

    for output in (str_output, repr_output):
        assert ds_name in output
        assert asset_name in output
        assert batch_definition_name in output


@pytest.mark.parametrize(
    "context_fixture_name",
    [
        pytest.param("ephemeral_context_with_defaults", marks=[pytest.mark.unit]),
        pytest.param("empty_data_context", marks=[pytest.mark.filesystem]),
        pytest.param("empty_cloud_context_fluent", marks=[pytest.mark.cloud]),
    ],
)
def test_deleted_fluent_datasources_do_not_show_when_printed(
    context_fixture_name: str,
    request,
) -> None:
    context = request.getfixturevalue(context_fixture_name)

    ds_name = "my-datasource"
    asset_name = "my-asset"
    batch_definition_name = "my-batch_definition"
    (
        context.data_sources.add_pandas(name=ds_name)
        .add_dataframe_asset(name=asset_name)
        .add_batch_definition_whole_dataframe(name=batch_definition_name)
    )

    context.delete_datasource(ds_name)

    str_output = context.__str__()
    repr_output = context.__repr__()

    for output in (str_output, repr_output):
        assert ds_name not in output
        assert asset_name not in output
        assert batch_definition_name not in output
