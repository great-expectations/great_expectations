from typing import Type

import pytest

from great_expectations.data_context.data_context.abstract_data_context import AbstractDataContext
from great_expectations.data_context.data_context.cloud_data_context import CloudDataContext
from great_expectations.data_context.data_context.ephemeral_data_context import EphemeralDataContext
from great_expectations.data_context.data_context.file_data_context import FileDataContext


@pytest.mark.parametrize(
    ("context_fixture_name", "concrete_class"),
    [
        pytest.param(
            "ephemeral_context_with_defaults",
            EphemeralDataContext,
            marks=[pytest.mark.unit],
        ),
        pytest.param(
            "empty_data_context",
            FileDataContext,
            marks=[pytest.mark.filesystem],
        ),
        pytest.param(
            "empty_cloud_context_fluent",
            CloudDataContext,
            marks=[pytest.mark.cloud],
        ),
    ],
)
@pytest.mark.unit
def test_fluent_datasources_show_when_printed(
    context_fixture_name: str,
    concrete_class: Type[AbstractDataContext],
    request,
) -> None:
    """Verify that fluent datasources show up when printed.
    This ends up testing both add and update (since adding assets and batch defs
    are implemented as updates on the datasource)
    """
    # just double check that the context is what we think it is :)
    context = request.getfixturevalue(context_fixture_name)
    assert isinstance(context, concrete_class)

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
    ("context_fixture_name", "concrete_class"),
    [
        pytest.param(
            "ephemeral_context_with_defaults",
            EphemeralDataContext,
            marks=[pytest.mark.unit],
        ),
        pytest.param(
            "empty_data_context",
            FileDataContext,
            marks=[pytest.mark.filesystem],
        ),
        pytest.param(
            "empty_cloud_context_fluent",
            CloudDataContext,
            marks=[pytest.mark.cloud],
        ),
    ],
)
@pytest.mark.unit
def test_deleted_fluent_datasources_do_not_show_when_printed(
    context_fixture_name: str,
    concrete_class: Type[AbstractDataContext],
    request,
) -> None:
    # just double check that the context is what we think it is :)
    context = request.getfixturevalue(context_fixture_name)
    assert isinstance(context, concrete_class)

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
