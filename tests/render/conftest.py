import os

import pytest

import great_expectations as ge


@pytest.fixture(scope="module")
def empty_data_context_module_scoped(tmp_path_factory):
    # Re-enable GE_USAGE_STATS
    project_path = str(tmp_path_factory.mktemp("empty_data_context"))
    context = ge.data_context.DataContext.create(project_path)
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")
    os.makedirs(asset_config_path, exist_ok=True)
    return context
