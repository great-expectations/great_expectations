import json
import os

import pytest

import great_expectations as ge
from great_expectations.data_context.util import file_relative_path
from great_expectations.self_check.util import expectationSuiteValidationResultSchema


@pytest.fixture(scope="module")
def empty_data_context_module_scoped(tmp_path_factory):
    # Re-enable GE_USAGE_STATS
    project_path = str(tmp_path_factory.mktemp("empty_data_context"))
    context = ge.data_context.DataContext.create(project_path)
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")
    os.makedirs(asset_config_path, exist_ok=True)
    return context


@pytest.fixture
def titanic_profiled_name_column_evrs():
    # This is a janky way to fetch expectations matching a specific name from an EVR suite.
    # TODO: It will no longer be necessary once we implement ValidationResultSuite._group_evrs_by_column
    from great_expectations.render.renderer.renderer import Renderer

    with open(
        file_relative_path(__file__, "./fixtures/BasicDatasetProfiler_evrs.json"),
    ) as infile:
        titanic_profiled_evrs_1 = expectationSuiteValidationResultSchema.load(
            json.load(infile)
        )

    evrs_by_column = Renderer()._group_evrs_by_column(titanic_profiled_evrs_1)
    name_column_evrs = evrs_by_column["Name"]

    return name_column_evrs
