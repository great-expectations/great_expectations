from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING

import pytest

import great_expectations as gx
from great_expectations.checkpoint.checkpoint import Checkpoint, CheckpointResult
from great_expectations.core.expectation_validation_result import ExpectationSuiteValidationResult
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.types.resource_identifiers import ValidationResultIdentifier
from great_expectations.data_context.util import file_relative_path
from great_expectations.self_check.util import expectationSuiteValidationResultSchema

if TYPE_CHECKING:
    import pytest_mock

    from great_expectations.core.expectation_validation_result import (
        ExpectationSuiteValidationResult,
    )


@pytest.fixture(scope="module")
def empty_data_context_module_scoped(tmp_path_factory) -> FileDataContext:
    # Re-enable GE_USAGE_STATS
    project_path = str(tmp_path_factory.mktemp("empty_data_context"))
    context = gx.data_context.FileDataContext.create(project_path)
    context_path = os.path.join(project_path, FileDataContext.GX_DIR)  # noqa: PTH118
    asset_config_path = os.path.join(context_path, "expectations")  # noqa: PTH118
    os.makedirs(asset_config_path, exist_ok=True)  # noqa: PTH103
    return context


@pytest.fixture
def titanic_profiled_name_column_evrs() -> ExpectationSuiteValidationResult:
    # This is a janky way to fetch expectations matching a specific name from an EVR suite.
    # TODO: It will no longer be necessary once we implement ValidationResultSuite._group_evrs_by_column  # noqa: E501
    from great_expectations.render.renderer.renderer import Renderer

    with open(
        file_relative_path(__file__, "./fixtures/BasicDatasetProfiler_evrs.json"),
    ) as infile:
        titanic_profiled_evrs_1 = expectationSuiteValidationResultSchema.load(json.load(infile))

    evrs_by_column = Renderer()._group_evrs_by_column(titanic_profiled_evrs_1)
    name_column_evrs = evrs_by_column["Name"]

    return name_column_evrs


@pytest.fixture
def v1_checkpoint_result(mocker: pytest_mock.MockFixture):
    result_a = mocker.MagicMock(
        spec=ExpectationSuiteValidationResult,
        suite_name="my_bad_suite",
        meta={},
        statistics={"successful_expectations": 3, "evaluated_expectations": 5},
        batch_id="my_batch",
        success=False,
    )
    result_a.asset_name = "my_first_asset"
    result_b = mocker.MagicMock(
        spec=ExpectationSuiteValidationResult,
        suite_name="my_good_suite",
        meta={"run_id": "my_run_id"},
        statistics={"successful_expectations": 1, "evaluated_expectations": 1},
        batch_id="my_other_batch",
        success=True,
    )
    result_b.asset_name = None

    return CheckpointResult(
        run_id=RunIdentifier(run_name="my_run_id"),
        run_results={
            mocker.MagicMock(spec=ValidationResultIdentifier): result_a,
            mocker.MagicMock(spec=ValidationResultIdentifier): result_b,
        },
        checkpoint_config=mocker.MagicMock(spec=Checkpoint, name="my_checkpoint"),
        success=False,
    )
