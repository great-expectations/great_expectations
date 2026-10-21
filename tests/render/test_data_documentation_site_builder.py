import logging
import os
import re
import shutil

import pytest

from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
)
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.data_context import get_context
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.store import ExpectationsStore, ValidationResultsStore
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.data_context.util import (
    file_relative_path,
    instantiate_class_from_config,
)

# module level markers
pytestmark = pytest.mark.filesystem


def test_site_builder_with_custom_site_section_builders_config(tmp_path_factory):
    """Test that site builder can handle partially specified custom site_section_builders config"""
    base_dir = str(tmp_path_factory.mktemp("project_dir"))
    project_dir = os.path.join(base_dir, "project_path")  # noqa: PTH118 # FIXME CoP
    os.mkdir(project_dir)  # noqa: PTH102 # FIXME CoP

    # fixture config swaps site section builder source stores and specifies custom run_name_filters
    shutil.copy(
        file_relative_path(
            __file__, "../test_fixtures/great_expectations_custom_local_site_config.yml"
        ),
        str(os.path.join(project_dir, FileDataContext.GX_YML)),  # noqa: PTH118 # FIXME CoP
    )
    context = get_context(context_root_dir=project_dir)
    local_site_config = context._project_config.data_docs_sites.get("local_site")

    module_name = "great_expectations.render.renderer.site_builder"
    site_builder = instantiate_class_from_config(
        config=local_site_config,
        runtime_environment={
            "data_context": context,
            "root_directory": context.root_directory,
            "site_name": "local_site",
        },
        config_defaults={"module_name": module_name},
    )
    site_section_builders = site_builder.site_section_builders

    expectations_site_section_builder = site_section_builders["expectations"]
    assert isinstance(expectations_site_section_builder.source_store, ValidationResultsStore)

    validations_site_section_builder = site_section_builders["validations"]
    assert isinstance(validations_site_section_builder.source_store, ExpectationsStore)
    assert validations_site_section_builder.run_name_filter == {
        "not_equals": "custom_validations_filter"
    }

    profiling_site_section_builder = site_section_builders["profiling"]
    assert isinstance(validations_site_section_builder.source_store, ExpectationsStore)
    assert profiling_site_section_builder.run_name_filter == {"equals": "custom_profiling_filter"}


def _validation_result(suite_name: str, meta: dict) -> ExpectationSuiteValidationResult:
    return ExpectationSuiteValidationResult(
        success=True,
        results=[],
        suite_name=suite_name,
        statistics={
            "evaluated_expectations": 0,
            "successful_expectations": 0,
            "unsuccessful_expectations": 0,
            "success_percent": 100.0,
        },
        meta={"expectation_suite_name": suite_name, **meta},
    )


def test_site_builder_renders_a_page_per_validation_result(tmp_path):
    """Every persisted validation result gets a page, including one whose meta has no run_id."""
    suite_name = "my_suite"
    context = get_context(mode="file", project_root_dir=str(tmp_path))
    context.suites.add(ExpectationSuite(name=suite_name))

    results = {
        "with_run_id": {"run_id": {"run_name": "with_run_id", "run_time": "2026-01-01T00:00:00Z"}},
        "without_run_id": {},
    }
    for run_name, meta in results.items():
        context.validation_results_store.add(
            ValidationResultIdentifier(
                expectation_suite_identifier=ExpectationSuiteIdentifier(name=suite_name),
                run_id=RunIdentifier(run_name=run_name, run_time="2026-01-01T00:00:00Z"),
                batch_identifier="my_batch",
            ),
            _validation_result(suite_name, meta),
        )

    context.build_data_docs()

    pages = list(tmp_path.glob("**/data_docs/local_site/validations/**/*.html"))
    assert len(pages) == len(results)


@pytest.mark.parametrize("flag", ["cloud_mode", "ge_cloud_mode"])
def test_site_builder_refuses_cloud_mode(flag):
    from great_expectations.data_context.data_context.cloud_data_context import SHUTDOWN_MESSAGE
    from great_expectations.exceptions import GreatExpectationsError
    from great_expectations.render.renderer.site_builder import SiteBuilder

    with pytest.raises(GreatExpectationsError, match=re.escape(SHUTDOWN_MESSAGE)):
        SiteBuilder(data_context=None, store_backend={}, **{flag: True})


def test_site_index_skips_a_validation_result_the_context_cannot_find(
    tmp_path, monkeypatch, caplog
):
    suite_name = "my_suite"
    context = get_context(mode="file", project_root_dir=str(tmp_path))
    context.suites.add(ExpectationSuite(name=suite_name))
    key = ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier(name=suite_name),
        run_id=RunIdentifier(run_name="a_run", run_time="2026-01-01T00:00:00Z"),
        batch_identifier="my_batch",
    )
    context.validation_results_store.add(key, _validation_result(suite_name, {}))
    # get_validation_result() returns {} when no result matches the key
    monkeypatch.setattr(context, "get_validation_result", lambda **kwargs: {})

    with caplog.at_level(logging.WARNING):
        context.build_data_docs()

    assert f"Validation result not found: {key.to_tuple()!s:s} - skipping" in caplog.messages
