import os
import shutil
from typing import Dict

import pytest

from great_expectations.data_context import get_context
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.store import ExpectationsStore, ValidationResultsStore
from great_expectations.data_context.util import (
    file_relative_path,
    instantiate_class_from_config,
)

# module level markers
pytestmark = pytest.mark.filesystem


def assert_how_to_buttons(
    context,
    index_page_locator_info: str,
    index_links_dict: Dict,
    show_how_to_buttons=True,
):
    """Helper function to assert presence or non-presence of how-to buttons and related content in various
    Data Docs pages.
    """  # noqa: E501

    # these are simple checks for presence of certain page elements
    show_walkthrough_button = "Show Walkthrough"
    walkthrough_modal = "Great Expectations Walkthrough"
    cta_footer = "To continue exploring Great Expectations check out one of these tutorials..."
    how_to_edit_suite_button = "How to Edit This Suite"
    how_to_edit_suite_modal = "How to Edit This Expectation Suite"
    action_card = "Actions"

    how_to_page_elements_dict = {
        "index_pages": [show_walkthrough_button, walkthrough_modal, cta_footer],
        "expectation_suites": [
            how_to_edit_suite_button,
            how_to_edit_suite_modal,
            show_walkthrough_button,
            walkthrough_modal,
        ],
        "validation_results": [
            how_to_edit_suite_button,
            how_to_edit_suite_modal,
            show_walkthrough_button,
            walkthrough_modal,
        ],
        "profiling_results": [action_card, show_walkthrough_button, walkthrough_modal],
    }

    data_docs_site_dir = os.path.join(  # noqa: PTH118
        context._context_root_directory,
        context._project_config.data_docs_sites["local_site"]["store_backend"]["base_directory"],
    )

    page_paths_dict = {
        "index_pages": [index_page_locator_info[7:]],
        "expectation_suites": [
            os.path.join(data_docs_site_dir, link_dict["filepath"])  # noqa: PTH118
            for link_dict in index_links_dict.get("expectations_links", [])
        ],
        "validation_results": [
            os.path.join(data_docs_site_dir, link_dict["filepath"])  # noqa: PTH118
            for link_dict in index_links_dict.get("validations_links", [])
        ],
        "profiling_results": [
            os.path.join(data_docs_site_dir, link_dict["filepath"])  # noqa: PTH118
            for link_dict in index_links_dict.get("profiling_links", [])
        ],
    }

    for page_type, page_paths in page_paths_dict.items():
        for page_path in page_paths:
            with open(page_path) as f:
                page = f.read()
                for how_to_element in how_to_page_elements_dict[page_type]:
                    if show_how_to_buttons:
                        assert how_to_element in page
                    else:
                        assert how_to_element not in page


def test_site_builder_with_custom_site_section_builders_config(tmp_path_factory):
    """Test that site builder can handle partially specified custom site_section_builders config"""
    base_dir = str(tmp_path_factory.mktemp("project_dir"))
    project_dir = os.path.join(base_dir, "project_path")  # noqa: PTH118
    os.mkdir(project_dir)  # noqa: PTH102

    # fixture config swaps site section builder source stores and specifies custom run_name_filters
    shutil.copy(
        file_relative_path(
            __file__, "../test_fixtures/great_expectations_custom_local_site_config.yml"
        ),
        str(os.path.join(project_dir, FileDataContext.GX_YML)),  # noqa: PTH118
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
