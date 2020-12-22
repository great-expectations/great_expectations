import os
import shutil
from typing import Dict

import pytest
from freezegun import freeze_time

from great_expectations import DataContext
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.data_context.store import ExpectationsStore, ValidationsStore
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.data_context.util import (
    file_relative_path,
    instantiate_class_from_config,
)
from great_expectations.render.renderer.site_builder import SiteBuilder


def assert_how_to_buttons(
    context,
    index_page_locator_info: str,
    index_links_dict: Dict,
    show_how_to_buttons=True,
):
    """Helper function to assert presence or non-presence of how-to buttons and related content in various
    Data Docs pages.
    """

    # these are simple checks for presence of certain page elements
    show_walkthrough_button = "Show Walkthrough"
    walkthrough_modal = "Great Expectations Walkthrough"
    cta_footer = (
        "To continue exploring Great Expectations check out one of these tutorials..."
    )
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

    data_docs_site_dir = os.path.join(
        context._context_root_directory,
        context._project_config.data_docs_sites["local_site"]["store_backend"][
            "base_directory"
        ],
    )

    page_paths_dict = {
        "index_pages": [index_page_locator_info[7:]],
        "expectation_suites": [
            os.path.join(data_docs_site_dir, link_dict["filepath"])
            for link_dict in index_links_dict.get("expectations_links", [])
        ],
        "validation_results": [
            os.path.join(data_docs_site_dir, link_dict["filepath"])
            for link_dict in index_links_dict.get("validations_links", [])
        ],
        "profiling_results": [
            os.path.join(data_docs_site_dir, link_dict["filepath"])
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


@freeze_time("09/26/2019 13:42:41")
@pytest.mark.rendered_output
def test_configuration_driven_site_builder(
    site_builder_data_context_with_html_store_titanic_random,
):
    context = site_builder_data_context_with_html_store_titanic_random

    context.add_validation_operator(
        "validate_and_store",
        {
            "class_name": "ActionListValidationOperator",
            "action_list": [
                {
                    "name": "store_validation_result",
                    "action": {
                        "class_name": "StoreValidationResultAction",
                        "target_store_name": "validations_store",
                    },
                },
                {
                    "name": "extract_and_store_eval_parameters",
                    "action": {
                        "class_name": "StoreEvaluationParametersAction",
                        "target_store_name": "evaluation_parameter_store",
                    },
                },
            ],
        },
    )

    # profiling the Titanic datasource will generate one expectation suite and one validation
    # that is a profiling result
    datasource_name = "titanic"
    data_asset_name = "Titanic"
    profiler_name = "BasicDatasetProfiler"
    generator_name = "subdir_reader"
    context.profile_datasource(datasource_name)

    # creating another validation result using the profiler's suite (no need to use a new expectation suite
    # for this test). having two validation results - one with run id "profiling" - allows us to test
    # the logic of run_name_filter that helps filtering validation results to be included in
    # the profiling and the validation sections.
    batch_kwargs = context.build_batch_kwargs(
        datasource=datasource_name,
        batch_kwargs_generator=generator_name,
        data_asset_name=data_asset_name,
    )

    expectation_suite_name = "{}.{}.{}.{}".format(
        datasource_name, generator_name, data_asset_name, profiler_name
    )

    batch = context.get_batch(
        batch_kwargs=batch_kwargs,
        expectation_suite_name=expectation_suite_name,
    )
    run_id = RunIdentifier(run_name="test_run_id_12345")
    context.run_validation_operator(
        assets_to_validate=[batch],
        run_id=run_id,
        validation_operator_name="validate_and_store",
    )

    data_docs_config = context._project_config.data_docs_sites
    local_site_config = data_docs_config["local_site"]

    validations_set = set(context.stores["validations_store"].list_keys())
    assert len(validations_set) == 6
    assert (
        ValidationResultIdentifier(
            expectation_suite_identifier=ExpectationSuiteIdentifier(
                expectation_suite_name=expectation_suite_name
            ),
            run_id="test_run_id_12345",
            batch_identifier=batch.batch_id,
        )
        in validations_set
    )
    assert (
        ValidationResultIdentifier(
            expectation_suite_identifier=ExpectationSuiteIdentifier(
                expectation_suite_name=expectation_suite_name
            ),
            run_id="profiling",
            batch_identifier=batch.batch_id,
        )
        in validations_set
    )
    assert (
        ValidationResultIdentifier(
            expectation_suite_identifier=ExpectationSuiteIdentifier(
                expectation_suite_name=expectation_suite_name
            ),
            run_id="profiling",
            batch_identifier=batch.batch_id,
        )
        in validations_set
    )
    assert (
        ValidationResultIdentifier(
            expectation_suite_identifier=ExpectationSuiteIdentifier(
                expectation_suite_name=expectation_suite_name
            ),
            run_id="profiling",
            batch_identifier=batch.batch_id,
        )
        in validations_set
    )

    site_builder = SiteBuilder(
        data_context=context,
        runtime_environment={"root_directory": context.root_directory},
        **local_site_config
    )
    res = site_builder.build()

    index_page_locator_info = res[0]
    index_links_dict = res[1]

    # assert that how-to buttons and related elements are rendered (default behavior)
    assert_how_to_buttons(context, index_page_locator_info, index_links_dict)
    # print(json.dumps(index_page_locator_info, indent=2))
    assert (
        index_page_locator_info
        == "file://"
        + context.root_directory
        + "/uncommitted/data_docs/local_site/index.html"
    )

    # print(json.dumps(index_links_dict, indent=2))

    assert "site_name" in index_links_dict

    assert "expectations_links" in index_links_dict
    assert len(index_links_dict["expectations_links"]) == 5

    assert "validations_links" in index_links_dict
    assert (
        len(index_links_dict["validations_links"]) == 1
    ), """
    The only rendered validation should be the one not generated by the profiler
    """

    assert "profiling_links" in index_links_dict
    assert len(index_links_dict["profiling_links"]) == 5

    # save documentation locally
    os.makedirs("./tests/render/output", exist_ok=True)
    os.makedirs("./tests/render/output/documentation", exist_ok=True)

    if os.path.isdir("./tests/render/output/documentation"):
        shutil.rmtree("./tests/render/output/documentation")
    shutil.copytree(
        os.path.join(
            site_builder_data_context_with_html_store_titanic_random.root_directory,
            "uncommitted/data_docs/",
        ),
        "./tests/render/output/documentation",
    )

    # let's create another validation result and run the site builder to add it
    # to the data docs
    # the operator does not have an StoreValidationResultAction action configured, so the site
    # will not be updated without our call to site builder

    expectation_suite_path_component = expectation_suite_name.replace(".", "/")
    validation_result_page_path = os.path.join(
        site_builder.site_index_builder.target_store.store_backends[
            ValidationResultIdentifier
        ].full_base_directory,
        "validations",
        expectation_suite_path_component,
        run_id.run_name,
        run_id.run_time.strftime("%Y%m%dT%H%M%S.%fZ"),
        batch.batch_id + ".html",
    )

    ts_last_mod_0 = os.path.getmtime(validation_result_page_path)

    run_id = RunIdentifier(run_name="test_run_id_12346")
    operator_result = context.run_validation_operator(
        assets_to_validate=[batch],
        run_id=run_id,
        validation_operator_name="validate_and_store",
    )

    validation_result_id = operator_result.list_validation_result_identifiers()[0]
    res = site_builder.build(resource_identifiers=[validation_result_id])

    index_links_dict = res[1]

    # verify that an additional validation result HTML file was generated
    assert len(index_links_dict["validations_links"]) == 2

    site_builder.site_index_builder.target_store.store_backends[
        ValidationResultIdentifier
    ].full_base_directory

    # verify that the validation result HTML file rendered in the previous run was NOT updated
    ts_last_mod_1 = os.path.getmtime(validation_result_page_path)

    assert ts_last_mod_0 == ts_last_mod_1

    # verify that the new method of the site builder that returns the URL of the HTML file that renders
    # a resource

    new_validation_result_page_path = os.path.join(
        site_builder.site_index_builder.target_store.store_backends[
            ValidationResultIdentifier
        ].full_base_directory,
        "validations",
        expectation_suite_path_component,
        run_id.run_name,
        run_id.run_time.strftime("%Y%m%dT%H%M%S.%fZ"),
        batch.batch_id + ".html",
    )

    html_url = site_builder.get_resource_url(resource_identifier=validation_result_id)
    assert "file://" + new_validation_result_page_path == html_url

    html_url = site_builder.get_resource_url()
    assert (
        "file://"
        + os.path.join(
            site_builder.site_index_builder.target_store.store_backends[
                ValidationResultIdentifier
            ].full_base_directory,
            "index.html",
        )
        == html_url
    )

    team_site_config = data_docs_config["team_site"]
    team_site_builder = SiteBuilder(
        data_context=context,
        runtime_environment={"root_directory": context.root_directory},
        **team_site_config
    )
    team_site_builder.clean_site()
    obs = [
        url_dict
        for url_dict in context.get_docs_sites_urls(site_name="team_site")
        if url_dict.get("site_url")
    ]
    assert len(obs) == 0

    # exercise clean_site
    site_builder.clean_site()
    obs = [
        url_dict
        for url_dict in context.get_docs_sites_urls()
        if url_dict.get("site_url")
    ]
    assert len(obs) == 0

    # restore site
    context = site_builder_data_context_with_html_store_titanic_random
    site_builder = SiteBuilder(
        data_context=context,
        runtime_environment={"root_directory": context.root_directory},
        **local_site_config
    )
    res = site_builder.build()


@freeze_time("09/26/2019 13:42:41")
@pytest.mark.rendered_output
def test_configuration_driven_site_builder_skip_and_clean_missing(
    site_builder_data_context_with_html_store_titanic_random,
):
    # tests auto-cleaning functionality of DefaultSiteIndexBuilder
    # when index page is built, if an HTML page is present without corresponding suite or validation result,
    # the HTML page should be removed and not appear on index page
    context = site_builder_data_context_with_html_store_titanic_random

    context.add_validation_operator(
        "validate_and_store",
        {
            "class_name": "ActionListValidationOperator",
            "action_list": [
                {
                    "name": "store_validation_result",
                    "action": {
                        "class_name": "StoreValidationResultAction",
                        "target_store_name": "validations_store",
                    },
                },
                {
                    "name": "extract_and_store_eval_parameters",
                    "action": {
                        "class_name": "StoreEvaluationParametersAction",
                        "target_store_name": "evaluation_parameter_store",
                    },
                },
            ],
        },
    )

    # profiling the Titanic datasource will generate one expectation suite and one validation
    # that is a profiling result
    datasource_name = "titanic"
    data_asset_name = "Titanic"
    profiler_name = "BasicDatasetProfiler"
    generator_name = "subdir_reader"
    context.profile_datasource(datasource_name)

    # creating another validation result using the profiler's suite (no need to use a new expectation suite
    # for this test). having two validation results - one with run id "profiling" - allows us to test
    # the logic of run_name_filter that helps filtering validation results to be included in
    # the profiling and the validation sections.
    batch_kwargs = context.build_batch_kwargs(
        datasource=datasource_name,
        batch_kwargs_generator=generator_name,
        data_asset_name=data_asset_name,
    )

    expectation_suite_name = "{}.{}.{}.{}".format(
        datasource_name, generator_name, data_asset_name, profiler_name
    )

    batch = context.get_batch(
        batch_kwargs=batch_kwargs,
        expectation_suite_name=expectation_suite_name,
    )
    run_id = RunIdentifier(run_name="test_run_id_12345")
    context.run_validation_operator(
        assets_to_validate=[batch],
        run_id=run_id,
        validation_operator_name="validate_and_store",
    )

    data_docs_config = context._project_config.data_docs_sites
    local_site_config = data_docs_config["local_site"]

    validations_set = set(context.stores["validations_store"].list_keys())
    assert len(validations_set) == 6

    expectation_suite_set = set(context.stores["expectations_store"].list_keys())
    assert len(expectation_suite_set) == 5

    site_builder = SiteBuilder(
        data_context=context,
        runtime_environment={"root_directory": context.root_directory},
        **local_site_config
    )
    site_builder.build()

    # test expectation suite pages
    expectation_suite_html_pages = {
        ExpectationSuiteIdentifier.from_tuple(suite_tuple)
        for suite_tuple in site_builder.target_store.store_backends[
            ExpectationSuiteIdentifier
        ].list_keys()
    }
    # suites in expectations store should match html pages
    assert expectation_suite_set == expectation_suite_html_pages

    # remove suites from expectations store
    for i in range(2):
        context.stores["expectations_store"].remove_key(list(expectation_suite_set)[i])

    # re-build data docs, which should remove suite HTML pages that no longer have corresponding suite in
    # expectations store
    site_builder.build()

    expectation_suite_set = set(context.stores["expectations_store"].list_keys())
    expectation_suite_html_pages = {
        ExpectationSuiteIdentifier.from_tuple(suite_tuple)
        for suite_tuple in site_builder.target_store.store_backends[
            ExpectationSuiteIdentifier
        ].list_keys()
    }
    assert expectation_suite_set == expectation_suite_html_pages

    # test validation result pages
    validation_html_pages = {
        ValidationResultIdentifier.from_tuple(result_tuple)
        for result_tuple in site_builder.target_store.store_backends[
            ValidationResultIdentifier
        ].list_keys()
    }
    # validations in store should match html pages
    assert validations_set == validation_html_pages

    # remove validations from store
    for i in range(2):
        context.stores["validations_store"].store_backend.remove_key(
            list(validations_set)[i]
        )

    # re-build data docs, which should remove validation HTML pages that no longer have corresponding validation in
    # validations store
    site_builder.build()

    validations_set = set(context.stores["validations_store"].list_keys())
    validation_html_pages = {
        ValidationResultIdentifier.from_tuple(result_tuple)
        for result_tuple in site_builder.target_store.store_backends[
            ValidationResultIdentifier
        ].list_keys()
    }
    assert validations_set == validation_html_pages


@pytest.mark.rendered_output
def test_configuration_driven_site_builder_without_how_to_buttons(
    site_builder_data_context_with_html_store_titanic_random,
):
    context = site_builder_data_context_with_html_store_titanic_random

    context.add_validation_operator(
        "validate_and_store",
        {
            "class_name": "ActionListValidationOperator",
            "action_list": [
                {
                    "name": "store_validation_result",
                    "action": {
                        "class_name": "StoreValidationResultAction",
                        "target_store_name": "validations_store",
                    },
                },
                {
                    "name": "extract_and_store_eval_parameters",
                    "action": {
                        "class_name": "StoreEvaluationParametersAction",
                        "target_store_name": "evaluation_parameter_store",
                    },
                },
            ],
        },
    )

    # profiling the Titanic datasource will generate one expectation suite and one validation
    # that is a profiling result
    datasource_name = "titanic"
    data_asset_name = "Titanic"
    profiler_name = "BasicDatasetProfiler"
    generator_name = "subdir_reader"
    context.profile_datasource(datasource_name)

    # creating another validation result using the profiler's suite (no need to use a new expectation suite
    # for this test). having two validation results - one with run id "profiling" - allows us to test
    # the logic of run_name_filter that helps filtering validation results to be included in
    # the profiling and the validation sections.
    batch_kwargs = context.build_batch_kwargs(
        datasource=datasource_name,
        batch_kwargs_generator=generator_name,
        name=data_asset_name,
    )

    expectation_suite_name = "{}.{}.{}.{}".format(
        datasource_name, generator_name, data_asset_name, profiler_name
    )

    batch = context.get_batch(
        batch_kwargs=batch_kwargs,
        expectation_suite_name=expectation_suite_name,
    )
    run_id = "test_run_id_12345"
    context.run_validation_operator(
        assets_to_validate=[batch],
        run_id=run_id,
        validation_operator_name="validate_and_store",
    )

    data_docs_config = context._project_config.data_docs_sites
    local_site_config = data_docs_config["local_site"]

    # set this flag to false in config to hide how-to buttons and related elements
    local_site_config["show_how_to_buttons"] = False

    site_builder = SiteBuilder(
        data_context=context,
        runtime_environment={"root_directory": context.root_directory},
        **local_site_config
    )
    res = site_builder.build()

    index_page_locator_info = res[0]
    index_links_dict = res[1]

    assert_how_to_buttons(
        context, index_page_locator_info, index_links_dict, show_how_to_buttons=False
    )


def test_site_builder_with_custom_site_section_builders_config(tmp_path_factory):
    """Test that site builder can handle partially specified custom site_section_builders config"""
    base_dir = str(tmp_path_factory.mktemp("project_dir"))
    project_dir = os.path.join(base_dir, "project_path")
    os.mkdir(project_dir)

    # fixture config swaps site section builder source stores and specifies custom run_name_filters
    shutil.copy(
        file_relative_path(
            __file__, "../test_fixtures/great_expectations_custom_local_site_config.yml"
        ),
        str(os.path.join(project_dir, "great_expectations.yml")),
    )
    context = DataContext(context_root_dir=project_dir)
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
    assert isinstance(expectations_site_section_builder.source_store, ValidationsStore)

    validations_site_section_builder = site_section_builders["validations"]
    assert isinstance(validations_site_section_builder.source_store, ExpectationsStore)
    assert validations_site_section_builder.run_name_filter == {
        "not_equals": "custom_validations_filter"
    }

    profiling_site_section_builder = site_section_builders["profiling"]
    assert isinstance(validations_site_section_builder.source_store, ExpectationsStore)
    assert profiling_site_section_builder.run_name_filter == {
        "equals": "custom_profiling_filter"
    }


@freeze_time("09/24/2019 23:18:36")
def test_site_builder_usage_statistics_enabled(
    site_builder_data_context_with_html_store_titanic_random,
):
    context = site_builder_data_context_with_html_store_titanic_random

    sites = (
        site_builder_data_context_with_html_store_titanic_random._project_config_with_variables_substituted.data_docs_sites
    )
    local_site_config = sites["local_site"]
    site_builder = instantiate_class_from_config(
        config=local_site_config,
        runtime_environment={
            "data_context": context,
            "root_directory": context.root_directory,
            "site_name": "local_site",
        },
        config_defaults={
            "module_name": "great_expectations.render.renderer.site_builder"
        },
    )
    site_builder_return_obj = site_builder.build()
    index_page_path = site_builder_return_obj[0]
    links_dict = site_builder_return_obj[1]
    expectation_suite_pages = [
        file_relative_path(index_page_path, expectation_suite_link_dict["filepath"])
        for expectation_suite_link_dict in links_dict["expectations_links"]
    ]
    profiling_results_pages = [
        file_relative_path(index_page_path, profiling_link_dict["filepath"])
        for profiling_link_dict in links_dict["profiling_links"]
    ]

    page_paths_to_check = (
        [index_page_path] + expectation_suite_pages + profiling_results_pages
    )

    expected_logo_url = "https://great-expectations-web-assets.s3.us-east-2.amazonaws.com/logo-long.png?d=20190924T231836.000000Z&dataContextId=f43d4897-385f-4366-82b0-1a8eda2bf79c"

    for page_path in page_paths_to_check:
        with open(page_path[7:]) as f:
            page_contents = f.read()
            assert expected_logo_url in page_contents


@freeze_time("09/24/2019 23:18:36")
def test_site_builder_usage_statistics_disabled(
    site_builder_data_context_with_html_store_titanic_random,
):
    context = site_builder_data_context_with_html_store_titanic_random
    context._project_config.anonymous_usage_statistics = {
        "enabled": False,
        "data_context_id": "f43d4897-385f-4366-82b0-1a8eda2bf79c",
    }
    data_context_id = context.anonymous_usage_statistics["data_context_id"]

    sites = (
        site_builder_data_context_with_html_store_titanic_random._project_config_with_variables_substituted.data_docs_sites
    )
    local_site_config = sites["local_site"]
    site_builder = instantiate_class_from_config(
        config=local_site_config,
        runtime_environment={
            "data_context": context,
            "root_directory": context.root_directory,
            "site_name": "local_site",
        },
        config_defaults={
            "module_name": "great_expectations.render.renderer.site_builder"
        },
    )
    site_builder_return_obj = site_builder.build()
    index_page_path = site_builder_return_obj[0]
    links_dict = site_builder_return_obj[1]
    expectation_suite_pages = [
        file_relative_path(index_page_path, expectation_suite_link_dict["filepath"])
        for expectation_suite_link_dict in links_dict["expectations_links"]
    ]
    profiling_results_pages = [
        file_relative_path(index_page_path, profiling_link_dict["filepath"])
        for profiling_link_dict in links_dict["profiling_links"]
    ]

    page_paths_to_check = (
        [index_page_path] + expectation_suite_pages + profiling_results_pages
    )

    expected_logo_url = "https://great-expectations-web-assets.s3.us-east-2.amazonaws.com/logo-long.png?d=20190924T231836.000000Z"

    for page_path in page_paths_to_check:
        with open(page_path[7:]) as f:
            page_contents = f.read()
            assert expected_logo_url in page_contents
            assert data_context_id not in page_contents
