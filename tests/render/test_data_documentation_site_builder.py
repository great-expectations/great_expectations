import pytest

import os
import json
import shutil

from great_expectations.render.renderer.site_builder import SiteBuilder
from great_expectations.data_context.types import (
    ValidationResultIdentifier, ExpectationSuiteIdentifier, DataAssetIdentifier
)

from great_expectations.data_context.util import safe_mmkdir


@pytest.mark.rendered_output
def test_configuration_driven_site_builder(site_builder_data_context_with_html_store_titanic_random):
    context = site_builder_data_context_with_html_store_titanic_random

    context.add_validation_operator(
        "validate_and_store",
        {
            "class_name": "ActionListValidationOperator",
            "action_list": [{
                "name": "store_validation_result",
                "action": {
                    "class_name": "StoreAction",
                    "target_store_name": "validations_store",
                }
            }, {
                "name": "extract_and_store_eval_parameters",
                "action": {
                    "class_name": "ExtractAndStoreEvaluationParamsAction",
                    "target_store_name": "evaluation_parameter_store",
                }
            }]
            }
    )

    # profiling the Titanic datasource will generate one expectation suite and one validation
    # that is a profiling result
    context.profile_datasource(context.list_datasources()[0]["name"])

    # creating another validation result using the profiler's suite (no need to use a new expectation suite
    # for this test). having two validation results - one with run id "profiling" - allows us to test
    # the logic of run_id_filter that helps filtering validation results to be included in
    # the profiling and the validation sections.
    batch = context.get_batch('Titanic', expectation_suite_name='BasicDatasetProfiler',
                              batch_kwargs=context.yield_batch_kwargs('Titanic'))
    run_id = "test_run_id_12345"
    context.run_validation_operator(
        assets_to_validate=[batch],
        run_id=run_id,
        validation_operator_name="validate_and_store",
    )

    data_docs_config = context._project_config.data_docs_sites
    local_site_config = data_docs_config['local_site']
    # local_site_config.pop('module_name')  # This isn't necessary
    local_site_config.pop('class_name')

    # set datasource_whitelist
    local_site_config['datasource_whitelist'] = ['titanic']

    validations_set = set(context.stores["validations_store"].list_keys())
    assert len(validations_set) == 4
    assert ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier(
            data_asset_name=DataAssetIdentifier("titanic", "default", "Titanic"),
            expectation_suite_name="BasicDatasetProfiler"
        ),
        run_id="test_run_id_12345"
    ) in validations_set
    assert ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier(
            data_asset_name=DataAssetIdentifier("titanic", "default", "Titanic"),
            expectation_suite_name="BasicDatasetProfiler"
        ),
        run_id="profiling"
    ) in validations_set
    assert ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier(
            data_asset_name=DataAssetIdentifier("random", "default", "f1"),
            expectation_suite_name="BasicDatasetProfiler"
        ),
        run_id="profiling"
    ) in validations_set
    assert ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier(
            data_asset_name=DataAssetIdentifier("random", "default", "f2"),
            expectation_suite_name="BasicDatasetProfiler"
        ),
        run_id="profiling"
    ) in validations_set

    site_builder = SiteBuilder(
            data_context=context,
            **local_site_config
        )
    res = site_builder.build()

    index_page_locator_info = res[0]
    index_links_dict = res[1]

    print(json.dumps(index_page_locator_info, indent=2))
    assert index_page_locator_info == context.root_directory + '/uncommitted/data_docs/local_site/index.html'

    print(json.dumps(index_links_dict, indent=2))
    assert json.loads(json.dumps(index_links_dict)) == json.loads("""\
{
  "titanic": {
    "default": {
      "Titanic": {
        "profiling_links": [
          {
            "full_data_asset_name": "titanic/default/Titanic",
            "expectation_suite_name": "BasicDatasetProfiler",
            "filepath": "validations/profiling/titanic/default/Titanic/BasicDatasetProfiler.html",
            "source": "titanic",
            "generator": "default",
            "asset": "Titanic",
            "run_id": "profiling",
            "validation_success": false
          }
        ],
        "validations_links": [
          {
            "full_data_asset_name": "titanic/default/Titanic",
            "expectation_suite_name": "BasicDatasetProfiler",
            "filepath": "validations/test_run_id_12345/titanic/default/Titanic/BasicDatasetProfiler.html",
            "source": "titanic",
            "generator": "default",
            "asset": "Titanic",
            "run_id": "test_run_id_12345",
            "validation_success": false
          }
        ],
        "expectations_links": [
          {
            "full_data_asset_name": "titanic/default/Titanic",
            "expectation_suite_name": "BasicDatasetProfiler",
            "filepath": "expectations/titanic/default/Titanic/BasicDatasetProfiler.html",
            "source": "titanic",
            "generator": "default",
            "asset": "Titanic",
            "run_id": null,
            "validation_success": null
          }
        ]
      }
    }
  }
}
    """)
    assert "random" not in index_links_dict, \
        """`random` must not appear in this documentation,
        because `datasource_whitelist` config option specifies only `titanic`"""

    assert len(index_links_dict['titanic']['default']['Titanic']['validations_links']) == 1, \
    """
    The only rendered validation should be the one not generated by the profiler
    """
    
    # save documentation locally
    safe_mmkdir("./tests/render/output")
    safe_mmkdir("./tests/render/output/documentation")

    if os.path.isdir("./tests/render/output/documentation"):
        shutil.rmtree("./tests/render/output/documentation")
    shutil.copytree(
        os.path.join(
            site_builder_data_context_with_html_store_titanic_random.root_directory,
            "uncommitted/data_docs/"
        ),
        "./tests/render/output/documentation"
    )

    # let's create another validation result and run the site builder to add it
    # to the data docs
    # the operator does not have an StoreAction action configured, so the site
    # will not be updated without our call to site builder

    ts_last_mod_0 = os.path.getmtime(os.path.join(site_builder.site_index_builder.target_store.store_backends[ValidationResultIdentifier].full_base_directory, "validations/test_run_id_12345/titanic/default/Titanic/BasicDatasetProfiler.html"))

    run_id = "test_run_id_12346"
    operator_result = context.run_validation_operator(
        assets_to_validate=[batch],
        run_id=run_id,
        validation_operator_name="validate_and_store",
    )

    validation_result_id = ValidationResultIdentifier(
        expectation_suite_identifier=[key for key in operator_result["details"].keys()][0],
        run_id=run_id)
    res = site_builder.build(resource_identifiers=[validation_result_id])

    index_links_dict = res[1]

    # verify that an additional validation result HTML file was generated
    assert len(index_links_dict["titanic"]["default"]["Titanic"]["validations_links"]) == 2

    site_builder.site_index_builder.target_store.store_backends[ValidationResultIdentifier].full_base_directory

    # verify that the validation result HTML file rendered in the previous run was NOT updated
    ts_last_mod_1 = os.path.getmtime(os.path.join(site_builder.site_index_builder.target_store.store_backends[ValidationResultIdentifier].full_base_directory, "validations/test_run_id_12345/titanic/default/Titanic/BasicDatasetProfiler.html"))

    assert ts_last_mod_0 == ts_last_mod_1

    print("mmm")

