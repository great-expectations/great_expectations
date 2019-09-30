import os
import pytest
import json
import re
import shutil

from great_expectations.util import (
    gen_directory_tree_str
)
from great_expectations.data_context.util import (
    parse_string_to_data_context_resource_identifier
)
from great_expectations.render.renderer.site_builder import (
    SiteBuilder,
    DefaultSiteSectionBuilder,
    DefaultSiteIndexBuilder,
)
from great_expectations.data_context.types import (
    ValidationResultIdentifier,
    SiteSectionIdentifier,
)
from great_expectations.data_context.util import safe_mmkdir


def test_configuration_driven_site_builder(site_builder_data_context_with_html_store_titanic_random):
    context = site_builder_data_context_with_html_store_titanic_random

    context.add_validation_operator(
        "validate_and_store",
        {
        "class_name": "PerformActionListValidationOperator",
        "action_list": [{
            "name": "store_validation_result",
            "action": {
                "class_name": "StoreAction",
                "target_store_name": "local_validation_result_store",
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
        run_identifier=run_id,
        validation_operator_name="validate_and_store",
    )

    data_docs_config = context._project_config.get('data_docs')
    local_site_config = data_docs_config['sites']['local_site']
    local_site_config.pop('module_name')
    local_site_config.pop('class_name')

    # set datasource_whitelist
    local_site_config['datasource_whitelist'] = ['titanic']

    keys_as_strings = [x.to_string() for x in context.stores["local_validation_result_store"].list_keys()]
    print("\n".join(keys_as_strings))
    assert set(keys_as_strings) == set([
        "ValidationResultIdentifier.titanic.default.Titanic.BasicDatasetProfiler.test_run_id_12345",
        "ValidationResultIdentifier.titanic.default.Titanic.BasicDatasetProfiler.profiling",
        "ValidationResultIdentifier.random.default.f2.BasicDatasetProfiler.profiling",
        "ValidationResultIdentifier.random.default.f1.BasicDatasetProfiler.profiling",
    ])

    res = SiteBuilder(
            data_context=context,
            **local_site_config
            # datasource_whitelist=[],
            # datasource_blacklist=[],
        ).build()
    index_page_locator_info = res[0]
    index_links_dict = res[1]

    print( json.dumps(index_page_locator_info, indent=2) )
    assert index_page_locator_info == context.root_directory + '/uncommitted/documentation/local_site/index.html'

    print( json.dumps(index_links_dict, indent=2) )
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
            "uncommitted/documentation/"
        ),
        "./tests/render/output/documentation"
    )
    

def test_SiteSectionBuilder(site_builder_data_context_with_html_store_titanic_random):
    context = site_builder_data_context_with_html_store_titanic_random

    my_site_section_builders = DefaultSiteSectionBuilder(
        "test_name",
        context,
        source_store_name="expectations_store",
        target_store_name="local_site_html_store",
        renderer={
            "module_name": "great_expectations.render.renderer",
            "class_name": "ExpectationSuitePageRenderer",
        },
    )

    datasource_whitelist = ['titanic']
    my_site_section_builders.build(datasource_whitelist)

    print(context.stores["local_site_html_store"].list_keys())
    assert len(context.stores["local_site_html_store"].list_keys()) == 1

    first_key = context.stores["local_site_html_store"].list_keys()[0]
    print(first_key)
    print(type(first_key.resource_identifier))
    assert first_key == SiteSectionIdentifier(
        site_section_name="test_name",
        resource_identifier=parse_string_to_data_context_resource_identifier(
            "ExpectationSuiteIdentifier.titanic.default.Titanic.BasicDatasetProfiler"
        ),
    )

    content = context.stores["local_site_html_store"].get(first_key)
    assert len(re.findall("<div.*>", content)) > 20  # Ah! Then it MUST be HTML!
    with open('./tests/render/output/test_SiteSectionBuilder.html', 'w') as f:
        f.write(content)