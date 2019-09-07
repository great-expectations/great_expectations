import os
import pytest
import json

from great_expectations.render.renderer.site_builder import SiteBuilder
from great_expectations.data_context.types import (
    ValidationResultIdentifier
)

def test_configuration_driven_site_builder(titanic_data_context, filesystem_csv_2):

    # profiling the Titanic datasource will generate one expectation suite and one validation
    # that is a profiling result
    titanic_data_context.profile_datasource(titanic_data_context.list_datasources()[0]["name"])

    # adding another datasource to the context, but will configure the site to not document it -
    # this will allow us to test that the the datasource logic in the site builder is working properly.
    titanic_data_context.add_datasource("unused_datasource", "pandas", base_directory=str(filesystem_csv_2))

    # creating another validation result using the profiler's suite (no need to use a new expectation suite
    # for this test). having two validation results - one with run id "profiling" - allows us to test
    # the logic of run_id_filter that helps filtering validation results to be included in
    # the profiling and the validation sections.
    batch = titanic_data_context.get_batch('Titanic', expectation_suite_name='BasicDatasetProfiler')
    run_id = "test_run_id_12345"
    batch.validate(run_id=run_id)

    # creating a configuration for the site. this test's purpose is to assert that, given a correct
    # configuration, the site builder will produce correct documentation.
    site_config = {
        "site_store": {
            "type": "filesystem",
            "base_directory": "uncommitted/documentation/local_site"
        },
        "validations_store": {
            "name": "local_validation_result_store",
        },
        "profiling_store": {
            "name": "local_validation_result_store",
        },
        "datasources": ['mydatasource'],
        "sections": {
            "index": {
                "renderer": {
                    "module": "great_expectations.render.renderer",
                    "class": "SiteIndexPageRenderer"
                }
            },
            "expectations": {
                "renderer": {
                    "module": "great_expectations.render.renderer",
                    "class": "ExpectationSuitePageRenderer"
                },
                "view": {
                    "module": "great_expectations.render.view",
                    "class": "DefaultJinjaPageView"
                }
            },
            "profiling": {
                "run_id_filter": {
                    "eq": "profiling"
                },
                "renderer": {
                    "module": "great_expectations.render.renderer",
                    "class": "ProfilingResultsPageRenderer"
                },
                "view": {
                    "module": "great_expectations.render.view",
                    "class": "DefaultJinjaPageView"
                }
            },
            "validations": {
                "run_id_filter": {
                    "ne": "profiling"
                },
                "renderer": {
                    "module": "great_expectations.render.renderer",
                    "class": "ValidationResultsPageRenderer"
                },
                "view": {
                    "module": "great_expectations.render.view",
                    "class": "DefaultJinjaPageView"
                }
            },
        }
    }

    res = SiteBuilder.build(titanic_data_context, site_config)
    index_page_locator_info = res[0]
    index_links_dict = res[1]

    print( json.dumps(index_page_locator_info, indent=2) )
    assert index_page_locator_info['path'] == titanic_data_context.root_directory + '/uncommitted/documentation/local_site/index.html'

    print( json.dumps(index_links_dict, indent=2) )
    assert index_links_dict == {
        "mydatasource": {
            "mygenerator": {
            "Titanic": {
                "profiling_links": [
                {
                    "full_data_asset_name": "mydatasource/mygenerator/Titanic",
                    "expectation_suite_name": "BasicDatasetProfiler",
                    "filepath": "profiling/mydatasource/mygenerator/Titanic/BasicDatasetProfiler.html",
                    "source": "mydatasource",
                    "generator": "mygenerator",
                    "asset": "Titanic",
                    "run_id": None
                }
                ],
                "validation_links": [
                {
                    "full_data_asset_name": "mydatasource/mygenerator/Titanic",
                    "expectation_suite_name": "BasicDatasetProfiler",
                    "filepath": "validation/test_run_id_12345/mydatasource/mygenerator/Titanic/BasicDatasetProfiler.html",
                    "source": "mydatasource",
                    "generator": "mygenerator",
                    "asset": "Titanic",
                    "run_id": "test_run_id_12345"
                }
                ],
                "expectations_links": [
                {
                    "full_data_asset_name": "mydatasource/mygenerator/Titanic",
                    "expectation_suite_name": "BasicDatasetProfiler",
                    "filepath": "expectations/mydatasource/mygenerator/Titanic/BasicDatasetProfiler.html",
                    "source": "mydatasource",
                    "generator": "mygenerator",
                    "asset": "Titanic",
                    "run_id": None
                }
                ]
            }
            }
        }
        }

    assert "unused_datasource" not in index_links_dict, \
        """`unused_datasource` must not appear in this documentation, 
        because `datasources` config option specifies only `mydatasource`"""


    assert len(index_links_dict['mydatasource']['mygenerator']['Titanic']['expectations_links']) == 1, \
    """
    The only rendered expectation suite should be the one generated by the profiler
    """