import os
import pytest
from great_expectations.render.renderer.site_builder import SiteBuilder



def test_cli_profile(titanic_data_context):

    titanic_data_context.profile_datasource(titanic_data_context.list_datasources()[0]["name"])

    site_config = {
        "site_store": {
            "type": "filesystem",
            "base_directory": "uncommitted/documentation/local_site"
        },
        "validations_store": {
            "type": "filesystem",
            "base_directory": "uncommitted/validations/",
            "run_id_filter": {
                "ne": "profiling"
            }
        },
        "profiling_store": {
            "type": "filesystem",
            "base_directory": "uncommitted/validations/",
            "run_id_filter": {
                "eq": "profiling"
            }
        },
        "datasources": "*",
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
        }

    }

    res = SiteBuilder.build(titanic_data_context, site_config)
    index_page_locator_info = res[0]
    index_links_dict = res[1]

    assert index_page_locator_info['path'] == titanic_data_context.data_doc_directory + '/local_site/index.html'
    assert len(index_links_dict['mydatasource']['mygenerator']['Titanic']['expectation_suite_links']) == 1
    assert len(index_links_dict['mydatasource']['mygenerator']['Titanic']['validation_links']) == 0
    assert len(index_links_dict['mydatasource']['mygenerator']['Titanic']['profiling_links']) == 0

