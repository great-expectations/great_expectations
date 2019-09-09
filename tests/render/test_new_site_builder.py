import os
import pytest
import json
import re

from great_expectations.util import (
    gen_directory_tree_str
)
from great_expectations.render.renderer.new_site_builder import (
    SiteBuilder,
    DefaultSiteSectionBuilder,
    DefaultSiteIndexBuilder,
)
from great_expectations.data_context.types import (
    ValidationResultIdentifier
)

def test_new_SiteBuilder(titanic_data_context, filesystem_csv_2):
    titanic_data_context.profile_datasource(titanic_data_context.list_datasources()[0]["name"])

    titanic_data_context.add_store(
        "local_site_html_store",
        {
            "module_name": "great_expectations.data_context.store",
            "class_name": "BasicInMemoryStore"
        }
    )

    my_site_builder = SiteBuilder(
        titanic_data_context,
        target_store_name= "local_site_html_store",
        site_index_builder= {
            "class_name": "DefaultSiteIndexBuilder",
        },
        site_section_builders = {
            "expectations" : {
                "class_name": "DefaultSiteSectionBuilder",
                "source_store_name": "expectations_store",
                "renderer" : {
                    "module_name": "great_expectations.render.renderer",
                    "class_name" : "ExpectationSuitePageRenderer",
                },
            }
        }
        # datasource_whitelist=[],
        # datasource_blacklist=[],
    )

    # FIXME : Re-up this
    # my_site_builder.build()

def test_SiteSectionBuilder(titanic_data_context, filesystem_csv_2):
    titanic_data_context.profile_datasource(titanic_data_context.list_datasources()[0]["name"])

    titanic_data_context.add_store(
        "local_site_html_store",
        {
            "module_name": "great_expectations.data_context.store",
            "class_name": "EvaluationParameterStore",
        }
    )

    my_site_section_builders = DefaultSiteSectionBuilder(
        titanic_data_context,
        source_store_name="expectations_store",
        target_store_name="local_site_html_store",
        renderer = {
            "module_name": "great_expectations.render.renderer",
            "class_name" : "ExpectationSuitePageRenderer",
        },
    )

    my_site_section_builders.build()

    print(titanic_data_context.stores["local_site_html_store"].list_keys())
    assert len(titanic_data_context.stores["local_site_html_store"].list_keys()) == 1
    
    first_key = titanic_data_context.stores["local_site_html_store"].list_keys()[0]
    assert first_key == "ExpectationSuiteIdentifier.mydatasource.mygenerator.Titanic.BasicDatasetProfiler"

    content = titanic_data_context.stores["local_site_html_store"].get(first_key)
    assert len(re.findall("<div.*>", content)) > 20 # Ah! Then it MUST be HTML!


# def test_SiteIndexBuilder(titanic_data_context, filesystem_csv_2):
#     titanic_data_context.profile_datasource(titanic_data_context.list_datasources()[0]["name"])

#     titanic_data_context.add_store(
#         "local_site_html_store",
#         {
#             "module_name": "great_expectations.data_context.store",
#             "class_name": "EvaluationParameterStore",
#         }
#     )

#     my_site_section_builders = DefaultSiteIndexBuilder(
#         titanic_data_context,
#         target_store_name="local_site_html_store",
#         renderer = {
#             "module_name": "great_expectations.render.renderer",
#             "class_name" : "ExpectationSuitePageRenderer",
#         },
#     )

#     my_site_section_builders.build()

#     print(titanic_data_context.stores["local_site_html_store"].list_keys())
#     assert len(titanic_data_context.stores["local_site_html_store"].list_keys()) == 1
    
#     first_key = titanic_data_context.stores["local_site_html_store"].list_keys()[0]
#     assert first_key == "ExpectationSuiteIdentifier.mydatasource.mygenerator.Titanic.BasicDatasetProfiler"

#     content = titanic_data_context.stores["local_site_html_store"].get(first_key)
#     assert len(re.findall("<div.*>", content)) > 20 # Ah! Then it MUST be HTML!

from great_expectations.data_context.store import (
    HtmlSiteStore
)

def test_HtmlSiteStore_with_FixedLengthTupleFileSystemStoreBackend(tmp_path_factory):

    path = str(tmp_path_factory.mktemp('test_HtmlSiteStore_with_FixedLengthTupleFileSystemStoreBackend__dir'))
    project_path = str(tmp_path_factory.mktemp('my_dir'))

    my_store = HtmlSiteStore(
        store_backend = {
            "module_name" : "great_expectations.data_context.store",
            "class_name" : "FixedLengthTupleFilesystemStoreBackend",
            "base_directory" : "my_store/",
            "filepath_template" : "{4}/{0}/{1}/{2}/{3}.txt",
        },
        root_directory = path,
    )

    with pytest.raises(TypeError):
        my_store.get("not_a_ValidationResultIdentifier")

    with pytest.raises(KeyError):
        my_store.get(ValidationResultIdentifier(**{}))
    
    ns_1 = ValidationResultIdentifier(
        from_string="ValidationResultIdentifier.a.b.c.quarantine.prod-100"
    )
    my_store.set(ns_1, {"A": "aaa"})
    assert my_store.get(ns_1) == {"A": "aaa"}

    ns_2 = ValidationResultIdentifier(
        from_string="ValidationResultIdentifier.a.b.c.quarantine.prod-20"
    )
    my_store.set(ns_2, {"B": "bbb"})
    assert my_store.get(ns_2) == {"B": "bbb"}

    print(my_store.list_keys())
    assert set(my_store.list_keys()) == set([
        ns_1,
        ns_2,
    ])

    print(gen_directory_tree_str(path))
    assert gen_directory_tree_str(path) == """\
test_HtmlSiteStore_with_FixedLengthTupleFileSystemStoreBackend__dir0/
    my_store/
        prod-100/
            a/
                b/
                    c/
                        quarantine.txt
        prod-20/
            a/
                b/
                    c/
                        quarantine.txt
"""
