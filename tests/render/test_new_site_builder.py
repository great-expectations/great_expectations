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
from great_expectations.render.renderer.new_site_builder import (
    SiteBuilder,
    DefaultSiteSectionBuilder,
    DefaultSiteIndexBuilder,
)
from great_expectations.data_context.types import (
    ValidationResultIdentifier,
    SiteSectionIdentifier,
)
from great_expectations.data_context.util import safe_mmkdir
from great_expectations.data_context import (
    DataContext,
)
from great_expectations.cli.init import scaffold_directories_and_notebooks

def test_new_SiteBuilder(tmp_path_factory, filesystem_csv_3):
    # TODO : Use a standard test fixture
    # TODO : Have that test fixture copy a directory, rather than building a new one from scratch
    
    base_dir = str(tmp_path_factory.mktemp("project_dir"))
    project_dir = os.path.join(base_dir, "project_path")
    os.mkdir(project_dir)
    
    os.makedirs(os.path.join(project_dir, "data"))
    os.makedirs(os.path.join(project_dir, "data/titanic"))
    curdir = os.path.abspath(os.getcwd())
    shutil.copy(
        "./tests/test_sets/Titanic.csv",
        str(os.path.join(project_dir, "data/titanic/Titanic.csv"))
    )
    
    os.makedirs(os.path.join(project_dir, "data/random"))
    curdir = os.path.abspath(os.getcwd())
    shutil.copy(
        os.path.join(filesystem_csv_3, "f1.csv"),
        str(os.path.join(project_dir, "data/random/f1.csv"))
    )
    shutil.copy(
        os.path.join(filesystem_csv_3, "f2.csv"),
        str(os.path.join(project_dir, "data/random/f2.csv"))
    )
    
    # assert gen_directory_tree_str(project_dir) == """\
    # project_path/
    #     data/
    #         random/
    #             f1.csv
    #             f2.csv
    #         titanic/
    #             Titanic.csv
    # """
    
    context = DataContext.create(project_dir)
    ge_directory = os.path.join(project_dir, "great_expectations")
    scaffold_directories_and_notebooks(ge_directory)
    context.add_datasource(
        "titanic",
        "pandas",
        base_directory=os.path.join(project_dir, "data/titanic/")
    )
    context.add_datasource(
        "random",
        "pandas",
        base_directory=os.path.join(project_dir, "data/random/")
    )
    
    context.profile_datasource("titanic")
    # print(gen_directory_tree_str(project_dir))

    context.profile_datasource("random")
    # print(gen_directory_tree_str(project_dir))
    
    # save documentation locally
    safe_mmkdir("./tests/data_context/output")
    safe_mmkdir("./tests/data_context/output/documentation")
    
    if os.path.isdir("./tests/data_context/output/documentation"):
        shutil.rmtree("./tests/data_context/output/documentation")
    shutil.copytree(
        os.path.join(
            ge_directory,
            "uncommitted/documentation/"
        ),
        "./tests/data_context/output/documentation"
    )
    
    context.profile_datasource(context.list_datasources()[0]["name"])

    context.add_store(
        "local_site_html_store",
        {
            "module_name": "great_expectations.data_context.store",
            "class_name": "HtmlSiteStore",
            "base_directory": os.path.join(project_dir, "great_expectations/uncommitted/documentation")
        }
    )

    my_site_builder = SiteBuilder(
        context,
        target_store_name="local_site_html_store",
        site_index_builder={
            "class_name": "DefaultSiteIndexBuilder",
        },
        site_section_builders={
            "expectations": {
                "class_name": "DefaultSiteSectionBuilder",
                "source_store_name": "expectations_store",
                "renderer": {
                    "module_name": "great_expectations.render.renderer",
                    "class_name": "ExpectationSuitePageRenderer",
                },
            },
            "validation": {
                "class_name": "DefaultSiteSectionBuilder",
                "source_store_name": "local_validation_result_store",
                "renderer": {
                    "module_name": "great_expectations.render.renderer",
                    "class_name": "ValidationResultsPageRenderer"
                }
            },
            "profiling": {
                "class_name": "DefaultSiteSectionBuilder",
                "source_store_name": "local_validation_result_store",
                "renderer": {
                    "module_name": "great_expectations.render.renderer",
                    "class_name": "ProfilingResultsPageRenderer"
                }
            }
        }
        # datasource_whitelist=[],
        # datasource_blacklist=[],
    )

    # FIXME : Re-up this
    my_site_builder.build()

    # save documentation locally
    safe_mmkdir("./tests/data_context/output")
    safe_mmkdir("./tests/data_context/output/documentation")

    if os.path.isdir("./tests/data_context/output/documentation"):
        shutil.rmtree("./tests/data_context/output/documentation")
    shutil.copytree(
        os.path.join(
            ge_directory,
            "uncommitted/documentation/"
        ),
        "./tests/data_context/output/documentation"
    )

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
        "test_name",
        titanic_data_context,
        source_store_name="expectations_store",
        target_store_name="local_site_html_store",
        renderer={
            "module_name": "great_expectations.render.renderer",
            "class_name" : "ExpectationSuitePageRenderer",
        },
    )

    my_site_section_builders.build()

    print(titanic_data_context.stores["local_site_html_store"].list_keys())
    assert len(titanic_data_context.stores["local_site_html_store"].list_keys()) == 1
    
    first_key = titanic_data_context.stores["local_site_html_store"].list_keys()[0]
    print(first_key)
    print(type(first_key))
    assert first_key == SiteSectionIdentifier(
        site_section_name="test_name",
        resource_identifier=parse_string_to_data_context_resource_identifier(
            "ExpectationSuiteIdentifier.mydatasource.mygenerator.Titanic.BasicDatasetProfiler"
        ),
    )

    content = titanic_data_context.stores["local_site_html_store"].get(first_key)
    assert len(re.findall("<div.*>", content)) > 20 # Ah! Then it MUST be HTML!
    with open('./tests/render/output/test_SiteSectionBuilder.html', 'w') as f:
        f.write(content)


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