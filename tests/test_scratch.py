# import json
import great_expectations as ge
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.dataset import Dataset
# import great_expectations.jupyter_ux
# from great_expectations.datasource.types import BatchKwargs
# from datetime import datetime


def test_scratch(tmp_path_factory):
    context = ge.data_context.DataContext(
        context_root_dir='/Users/roblim/Dropbox/coding/superconductive/test_ge_project_dir/great_expectations'
    )

    # context.build_data_docs()
    # expectation_suite_name = "warning"
    # datasource_name = 'files_datasource'
    # batch_kwargs = {
    #     'path': "/Users/roblim/Dropbox/coding/superconductive/test_ge_project_dir/dataset_diabetes/IDs_mapping.csv",
    #     'datasource': datasource_name
    # }
    # docs_build_output = context.build_data_docs()
    # batch = context.get_batch(batch_kwargs, expectation_suite_name)
    # batch.edit_expectation_suite()
    # project_path = str(tmp_path_factory.mktemp('empty_data_context'))
    # test = ge.data_context.DataContext.create(project_path)
    # blah = 'blah'
    # test._project_config.anonymized_usage_statistics['enabled'] = False

    sites = context._project_config_with_variables_substituted.get('data_docs_sites', [])
    if sites:
        for site_name, site_config in sites.items():
                complete_site_config = site_config
                module_name = 'great_expectations.render.renderer.site_builder'
                site_builder = instantiate_class_from_config(
                    config=complete_site_config,
                    runtime_environment={
                        "data_context": context,
                        "root_directory": context.root_directory,
                        "site_name": site_name
                    },
                    config_defaults={
                        "module_name": module_name
                    }
                )

                site_builder.site_index_builder.build()
                # context.open_data_docs()