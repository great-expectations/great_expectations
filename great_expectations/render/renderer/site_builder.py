import logging
logger = logging.getLogger(__name__)
import os

from great_expectations.render.renderer import (
    DescriptivePageRenderer,
    PrescriptivePageRenderer,
    SiteIndexPageRenderer
)
from great_expectations.render.view import (
    DefaultJinjaPageView,
    DefaultJinjaIndexPageView,
)

from great_expectations.data_context.util import NormalizedDataAssetName, get_slack_callback, safe_mmkdir

class SiteBuilder():

    @classmethod
    def build(cls, data_context, site_config):
        index_links = []

        # profiling results

        datasources = data_context.list_datasources()

        #TODO: filter data sources if the config requires it

        for run_id, v0 in data_context.list_validation_results(validation_store=site_config['validations_store']).items():
            for datasource, v1 in v0.items():
                for generator, v2 in v1.items():
                    for data_asset_name, expectation_suite_names in v2.items():
                        for expectation_suite_name in expectation_suite_names:
                            validation = data_context.get_validation_result(data_asset_name,
                                                                            expectation_suite_name=expectation_suite_name,
                                                                            validation_store=site_config['validations_store'],
                                                                            run_id=run_id)

                            run_id = validation['meta']['run_id']
                            data_asset_name = validation['meta']['data_asset_name']
                            expectation_suite_name = validation['meta']['expectation_suite_name']
                            model = DescriptivePageRenderer.render(validation)

                            data_context.write_resource(
                                DefaultJinjaPageView.render(model),  # bytes
                                expectation_suite_name + '.html',  # name to be used inside namespace
                                resource_store=site_config['site_store'],
                                resource_namespace="profiling",
                                data_asset_name=data_asset_name,
                                expectation_suite_name=expectation_suite_name,
                                run_id=run_id
                            )

                            index_links.append({
                                "data_asset_name": data_asset_name,
                                "expectation_suite_name": expectation_suite_name,
                                "run_id": run_id,
                                "filepath": data_context._get_normalized_data_asset_name_filepath(
                                        data_asset_name,
                                        expectation_suite_name,
                                        base_path = 'profiling',
                                        file_extension=".html"
                                        )
                            })

        for datasource, v1 in data_context.list_expectation_suites(expectations_store=site_config['expectations_store']):
            for generator, v2 in v1.items():
                for data_asset_name, expectation_suite_names in v2.items():
                    for expectation_suite_name in expectation_suite_names:
                        expectation_suite = data_context.get_expectation_suite(
                            data_asset_name,
                            expectation_suite_name=expectation_suite_name,
                            expectations_store=site_config['expectations_store'])

                        data_asset_name = expectation_suite['data_asset_name']
                        expectation_suite_name = expectation_suite['expectation_suite_name']
                        model = PrescriptivePageRenderer.render(expectation_suite)

                        data_context.write_resource(
                            DefaultJinjaPageView.render(model),  # bytes
                            expectation_suite_name + '.html',  # name to be used inside namespace
                            resource_store=site_config['site_store'],
                            resource_namespace='expectations',
                            data_asset_name=data_asset_name,
                            expectation_suite_name=expectation_suite_name
                        )

                        index_links.append({
                            "data_asset_name": data_asset_name,
                            "expectation_suite_name": expectation_suite_name,
                            "filepath": data_context._get_normalized_data_asset_name_filepath(
                                data_asset_name,
                                expectation_suite_name,
                                base_path='expectations',
                                file_extension='.html'
                            )
                        })

        model = SiteIndexPageRenderer.render(index_links)
        index_page_output = DefaultJinjaIndexPageView.render({
            "utm_medium": "index-page",
            "sections": model
        })

        if site_config['site_store']['type'] == 's3':
            raise NotImplementedError("site_store.type = s3")
        elif site_config['site_store']['type'] == 'filesystem':
            index_page_path = os.path.join(data_context.get_absolute_path(site_config['site_store']['base_directory']), "index.html")
            with open(index_page_path, "w") as writer:
                writer.write(index_page_output)
        else:
            raise ValueError("Unrecognized site_store.type: " + site_config['site_store']['type'])



