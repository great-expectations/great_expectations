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

from great_expectations.data_context.util import NormalizedDataAssetName, safe_mmkdir

class SiteBuilder():
    """SiteBuilder builds a data documentation website for the project defined by a DataContext.

    A data documentation site consists of HTML pages for expectation suites, profiling and validation results, and
    an index.html page that links to all the pages.

    The exact behavior of SiteBuilder is controlled by configuration in the DataContext's great_expectations.yml file.

    Users can specify
    * which datasources to document (by default, all)
    * whether to include expectations, validations and profiling results sections
    * where the expectations and validations should be read from (filesystem or S3)
    * where the HTML files should be written (filesystem or S3)

    Here is an example of config for a site:

      type: SiteBuilder
      site_store:
        type: filesystem
        base_directory: uncommitted/documentation/local_site
      expectations_store:
        type: filesystem
        base_directory: expectations/
      validations_store:
        type: filesystem
        base_directory: uncommitted/validations/
      profiling_store:
        type: filesystem
        base_directory: fixtures/validations/
      datasources: "*"
      sections:
        index:
          renderer:
            type: IndexRenderer
          view:
            type: IndexView
        validations:
          renderer:
            type: ValidationRenderer
            run_id_filter:
              ne: profiling
          view:
            type: ValidationPageView
        expectations:
          renderer:
            type: ExpectationRenderer
          view:
            type: ExpectationPageView
        profiling:
          renderer:
            type: ProfileRenderer
          view:
            type: ProfilePageView
    """

    @classmethod
    def build(cls, data_context, site_config):
        index_links = []

        # profiling results

        datasources = data_context.list_datasources()

        #TODO: filter data sources if the config requires it

        for run_id, v0 in data_context.list_validation_results(validations_store=site_config['profiling_store']).items():
            for datasource, v1 in v0.items():
                for generator, v2 in v1.items():
                    for generator_asset, expectation_suite_names in v2.items():
                        data_asset_name = data_context.data_asset_name_delimiter.join([datasource, generator, generator_asset])
                        for expectation_suite_name in expectation_suite_names:
                            validation = data_context.get_validation_result(data_asset_name,
                                                                            expectation_suite_name=expectation_suite_name,
                                                                            validations_store=site_config['profiling_store'],
                                                                            run_id=run_id)

                            data_asset_name = validation['meta']['data_asset_name']
                            expectation_suite_name = validation['meta']['expectation_suite_name']
                            model = DescriptivePageRenderer.render(validation)

                            data_context.write_resource(
                                DefaultJinjaPageView.render(model),  # bytes
                                expectation_suite_name + '.html',  # name to be used inside namespace
                                resource_store=site_config['site_store'],
                                resource_namespace="profiling",
                                data_asset_name=data_asset_name
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

        # expectation suites

        for datasource, v1 in data_context.list_expectation_suites().items():
            for generator, v2 in v1.items():
                for data_asset_name, expectation_suite_names in v2.items():
                    for expectation_suite_name in expectation_suite_names:
                        expectation_suite = data_context.get_expectation_suite(
                            data_asset_name,
                            expectation_suite_name=expectation_suite_name)

                        data_asset_name = expectation_suite['data_asset_name']
                        expectation_suite_name = expectation_suite['expectation_suite_name']
                        model = PrescriptivePageRenderer.render(expectation_suite)
                        data_context.write_resource(
                            DefaultJinjaPageView.render(model),  # bytes
                            expectation_suite_name + '.html',  # name to be used inside namespace
                            resource_store=site_config['site_store'],
                            resource_namespace='expectations',
                            data_asset_name=data_asset_name
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

        data_context.write_resource(
            index_page_output,  # bytes
            'index.html',  # name to be used inside namespace
            resource_store=site_config['site_store']
        )




