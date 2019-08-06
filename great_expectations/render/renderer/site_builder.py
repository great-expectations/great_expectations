import logging
logger = logging.getLogger(__name__)
import importlib

from great_expectations.render.renderer import (
    SiteIndexPageRenderer
)
from great_expectations.render.view import (
    DefaultJinjaIndexPageView,
)

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
    * which renderer and view class should be used to render each section

    Here is an example of config for a site:

    local_site:
      type: SiteBuilder
      site_store:
        type: filesystem
        base_directory: uncommitted/documentation/local_site
      validations_store:
        type: filesystem
        base_directory: uncommitted/validations/
      profiling_store:
        type: filesystem
        base_directory: uncommitted/validations/
        run_id_filter:
          eq: profiling

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
            module: great_expectations.render.renderer
            class: PrescriptivePageRenderer
          view:
            module: great_expectations.render.view
            class: DefaultJinjaPageView
        profiling:
          renderer:
            module: great_expectations.render.renderer
            class: DescriptivePageRenderer
          view:
            module: great_expectations.render.view
            class: DefaultJinjaPageView


    """

    @classmethod
    def build(cls, data_context, site_config):
        index_links = []

        # profiling results

        datasources = data_context.list_datasources()

        sections_config = site_config.get('sections')
        if not sections_config:
            raise Exception('"sections" key is missing') #TODO: specific exception class

        profiling_section_config = sections_config.get('profiling')
        if profiling_section_config:
            try:
                profiling_renderer_module = importlib.import_module(profiling_section_config['renderer']['module'])
                profiling_renderer_class = getattr(profiling_renderer_module, profiling_section_config['renderer']['class'])
                profiling_view_module = importlib.import_module(profiling_section_config['view']['module'])
                profiling_view_class = getattr(profiling_view_module, profiling_section_config['view']['class'])
            except Exception:
                logger.exception("Failed to load profiling renderer or view class")
                raise


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
                                model = profiling_renderer_class.render(validation)

                                data_context.write_resource(
                                    profiling_view_class.render(model),  # bytes
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

        expectations_section_config = sections_config.get('expectations')
        if expectations_section_config:
            try:
                expectations_renderer_module = importlib.import_module(expectations_section_config['renderer']['module'])
                expectations_renderer_class = getattr(expectations_renderer_module, expectations_section_config['renderer']['class'])
                expectations_view_module = importlib.import_module(expectations_section_config['view']['module'])
                expectations_view_class = getattr(expectations_view_module, expectations_section_config['view']['class'])
            except Exception:
                logger.exception("Failed to load expectations renderer or view class")
                raise

            for datasource, v1 in data_context.list_expectation_suites().items():
                for generator, v2 in v1.items():
                    for data_asset_name, expectation_suite_names in v2.items():
                        for expectation_suite_name in expectation_suite_names:
                            expectation_suite = data_context.get_expectation_suite(
                                data_asset_name,
                                expectation_suite_name=expectation_suite_name)

                            data_asset_name = expectation_suite['data_asset_name']
                            expectation_suite_name = expectation_suite['expectation_suite_name']
                            model = expectations_renderer_class.render(expectation_suite)
                            data_context.write_resource(
                                expectations_view_class.render(model),  # bytes
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




