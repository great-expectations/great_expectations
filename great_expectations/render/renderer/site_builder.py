import logging
logger = logging.getLogger(__name__)

import json
import importlib
from collections import OrderedDict

from great_expectations.render.renderer import (
    SiteIndexPageRenderer
)
from great_expectations.render.view import (
    DefaultJinjaIndexPageView,
)
from great_expectations.data_context.types import (
    ValidationResultIdentifier,
    ExpectationSuiteIdentifier,
    DataAssetIdentifier,
    NormalizedDataAssetName,
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
        run_id_filter:
          ne: profiling
      profiling_store:
        type: filesystem
        base_directory: uncommitted/validations/
        run_id_filter:
          eq: profiling

      datasources: '*'
      sections:
        index:
          renderer:
            module: great_expectations.render.renderer
            class: SiteIndexPageRenderer
          view:
            module: great_expectations.render.view
            class: DefaultJinjaIndexPageView
        validations:
          renderer:
            module: great_expectations.render.renderer
            class: ValidationResultsPageRenderer
          view:
            module: great_expectations.render.view
            class: DefaultJinjaPageView
        expectations:
          renderer:
            module: great_expectations.render.renderer
            class: ExpectationSuitePageRenderer
          view:
            module: great_expectations.render.view
            class: DefaultJinjaPageView
        profiling:
          renderer:
            module: great_expectations.render.renderer
            class: ProfilingResultsPageRenderer
          view:
            module: great_expectations.render.view
            class: DefaultJinjaPageView
    """

    @classmethod
    def build(cls, data_context, site_config, specified_data_asset_name=None):
        """

        :param data_context:
        :param site_config:
        :return: tupple: index_page_locator_info (a dictionary describing how to locate the index page of the site (specific to resource_store type)
                         index_links_dict

        """
        logger.debug("Starting SiteBuilder.build")

        index_links_dict = OrderedDict()

        # the site config may specify the list of datasource names to document.
        # if the config property is absent or is *, treat as "all"
        datasources_to_document = site_config.get('datasources')
        if not datasources_to_document or datasources_to_document == '*':
            datasources_to_document = [datasource['name'] for datasource in data_context.list_datasources()]


        sections_config = site_config.get('sections')
        if not sections_config:
            raise Exception('"sections" key is missing') #TODO: specific exception class

        # profiling results

        profiling_section_config = sections_config.get('profiling')
        if profiling_section_config:
            new_index_links_dict = cls.generate_profiling_section(
                profiling_section_config,
                data_context,
                index_links_dict,
                datasources_to_document,
                specified_data_asset_name,
                site_config['site_store'],
                site_config['profiling_store']['name']
            )

        # validations

        validation_section_config = sections_config.get('validations')
        if validation_section_config:
            #TODO : Everything in this if statement can probably be factored into a generic version of `generate_profiling_section` -> `generate_section`
            validation_renderer_class, validation_view_class = cls.get_renderer_and_view_classes(validation_section_config)

            # FIXME: local_validation_result_store should not be hardcoded
            for validation_result_key in data_context.stores['local_validation_result_store'].list_keys():
                run_id = validation_result_key.run_id
                run_id_filter = validation_section_config.get("run_id_filter")
                # run_id_filter attribute in the config of validation store allows to filter run ids
                if run_id_filter:
                    if run_id_filter.get("eq"):
                        if run_id_filter.get("eq") != run_id:
                            continue
                    elif run_id_filter.get("ne"):
                        if run_id_filter.get("ne") == run_id:
                            continue

                datasource = validation_result_key.expectation_suite_identifier.data_asset_name.datasource
                if datasource not in datasources_to_document:
                    continue

                data_asset_name = validation_result_key.expectation_suite_identifier.data_asset_name
                if specified_data_asset_name:
                    if data_asset_name != data_context._normalize_data_asset_name(specified_data_asset_name):
                        continue

                generator = validation_result_key.expectation_suite_identifier.data_asset_name.generator

                generator_asset = validation_result_key.expectation_suite_identifier.data_asset_name.generator_asset

                expectation_suite_name = validation_result_key.expectation_suite_identifier.expectation_suite_name

                validation = data_context.get_validation_result(data_asset_name,
                                                                expectation_suite_name=expectation_suite_name,
                                                                validations_store_name=site_config['validations_store']['name'],
                                                                run_id=run_id)
                logger.info("        Rendering validation: run id: {}, suite {} for data asset {}".format(run_id,
                                                                                                          expectation_suite_name,
                                                                                                          data_asset_name))
                data_asset_name = validation['meta']['data_asset_name']
                expectation_suite_name = validation['meta']['expectation_suite_name']
                model = validation_renderer_class.render(validation)

                data_context.write_resource(
                    validation_view_class.render(model),  # bytes
                    expectation_suite_name + '.html',  # name to be used inside namespace
                    resource_store=site_config['site_store'],
                    resource_namespace="validation",
                    data_asset_name=data_asset_name,
                    run_id=run_id
                )

                index_links_dict = cls.add_resource_info_to_index_links_dict(
                    data_context,
                    index_links_dict,
                    data_asset_name,
                    datasource, generator, generator_asset, expectation_suite_name,
                    "validation",
                    run_id = run_id
                )

        # expectation suites

        expectations_section_config = sections_config.get('expectations')
        if expectations_section_config:
            #TODO : Everything in this if statement can probably be factored into a generic version of `generate_profiling_section` -> `generate_section`
            # It's unclear whether the difference between Expectations and ValidationResults will require a second function.

            expectations_renderer_class, expectations_view_class = cls.get_renderer_and_view_classes(expectations_section_config)

            for expectation_suite_key in data_context.stores['expectations_store'].list_keys():

            # for datasource, v1 in data_context.list_expectation_suites().items():

            #     if datasource not in datasources_to_document:
            #         continue

            #     for generator, v2 in v1.items():
            #         for generator_asset, expectation_suite_names in v2.items():
            #             data_asset_name = data_context.data_asset_name_delimiter.join(
            #                 [datasource, generator, generator_asset])
            #             if specified_data_asset_name:
            #                    if data_context._normalize_data_asset_name(data_asset_name) != data_context._normalize_data_asset_name(specified_data_asset_name):
            #                        continue
            #             for expectation_suite_name in expectation_suite_names:
            #                 expectation_suite = data_context.get_expectation_suite(
            #                     data_asset_name,
            #                     expectation_suite_name=expectation_suite_name)

                            expectation_suite = data_context.stores['expectations_store'].get(expectation_suite_key)
                            data_asset_name = expectation_suite["data_asset_name"]
                            expectation_suite_name = expectation_suite_key.expectation_suite_name

                            logger.info(
                                "        Rendering expectation suite {} for data asset {}".format(
                                    expectation_suite_name,
                                    data_asset_name
                                ))
                            model = expectations_renderer_class.render(expectation_suite)

                            data_context.write_resource(
                                expectations_view_class.render(model),  # bytes
                                expectation_suite_name + '.html',  # name to be used inside namespace
                                resource_store=site_config['site_store'],
                                resource_namespace='expectations',
                                data_asset_name=data_asset_name
                            )

                            index_links_dict = cls.add_resource_info_to_index_links_dict(
                                data_context,
                                index_links_dict,
                                data_asset_name,
                                expectation_suite_key.data_asset_name.datasource,
                                expectation_suite_key.data_asset_name.generator,
                                expectation_suite_key.data_asset_name.generator_asset,
                                expectation_suite_key.expectation_suite_name,
                                "expectations",
                            )


        # TODO: load dynamically
        model = SiteIndexPageRenderer.render(index_links_dict)

        index_page_output = DefaultJinjaIndexPageView.render(model)

        index_page_locator_info = data_context.write_resource(
            index_page_output,  # bytes
            'index.html',  # name to be used inside namespace
            resource_store=site_config['site_store']
        )

        return (index_page_locator_info, index_links_dict)

    @classmethod
    def get_renderer_and_view_classes(cls, section_config):
        try:
            renderer_module = importlib.import_module(section_config['renderer']['module'])
            renderer_class = getattr(renderer_module, section_config['renderer']['class'])
            view_module = importlib.import_module(section_config['view']['module'])
            view_class = getattr(view_module, section_config['view']['class'])
        except Exception:
            logger.exception("Failed to load profiling renderer or view class")
            raise
        
        return renderer_class, view_class


    @classmethod
    def generate_profiling_section(cls, section_config, data_context, index_links_dict, datasources_to_document, specified_data_asset_name, resource_store, validations_store_name):
        logger.debug("Starting SiteBuilder.generate_profiling_section")

        profiling_renderer_class, profiling_view_class = cls.get_renderer_and_view_classes(section_config)

        for validation_result_key in data_context.stores['local_validation_result_store'].list_keys():
            run_id = validation_result_key.run_id
            run_id_filter = section_config.get("run_id_filter")
            # run_id_filter attribute in the config of validation store allows to filter run ids
            if run_id_filter:
                if run_id_filter.get("eq"):
                    if run_id_filter.get("eq") != run_id:
                        continue
                elif run_id_filter.get("ne"):
                    if run_id_filter.get("ne") == run_id:
                        continue

            datasource = validation_result_key.expectation_suite_identifier.data_asset_name.datasource
            if datasource not in datasources_to_document:
                continue

            data_asset_name = validation_result_key.expectation_suite_identifier.data_asset_name
            if specified_data_asset_name:
                if data_asset_name != data_context._normalize_data_asset_name(specified_data_asset_name):
                    continue

            generator = validation_result_key.expectation_suite_identifier.data_asset_name.generator

            generator_asset = validation_result_key.expectation_suite_identifier.data_asset_name.generator_asset

            expectation_suite_name = validation_result_key.expectation_suite_identifier.expectation_suite_name

            validation = data_context.get_validation_result(data_asset_name,
                                                            expectation_suite_name=expectation_suite_name,
                                                            validations_store_name=validations_store_name,
                                                            run_id=run_id)
            logger.info("        Rendering profiling for data asset {}".format(data_asset_name))
            data_asset_name = validation['meta']['data_asset_name']
            expectation_suite_name = validation['meta']['expectation_suite_name']
            model = profiling_renderer_class.render(validation)

            data_context.write_resource(
                profiling_view_class.render(model),  # bytes
                expectation_suite_name + '.html',  # name to be used inside namespace
                resource_store=resource_store,
                resource_namespace="profiling",
                data_asset_name=data_asset_name
            )

            index_links_dict = cls.add_resource_info_to_index_links_dict(
                data_context,
                index_links_dict,
                data_asset_name,
                datasource, generator, generator_asset, expectation_suite_name, "profiling"
            )


    @classmethod
    def add_resource_info_to_index_links_dict(cls,
        data_context,
        index_links_dict,
        data_asset_name,
        datasource,
        generator,
        generator_asset,
        expectation_suite_name,
        section_name,
        run_id=None
    ):
        if not datasource in index_links_dict:
            index_links_dict[datasource] = OrderedDict()

        if not generator in index_links_dict[datasource]:
            index_links_dict[datasource][generator] = OrderedDict()

        if not generator_asset in index_links_dict[datasource][generator]:
            index_links_dict[datasource][generator][generator_asset] = {
                'profiling_links': [],
                'validation_links': [],
                'expectations_links': []
            }


        if run_id:
            base_path = section_name + "/" + run_id
        else:
            base_path = section_name
        
        index_links_dict[datasource][generator][generator_asset][section_name + "_links"].append(
            {
                "full_data_asset_name": data_asset_name,
                "expectation_suite_name": expectation_suite_name,
                "filepath": data_context._get_normalized_data_asset_name_filepath(
                    data_asset_name,
                    expectation_suite_name,
                    base_path=base_path,
                    file_extension=".html"
                ),
                "source": datasource,
                "generator": generator,
                "asset": generator_asset,
                "run_id": run_id
            }
        )

        return index_links_dict

    # @classmethod
    # def pack_validation_result_list_into_nested_dict(cls, validation_result_list, run_id_filter=None):
    #     """
    #     {
    #       "run_id":
    #         "datasource": {
    #             "generator": {
    #                 "generator_asset": [expectation_suite_1, expectation_suite_1, ...]
    #             }
    #         }
    #     }
    #     """
    #
    #     # NOTE : Future versions of Stores might allow fetching of nested objects.
    #     # In that case, this logic would almost certainly live there instead.
    #
    #     validation_results = {}
    #
    #     relative_paths = validation_result_list
    #
    #     for result in relative_paths:
    #         assert isinstance(result, ValidationResultIdentifier)
    #
    #         run_id = result.run_id
    #
    #         # run_id_filter attribute in the config of validation store allows to filter run ids
    #         if run_id_filter:
    #             if run_id_filter.get("eq"):
    #                 if run_id_filter.get("eq") != run_id:
    #                     continue
    #             elif run_id_filter.get("ne"):
    #                 if run_id_filter.get("ne") == run_id:
    #                     continue
    #
    #         datasource_name = result.expectation_suite_identifier.data_asset_name.datasource
    #         generator_name = result.expectation_suite_identifier.data_asset_name.generator
    #         generator_asset = result.expectation_suite_identifier.data_asset_name.generator_asset
    #
    #         expectation_suite = result.expectation_suite_identifier.expectation_suite_name
    #
    #         if run_id not in validation_results:
    #             validation_results[run_id] = {}
    #
    #         if datasource_name not in validation_results[run_id]:
    #             validation_results[run_id][datasource_name] = {}
    #
    #         if generator_name not in validation_results[run_id][datasource_name]:
    #             validation_results[run_id][datasource_name][generator_name] = {}
    #
    #         if generator_asset not in validation_results[run_id][datasource_name][generator_name]:
    #             validation_results[run_id][datasource_name][generator_name][generator_asset] = []
    #
    #         validation_results[run_id][datasource_name][generator_name][generator_asset].append(expectation_suite)
    #
    #     # print(validation_results)
    #     return validation_results
