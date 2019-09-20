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
from great_expectations.data_context.util import (
    instantiate_class_from_config,
)

from great_expectations.data_context.store.namespaced_read_write_store import (
    SiteSectionIdentifier,
)

class SiteBuilder(object):
    """SiteBuilder builds data documentation for the project defined by a DataContext.

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
        
        class_name: SiteBuilder
        target_store_name: local_site_html_store

        site_index_builder:
            class_name: DefaultSiteIndexBuilder

        # Verbose version:
        # index_builder:
        #     module_name: great_expectations.render.builder
        #     class_name: DefaultSiteIndexBuilder
        #     renderer:
        #         module_name: great_expectations.render.renderer
        #         class_name: SiteIndexPageRenderer
        #     view:
        #         module_name: great_expectations.render.view
        #         class_name: DefaultJinjaIndexPageView

        site_section_builders:
            # Minimal specification
            expectations:
                class_name: DefaultSiteSectionBuilder
                source_store_name: expectation_store
            renderer:
                module_name: great_expectations.render.renderer
                class_name: ExpectationSuitePageRenderer
                    
            # More verbose specification with optional arguments
            validations:
                module_name: great_expectations.data_context.render
                class_name: DefaultSiteSectionBuilder
                source_store_name: local_validation_store
                renderer:
                    module_name: great_expectations.render.renderer
                    class_name: SiteIndexPageRenderer
                view:
                    module_name: great_expectations.render.view
                    class_name: DefaultJinjaIndexPageView

        # specify a whitelist here if you would like to restrict the datasources to document
        datasource_whitelist:

    """

    def __init__(self,
        data_context,
        target_store_name,
        site_index_builder,
        site_section_builders,
        datasource_whitelist=None
    ):
        self.data_context = data_context
        self.target_store_name = target_store_name

        # the site config may specify the list of datasource names to document.
        # if the config property is absent or is *, treat as "all"
        self.datasource_whitelist = datasource_whitelist
        if not self.datasource_whitelist or self.datasource_whitelist == '*':
            self.datasource_whitelist = [datasource['name'] for datasource in data_context.list_datasources()]

        self.site_index_builder = instantiate_class_from_config(
            config=site_index_builder,
            runtime_config={
                "data_context": data_context,
                "target_store_name": target_store_name,
            },
            config_defaults={
                "name": "site_index_builder",
                "module_name": "great_expectations.render.renderer.site_builder"
            }
        )

        self.site_section_builders = {}
        for site_section_name, site_section_config in site_section_builders.items():
            self.site_section_builders[site_section_name] = instantiate_class_from_config(
                config=site_section_config,
                runtime_config={
                    "data_context": data_context,
                    "target_store_name": target_store_name,
                },
                config_defaults={
                    "name": site_section_name,
                    "module_name": "great_expectations.render.renderer.site_builder"
                }
            )

    def build(self):
        for site_section, site_section_builder in self.site_section_builders.items():
            site_section_builder.build(datasource_whitelist=self.datasource_whitelist)
        
        return self.site_index_builder.build()


class DefaultSiteSectionBuilder(object):

    def __init__(self,
        name,
        data_context,
        target_store_name,
        source_store_name,
        run_id_filter=None, #NOTE: Ideally, this would allow specification of ANY element (or combination of elements) within an ID key
        renderer=None,
        view=None,
    ):
        self.name = name
        self.source_store = data_context.stores[source_store_name]
        self.target_store = data_context.stores[target_store_name]
        self.run_id_filter = run_id_filter

        # TODO : Push conventions for configurability down to renderers and views.
        # Until then, they won't be configurable, and defaults will be hard.
        if renderer == None:
            renderer = {
                "module_name": "great_expectations.render.renderer",
                # "class_name": "SiteIndexPageRenderer",
            }
        renderer_module = importlib.import_module(renderer.pop("module_name"))
        self.renderer_class = getattr(renderer_module, renderer.pop("class_name"))

        if view == None:
            view = {
                "module_name": "great_expectations.render.view",
                "class_name": "DefaultJinjaPageView",
            }
        view_module = importlib.import_module(view.pop("module_name"))
        self.view_class = getattr(view_module, view.pop("class_name"))

    def build(self, datasource_whitelist):
        for resource_key in self.source_store.list_keys():

            if self.run_id_filter:
                if not self._resource_key_passes_run_id_filter(resource_key):
                    continue
                    
            if not self._resource_key_passes_datasource_whitelist(resource_key, datasource_whitelist):
                continue
                
            resource = self.source_store.get(resource_key)

            if type(resource_key) is ExpectationSuiteIdentifier:
                expectation_suite_name = resource_key.expectation_suite_name
                data_asset_name = resource_key.data_asset_name.generator_asset
                logger.info(
                    "        Rendering expectation suite {} for data asset {}".format(
                        expectation_suite_name,
                        data_asset_name
                    ))
            elif type(resource_key) is ValidationResultIdentifier:
                data_asset_name = resource_key.expectation_suite_identifier.data_asset_name.generator_asset
                run_id = resource_key.run_id
                expectation_suite_name = resource_key.expectation_suite_identifier.expectation_suite_name
                if run_id == "profiling":
                    logger.info("        Rendering profiling for data asset {}".format(data_asset_name))
                else:
                    
                    logger.info("        Rendering validation: run id: {}, suite {} for data asset {}".format(run_id,
                                                                                                              expectation_suite_name,
                                                                                                              data_asset_name))

            # TODO : Typing resources is SUPER important for usability now that we're slapping configurable renders together with arbitrary stores.
            rendered_content = self.renderer_class.render(resource)
            viewable_content = self.view_class.render(rendered_content)

            self.target_store.set(
                SiteSectionIdentifier(
                    site_section_name=self.name,
                    resource_identifier=resource_key,
                ),
                viewable_content
            )

    def _resource_key_passes_datasource_whitelist(self, resource_key, datasource_whitelist):
        if type(resource_key) is ExpectationSuiteIdentifier:
            datasource = resource_key.data_asset_name.datasource
        elif type(resource_key) is ValidationResultIdentifier:
            datasource = resource_key.expectation_suite_identifier.data_asset_name.datasource
        return datasource in datasource_whitelist
    
    def _resource_key_passes_run_id_filter(self, resource_key):
        if type(resource_key) == ValidationResultIdentifier:
            run_id = resource_key.run_id
        else:
            raise TypeError("run_id_filter filtering is only implemented for ValidationResultResources.")

        if self.run_id_filter.get("eq"):
            return self.run_id_filter.get("eq") == run_id

        elif self.run_id_filter.get("ne"):
            return self.run_id_filter.get("ne") != run_id


class DefaultSiteIndexBuilder(object):

    def __init__(self,
        name,
        data_context,
        target_store_name,
        renderer=None,
        view=None,
    ):
        # NOTE: This method is almost idenitcal to DefaultSiteSectionBuilder
        self.name = name
        self.data_context = data_context
        self.target_store = data_context.stores[target_store_name]

        # TODO : Push conventions for configurability down to renderers and views.
        # Until then, they won't be configurable, and defaults will be hard.
        if renderer == None:
            renderer = {
                "module_name": "great_expectations.render.renderer",
                "class_name": "SiteIndexPageRenderer",
            }
        renderer_module = importlib.import_module(renderer.pop("module_name"))
        self.renderer_class = getattr(renderer_module, renderer.pop("class_name"))

        if view == None:
            view = {
                "module_name" : "great_expectations.render.view",
                "class_name" : "DefaultJinjaIndexPageView",
            }
        view_module = importlib.import_module(view.pop("module_name"))
        self.view_class = getattr(view_module, view.pop("class_name"))

    def add_resource_info_to_index_links_dict(self,
                                              data_context,
                                              index_links_dict,
                                              data_asset_name,
                                              datasource,
                                              generator,
                                              generator_asset,
                                              expectation_suite_name,
                                              section_name,
                                              run_id=None,
                                              validation_success=None
                                              ):
        if not datasource in index_links_dict:
            index_links_dict[datasource] = OrderedDict()
    
        if not generator in index_links_dict[datasource]:
            index_links_dict[datasource][generator] = OrderedDict()
    
        if not generator_asset in index_links_dict[datasource][generator]:
            index_links_dict[datasource][generator][generator_asset] = {
                'profiling_links': [],
                'validations_links': [],
                'expectations_links': []
            }
    
        if run_id:
            base_path = "validations/" + run_id
        else:
            base_path = "expectations"
    
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
                "run_id": run_id,
                "validation_success": validation_success
            }
        )
    
        return index_links_dict

    def build(self):
        # Loop over sections in the HtmlStore
        logger.debug("DefaultSiteIndexBuilder.build")

        target_store = self.target_store
        resource_keys = target_store.list_keys()
        index_links_dict = OrderedDict()

        for key in resource_keys:
            key_resource_identifier = key.resource_identifier
            
            if type(key_resource_identifier) == ExpectationSuiteIdentifier:
                self.add_resource_info_to_index_links_dict(
                    data_context=self.data_context,
                    index_links_dict=index_links_dict,
                    data_asset_name=key_resource_identifier.data_asset_name.to_string(
                        include_class_prefix=False,
                        separator=self.data_context.data_asset_name_delimiter
                    ),
                    datasource=key_resource_identifier.data_asset_name.datasource,
                    generator=key_resource_identifier.data_asset_name.generator,
                    generator_asset=key_resource_identifier.data_asset_name.generator_asset,
                    expectation_suite_name=key_resource_identifier.expectation_suite_name,
                    section_name=key.site_section_name
                )
            elif type(key_resource_identifier) == ValidationResultIdentifier:
                data_asset_name = key_resource_identifier.expectation_suite_identifier.data_asset_name.to_string(
                        include_class_prefix=False,
                        separator=self.data_context.data_asset_name_delimiter
                    )
                expectation_suite_name = key_resource_identifier.expectation_suite_identifier.expectation_suite_name
                run_id = key_resource_identifier.run_id
                validation = self.data_context.get_validation_result(
                    data_asset_name=data_asset_name,
                    expectation_suite_name=expectation_suite_name,
                    validations_store_name="local_validation_result_store",
                    run_id=run_id
                )
                
                validation_success = validation.get("success")
                
                self.add_resource_info_to_index_links_dict(
                    data_context=self.data_context,
                    index_links_dict=index_links_dict,
                    data_asset_name=key_resource_identifier.expectation_suite_identifier.data_asset_name.to_string(
                        include_class_prefix=False,
                        separator=self.data_context.data_asset_name_delimiter
                    ),
                    datasource=key_resource_identifier.expectation_suite_identifier.data_asset_name.datasource,
                    generator=key_resource_identifier.expectation_suite_identifier.data_asset_name.generator,
                    generator_asset=key_resource_identifier.expectation_suite_identifier.data_asset_name.generator_asset,
                    expectation_suite_name=expectation_suite_name,
                    section_name=key.site_section_name,
                    run_id=run_id,
                    validation_success=validation_success
                )

        rendered_content = self.renderer_class.render(index_links_dict)
        viewable_content = self.view_class.render(rendered_content)

        return (
            self.target_store.write_index_page(viewable_content),
            index_links_dict
        )