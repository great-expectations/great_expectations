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

        self.site_index_builder = instantiate_class_from_config(
            config=site_index_builder,
            runtime_config={
                "data_context": data_context,
                "target_store_name": target_store_name,
            },
            config_defaults={
                "name" : "site_index_builder",
                "module_name": "great_expectations.render.renderer.new_site_builder"
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
                    "name" : site_section_name,
                    "module_name": "great_expectations.render.renderer.new_site_builder"
                }
            )

    def build(self):
        for site_section, site_section_builder in self.site_section_builders.items():
            site_section_builder.build()
        
        self.site_index_builder.build()

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
        # self.target_store = None # FIXME: Need better test fixtures. And to Implement the HtmlWriteOnlyStore class.
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
                "module_name" : "great_expectations.render.view",
                "class_name" : "DefaultJinjaIndexPageView",
            }
        view_module = importlib.import_module(view.pop("module_name"))
        self.view_class = getattr(view_module, view.pop("class_name"))


    def build(self):
        for resource_key in self.source_store.list_keys():

            if self.run_id_filter:
                if not self._resource_key_passes_run_id_filter(resource_key):
                    continue

            resource = self.source_store.get(resource_key)

            # TODO : This will need to change slightly when renderer and view classes are configurable.
            # TODO : Typing resources is SUPER important for usability now that we're slapping configurable renders together with arbitrary stores.
            rendered_content = self.renderer_class().render(resource)
            viewable_content = self.view_class().render(rendered_content)

            self.target_store.set(
                SiteSectionIdentifier(
                    site_section_name=self.name,
                    resource_identifier=resource_key,
                ),
                viewable_content
            )

            #Where do index_link_dicts live?
            # In the HtmlSiteStore!
    
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


    def build(self):
        # Loop over sections in the HtmlStore
        logger.debug("DefaultSiteIndexBuilder.build")

        resource_keys = self.target_store.list_keys()

        # FIXME : There were no tests to verify that content is created or correct,
        # so it's not my job to re-implement, right?
        # rendered_content = self.renderer_class.render(resource_keys)
        # viewable_content = self.view_class.render(rendered_content)
        viewable_content = ""

        self.target_store.write_index_page(
            viewable_content
        )