from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import great_expectations.exceptions as gx_exceptions
from great_expectations.data_context.util import instantiate_class_from_config

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )
    from great_expectations.render.renderer.site_builder import SiteBuilder

logger = logging.getLogger(__name__)


class DataDocsManager:
    def __init__(
        self,
        data_docs_sites: dict | None,
        root_directory: str | None,
        context: AbstractDataContext,
    ):
        self._sites = data_docs_sites or {}
        self._root_directory = root_directory
        self._context = context

        from great_expectations.data_context import CloudDataContext

        self._cloud_mode = isinstance(context, CloudDataContext)

    def build_data_docs(
        self,
        site_names: list[str] | None = None,
        resource_identifiers: list | None = None,
        dry_run: bool = False,
        build_index: bool = True,
    ):
        index_page_locator_infos = {}

        sites = self._sites
        if sites:
            logger.debug("Found data_docs_sites. Building sites...")

            for site_name, site_config in sites.items():
                logger.debug(
                    f"Building Data Docs Site {site_name}",
                )

                if (site_names and (site_name in site_names)) or not site_names:
                    complete_site_config = site_config
                    module_name = "great_expectations.render.renderer.site_builder"
                    site_builder: SiteBuilder = (
                        self._init_site_builder_for_data_docs_site_creation(
                            site_name=site_name,
                            site_config=site_config,
                        )
                    )
                    if not site_builder:
                        raise gx_exceptions.ClassInstantiationError(
                            module_name=module_name,
                            package_name=None,
                            class_name=complete_site_config["class_name"],
                        )
                    if dry_run:
                        index_page_locator_infos[site_name] = (
                            site_builder.get_resource_url(only_if_exists=False)
                        )
                    else:
                        index_page_resource_identifier_tuple = site_builder.build(
                            resource_identifiers,
                            build_index=build_index,
                        )
                        if index_page_resource_identifier_tuple:
                            index_page_locator_infos[site_name] = (
                                index_page_resource_identifier_tuple[0]
                            )

        else:
            logger.debug("No data_docs_config found. No site(s) built.")

        return index_page_locator_infos

    def _init_site_builder_for_data_docs_site_creation(
        self,
        site_name: str,
        site_config: dict,
    ) -> SiteBuilder:
        site_builder: SiteBuilder = instantiate_class_from_config(
            config=site_config,
            runtime_environment={
                "data_context": self._context,
                "root_directory": self._root_directory,
                "site_name": site_name,
                "cloud_mode": self._cloud_mode,
            },
            config_defaults={
                "class_name": "SiteBuilder",
                "module_name": "great_expectations.render.renderer.site_builder",
            },
        )
        return site_builder

    def get_docs_sites_urls(
        self,
        resource_identifier=None,
        site_name: str | None = None,
        only_if_exists: bool = True,
        site_names: list[str] | None = None,
    ) -> list[dict[str, str]]:
        unfiltered_sites = self._sites

        # Filter out sites that are not in site_names
        sites = (
            {k: v for k, v in unfiltered_sites.items() if k in site_names}  # type: ignore[union-attr]
            if site_names
            else unfiltered_sites
        )

        if not sites:
            logger.debug("Found no data_docs_sites.")
            return []
        logger.debug(f"Found {len(sites)} data_docs_sites.")

        if site_name:
            if site_name not in sites.keys():
                raise gx_exceptions.DataContextError(
                    f"Could not find site named {site_name}. Please check your configurations"
                )
            site = sites[site_name]
            site_builder = self._load_site_builder_from_site_config(site)
            url = site_builder.get_resource_url(
                resource_identifier=resource_identifier, only_if_exists=only_if_exists
            )
            return [{"site_name": site_name, "site_url": url}]

        site_urls = []
        for _site_name, site_config in sites.items():
            site_builder = self._load_site_builder_from_site_config(site_config)
            url = site_builder.get_resource_url(
                resource_identifier=resource_identifier, only_if_exists=only_if_exists
            )
            site_urls.append({"site_name": _site_name, "site_url": url})

        return site_urls

    def _load_site_builder_from_site_config(self, site_config) -> SiteBuilder:
        default_module_name = "great_expectations.render.renderer.site_builder"
        site_builder = instantiate_class_from_config(
            config=site_config,
            runtime_environment={
                "data_context": self._context,
                "root_directory": self._root_directory,
            },
            config_defaults={"module_name": default_module_name},
        )
        if not site_builder:
            raise gx_exceptions.ClassInstantiationError(
                module_name=default_module_name,
                package_name=None,
                class_name=site_config["class_name"],
            )
        return site_builder

    def clean_data_docs(self, site_name: str | None = None) -> bool:
        data_docs_sites = self._sites
        if not data_docs_sites:
            raise gx_exceptions.DataContextError(
                "No data docs sites were found on this DataContext, therefore no sites will be cleaned.",
            )

        data_docs_site_names = list(data_docs_sites.keys())
        if site_name:
            if site_name not in data_docs_site_names:
                raise gx_exceptions.DataContextError(
                    f"The specified site name `{site_name}` does not exist in this project."
                )
            return self._clean_data_docs_site(site_name)

        cleaned = []
        for existing_site_name in data_docs_site_names:
            cleaned.append(self._clean_data_docs_site(existing_site_name))
        return all(cleaned)

    def _clean_data_docs_site(self, site_name: str) -> bool:
        sites = self._sites
        if not sites:
            return False
        site_config = sites.get(site_name)

        site_builder = instantiate_class_from_config(
            config=site_config,
            runtime_environment={
                "data_context": self._context,
                "root_directory": self._root_directory,
            },
            config_defaults={
                "module_name": "great_expectations.render.renderer.site_builder"
            },
        )
        site_builder.clean_site()
        return True

    def get_site_names(self) -> list[str]:
        return list(self._sites.keys())  # type: ignore[union-attr]
