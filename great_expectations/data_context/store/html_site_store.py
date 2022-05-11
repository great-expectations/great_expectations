import logging
import os
import re
import tempfile
from mimetypes import guess_type
from zipfile import ZipFile, is_zipfile

from great_expectations.core.data_context_key import DataContextKey
from great_expectations.data_context.store.ge_cloud_store_backend import (
    GeCloudStoreBackend,
)
from great_expectations.data_context.store.tuple_store_backend import TupleStoreBackend
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    SiteSectionIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.data_context.util import (
    file_relative_path,
    instantiate_class_from_config,
    load_class,
)
from great_expectations.exceptions import ClassInstantiationError, DataContextError
from great_expectations.util import (
    filter_properties_dict,
    verify_dynamic_loading_support,
)

logger = logging.getLogger(__name__)


class HtmlSiteStore:
    "\n    A HtmlSiteStore facilitates publishing rendered documentation built from Expectation Suites, Profiling Results, and Validation Results.\n\n    --ge-feature-maturity-info--\n\n        id: html_site_store_filesystem\n        title: HTML Site Store - Filesystem\n        icon:\n        short_description: DataDocs on Filesystem\n        description: For publishing rendered documentation built from Expectation Suites, Profiling Results, and Validation Results on the Filesystem\n        how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/configuring_data_docs/how_to_host_and_share_data_docs_on_a_filesystem.html\n        maturity: Production\n        maturity_details:\n            api_stability: Mostly Stable (profiling)\n            implementation_completeness: Complete\n            unit_test_coverage: Complete\n            integration_infrastructure_test_coverage: N/A\n            documentation_completeness: Partial\n            bug_risk: Low\n\n        id: html_site_store_s3\n        title: HTML Site Store - S3\n        icon:\n        short_description: DataDocs on S3\n        description: For publishing rendered documentation built from Expectation Suites, Profiling Results, and Validation Results on S3\n        how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/configuring_data_docs/how_to_host_and_share_data_docs_on_s3.html\n        maturity: Beta\n        maturity_details:\n            api_stability: Mostly Stable (profiling)\n            implementation_completeness: Complete\n            unit_test_coverage: Complete\n            integration_infrastructure_test_coverage: Minimal\n            documentation_completeness: Complete\n            bug_risk: Moderate\n\n        id: html_site_store_gcs\n        title: HTMLSiteStore - GCS\n        icon:\n        short_description: DataDocs on GCS\n        description: For publishing rendered documentation built from Expectation Suites, Profiling Results, and Validation Results on GCS\n        how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.html\n        maturity: Beta\n        maturity_details:\n            api_stability: Mostly Stable (profiling)\n            implementation_completeness: Complete\n            unit_test_coverage: Complete\n            integration_infrastructure_test_coverage: Minimal\n            documentation_completeness: Partial (needs auth)\n            bug_risk: Moderate (resource URL may have bugs)\n\n        id: html_site_store_azure_blob_storage\n        title: HTMLSiteStore - Azure\n        icon:\n        short_description: DataDocs on Azure Blob Storage\n        description: For publishing rendered documentation built from Expectation Suites, Profiling Results, and Validation Results on Azure Blob Storage\n        how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/configuring_data_docs/how_to_host_and_share_data_docs_on_azure_blob_storage.html\n        maturity: N/A\n        maturity_details:\n            api_stability: Mostly Stable (profiling)\n            implementation_completeness: Minimal\n            unit_test_coverage: Minimal\n            integration_infrastructure_test_coverage: Minimal\n            documentation_completeness: Minimal\n            bug_risk: Moderate\n\n    --ge-feature-maturity-info--\n"
    _key_class = SiteSectionIdentifier

    def __init__(self, store_backend=None, runtime_environment=None) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        store_backend_module_name = store_backend.get(
            "module_name", "great_expectations.data_context.store"
        )
        store_backend_class_name = store_backend.get(
            "class_name", "TupleFilesystemStoreBackend"
        )
        verify_dynamic_loading_support(module_name=store_backend_module_name)
        store_class = load_class(store_backend_class_name, store_backend_module_name)
        if not issubclass(store_class, (TupleStoreBackend, GeCloudStoreBackend)):
            raise DataContextError(
                "Invalid configuration: HtmlSiteStore needs a TupleStoreBackend or GeCloudStoreBackend"
            )
        if ("filepath_template" in store_backend) or (
            ("fixed_length_key" in store_backend)
            and (store_backend["fixed_length_key"] is True)
        ):
            logger.warning(
                "Configuring a filepath_template or using fixed_length_key is not supported in SiteBuilder: filepaths will be selected based on the type of asset rendered."
            )
        module_name = "great_expectations.data_context.store"
        filepath_suffix = ".html"
        is_ge_cloud_store = store_backend["class_name"] == "GeCloudStoreBackend"
        expectation_config_defaults = {
            "module_name": module_name,
            "filepath_prefix": "expectations",
            "filepath_suffix": filepath_suffix,
            "suppress_store_backend_id": True,
        }
        if is_ge_cloud_store:
            expectation_config_defaults = {
                "module_name": module_name,
                "suppress_store_backend_id": True,
            }
        expectation_suite_identifier_obj = instantiate_class_from_config(
            config=store_backend,
            runtime_environment=runtime_environment,
            config_defaults=expectation_config_defaults,
        )
        if not expectation_suite_identifier_obj:
            raise ClassInstantiationError(
                module_name=module_name,
                package_name=None,
                class_name=store_backend["class_name"],
            )
        validation_result_config_defaults = {
            "module_name": module_name,
            "filepath_prefix": "validations",
            "filepath_suffix": filepath_suffix,
            "suppress_store_backend_id": True,
        }
        if is_ge_cloud_store:
            validation_result_config_defaults = {
                "module_name": module_name,
                "suppress_store_backend_id": True,
            }
        validation_result_idendifier_obj = instantiate_class_from_config(
            config=store_backend,
            runtime_environment=runtime_environment,
            config_defaults=validation_result_config_defaults,
        )
        if not validation_result_idendifier_obj:
            raise ClassInstantiationError(
                module_name=module_name,
                package_name=None,
                class_name=store_backend["class_name"],
            )
        filepath_template = "index.html"
        index_page_config_defaults = {
            "module_name": module_name,
            "filepath_template": filepath_template,
            "suppress_store_backend_id": True,
        }
        if is_ge_cloud_store:
            index_page_config_defaults = {
                "module_name": module_name,
                "suppress_store_backend_id": True,
            }
        index_page_obj = instantiate_class_from_config(
            config=store_backend,
            runtime_environment=runtime_environment,
            config_defaults=index_page_config_defaults,
        )
        if not index_page_obj:
            raise ClassInstantiationError(
                module_name=module_name,
                package_name=None,
                class_name=store_backend["class_name"],
            )
        static_assets_config_defaults = {
            "module_name": module_name,
            "filepath_template": None,
            "suppress_store_backend_id": True,
        }
        if is_ge_cloud_store:
            static_assets_config_defaults = {
                "module_name": module_name,
                "suppress_store_backend_id": True,
            }
        static_assets_obj = instantiate_class_from_config(
            config=store_backend,
            runtime_environment=runtime_environment,
            config_defaults=static_assets_config_defaults,
        )
        if not static_assets_obj:
            raise ClassInstantiationError(
                module_name=module_name,
                package_name=None,
                class_name=store_backend["class_name"],
            )
        self.store_backends = {
            ExpectationSuiteIdentifier: expectation_suite_identifier_obj,
            ValidationResultIdentifier: validation_result_idendifier_obj,
            "index_page": index_page_obj,
            "static_assets": static_assets_obj,
        }
        self.keys = set()
        self._config = {
            "store_backend": store_backend,
            "runtime_environment": runtime_environment,
            "module_name": self.__class__.__module__,
            "class_name": self.__class__.__name__,
        }
        filter_properties_dict(properties=self._config, clean_falsy=True, inplace=True)

    def get(self, key):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        self._validate_key(key)
        return self.store_backends[type(key.resource_identifier)].get(key.to_tuple())

    def set(self, key, serialized_value):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        self._validate_key(key)
        self.keys.add(key)
        return self.store_backends[type(key.resource_identifier)].set(
            key.resource_identifier.to_tuple(),
            serialized_value,
            content_encoding="utf-8",
            content_type="text/html; charset=utf-8",
        )

    def get_url_for_resource(self, resource_identifier=None, only_if_exists=True):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Return the URL of the HTML document that renders a resource\n        (e.g., an expectation suite or a validation result).\n\n        :param resource_identifier: ExpectationSuiteIdentifier, ValidationResultIdentifier\n                or any other type's identifier. The argument is optional - when\n                not supplied, the method returns the URL of the index page.\n        :return: URL (string)\n        "
        if resource_identifier is None:
            store_backend = self.store_backends["index_page"]
            key = ()
        elif isinstance(resource_identifier, ExpectationSuiteIdentifier):
            store_backend = self.store_backends[ExpectationSuiteIdentifier]
            key = resource_identifier.to_tuple()
        elif isinstance(resource_identifier, ValidationResultIdentifier):
            store_backend = self.store_backends[ValidationResultIdentifier]
            key = resource_identifier.to_tuple()
        else:
            raise ValueError(
                f"Cannot get URL for resource {str(resource_identifier):s}"
            )
        if store_backend.base_public_path:
            if only_if_exists:
                return (
                    store_backend.get_public_url_for_key(key)
                    if store_backend.has_key(key)
                    else None
                )
            else:
                return store_backend.get_public_url_for_key(key)
        elif only_if_exists:
            return (
                store_backend.get_url_for_key(key)
                if store_backend.has_key(key)
                else None
            )
        else:
            return store_backend.get_url_for_key(key)

    def _validate_key(self, key):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if not isinstance(key, SiteSectionIdentifier):
            raise TypeError(
                f"key: {key!r} must be a SiteSectionIdentifier, not {type(key)!r}"
            )
        for key_class in self.store_backends.keys():
            try:
                if isinstance(key.resource_identifier, key_class):
                    return
            except TypeError:
                continue
        raise TypeError(
            "resource_identifier in key: {!r} must one of {}, not {!r}".format(
                key, set(self.store_backends.keys()), type(key)
            )
        )

    def list_keys(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        keys = []
        for (type_, backend) in self.store_backends.items():
            try:
                key_tuples = backend.list_keys()
            except NotImplementedError:
                pass
            try:
                if issubclass(type_, DataContextKey):
                    keys += [type_.from_tuple(tuple_) for tuple_ in key_tuples]
            except TypeError:
                pass
        return keys

    def write_index_page(self, page):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "This third param_store has a special method, which uses a zero-length tuple as a key."
        return self.store_backends["index_page"].set(
            (), page, content_encoding="utf-8", content_type="text/html; charset=utf-8"
        )

    def clean_site(self) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        for (_, target_store_backend) in self.store_backends.items():
            keys = target_store_backend.list_keys()
            for key in keys:
                target_store_backend.remove_key(key)

    def copy_static_assets(self, static_assets_source_dir=None):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        '\n        Copies static assets, using a special "static_assets" backend store that accepts variable-length tuples as\n        keys, with no filepath_template.\n        '
        file_exclusions = [".DS_Store"]
        dir_exclusions = []
        if not static_assets_source_dir:
            static_assets_source_dir = file_relative_path(
                __file__, os.path.join("..", "..", "render", "view", "static")
            )
        if ".zip" in static_assets_source_dir.lower():
            unzip_destdir = tempfile.mkdtemp()
            unzipped_ok = self._unzip_assets(static_assets_source_dir, unzip_destdir)
            if unzipped_ok:
                return self.copy_static_assets(unzip_destdir)
        for item in os.listdir(static_assets_source_dir):
            if os.path.isdir(os.path.join(static_assets_source_dir, item)):
                if item in dir_exclusions:
                    continue
                new_source_dir = os.path.join(static_assets_source_dir, item)
                self.copy_static_assets(new_source_dir)
            else:
                if item in file_exclusions:
                    continue
                source_name = os.path.join(static_assets_source_dir, item)
                with open(source_name, "rb") as f:
                    store_key = tuple(os.path.normpath(source_name).split(os.sep))
                    store_key = store_key[store_key.index("static") :]
                    (content_type, content_encoding) = guess_type(item, strict=False)
                    if content_type is None:
                        if source_name.endswith(".otf"):
                            content_type = "font/opentype"
                        else:
                            logger.warning(
                                "Unable to automatically determine content_type for {}".format(
                                    source_name
                                )
                            )
                            content_type = "text/html; charset=utf8"
                    if not isinstance(
                        self.store_backends["static_assets"], GeCloudStoreBackend
                    ):
                        self.store_backends["static_assets"].set(
                            store_key,
                            f.read(),
                            content_encoding=content_encoding,
                            content_type=content_type,
                        )

    def _unzip_assets(self, assets_full_path: str, unzip_directory: str) -> bool:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        '\n        This function receives an `assets_full_path` parameter,\n        (e.g. "/home/joe/libs/my_python_libs.zip/great_expectations/render/view/static")\n        and an `unzip_directory` parameter (e.g. "/tmp/extract_statics_here")\n\n        If `assets_full_path` is a folder inside a zip, then said folder is extracted\n        (unzipped) to the `unzip_directory` and this function returns True.\n        Otherwise, this function returns False\n        '
        static_assets_source_absdir = os.path.abspath(assets_full_path)
        zip_re = re.match(
            f"(.+[.]zip){re.escape(os.sep)}(.+)",
            static_assets_source_absdir,
            flags=re.IGNORECASE,
        )
        if zip_re:
            zip_filename = zip_re.groups()[0]
            path_in_zip = zip_re.groups()[1]
            if is_zipfile(zip_filename):
                with ZipFile(zip_filename) as zipfile:
                    static_files_to_extract = [
                        file
                        for file in zipfile.namelist()
                        if file.startswith(path_in_zip)
                    ]
                    zipfile.extractall(unzip_directory, static_files_to_extract)
                return True
        return False

    @property
    def config(self) -> dict:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._config

    def self_check(self, pretty_print: bool = True) -> dict:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        report_object = self._config
        return report_object
