import datetime
import os

from dateutil.parser import parse, ParserError

from great_expectations import DataContext
from great_expectations.data_context.store import ValidationsStore, HtmlSiteStore, TupleFilesystemStoreBackend
from great_expectations.data_context.types.resource_identifiers import ValidationResultIdentifier


class MigratorV11:
    def __init__(self, data_context=None, context_root_dir=None):
        assert data_context or context_root_dir, "Please provide a data_context object or a context_root_dir."

        self.data_context = data_context or DataContext(context_root_dir=context_root_dir)
        self.validations_store_backends = {
            store_name: store.store_backend for (store_name, store) in self.data_context.stores.items()
            if isinstance(store, ValidationsStore)
        }
        self.docs_validations_store_backends = {}
        self.validation_run_times = {}

        sites = self.data_context._project_config_with_variables_substituted.data_docs_sites

        if sites:
            for site_name, site_config in sites.items():
                site_html_store = HtmlSiteStore(
                    store_backend=site_config.get("store_backend"),
                    runtime_environment={
                        "data_context": self.data_context,
                        "root_directory": self.data_context.root_directory,
                        "site_name": site_name
                    }
                )
                site_validations_store_backend = site_html_store.store_backends[ValidationResultIdentifier]
                self.docs_validations_store_backends[site_name] = site_validations_store_backend

        self.migrators_by_backend_type = {
            TupleFilesystemStoreBackend: self.migrate_tuple_filesystem_store_backend
        }

    def migrate_tuple_filesystem_store_backend(self, store_backend):
        validation_source_keys = store_backend.list_keys()

        for source_key in validation_source_keys:
            run_name = source_key[-2]
            if run_name not in self.validation_run_times:
                try:
                    self.validation_run_times[run_name] = parse(run_name).isoformat()
                except ParserError:
                    source_path = os.path.join(
                        store_backend.full_base_directory,
                        store_backend._convert_key_to_filepath(source_key)
                    )
                    path_mod_timestamp = os.path.getmtime(source_path)
                    path_mod_iso_str = datetime.datetime.fromtimestamp(
                        path_mod_timestamp,
                        tz=datetime.timezone.utc
                    ).isoformat()
                    self.validation_run_times[run_name] = path_mod_iso_str
            dest_key_list = list(source_key)
            dest_key_list.insert(-1, self.validation_run_times[run_name])
            dest_key = tuple(dest_key_list)
            store_backend.move(source_key, dest_key)

    def migrate_project(self):
        for (store_name, store) in self.validations_store_backends.items():
            self.migrators_by_backend_type.get(type(store))(store)
        for (site_name, store) in self.docs_validations_store_backends.items():
            self.migrators_by_backend_type.get(type(store))(store)
