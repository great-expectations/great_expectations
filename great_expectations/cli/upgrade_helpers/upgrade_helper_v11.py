import datetime
import os

from dateutil.parser import parse, ParserError

from great_expectations import DataContext
from great_expectations.cli.util import cli_message
from great_expectations.data_context.store import ValidationsStore, HtmlSiteStore, TupleFilesystemStoreBackend, \
    TupleS3StoreBackend, TupleGCSStoreBackend, MetricStore, DatabaseStoreBackend, InMemoryStoreBackend
from great_expectations.data_context.types.resource_identifiers import ValidationResultIdentifier


class UpgradeHelperV11:
    def __init__(self, data_context=None, context_root_dir=None):
        assert data_context or context_root_dir, "Please provide a data_context object or a context_root_dir."

        self.data_context = data_context or DataContext(context_root_dir=context_root_dir)

        self.upgrade_log = {
            "skipped_validations_stores": {
                "database_store_backends": [],
                "unsupported": []
            },
            "skipped_docs_validations_stores": {
                "unsupported": []
            },
            "skipped_metrics_stores": {
                "database_store_backends": [],
                "unsupported": []
            },
            "upgraded_validations_stores": {},
            "upgraded_docs_site_validations_stores": {},
        }

        self.upgrade_checklist = {
            "validations_store_backends": {},
            "docs_validations_store_backends": {}
        }

        self.validation_run_times = {}

        self.run_time_setters_by_backend_type = {
            TupleFilesystemStoreBackend: self._get_tuple_filesystem_store_backend_run_time,
            TupleS3StoreBackend: self._get_tuple_s3_store_backend_run_time,
            TupleGCSStoreBackend: self._get_tuple_gcs_store_backend_run_time
        }

        self.run_id_updaters_by_backend_type = {
            TupleFilesystemStoreBackend: self._update_tuple_filesystem_store_backend_run_id,
            TupleS3StoreBackend: self._update_tuple_s3_store_backend_run_id,
            TupleGCSStoreBackend: self._update_tuple_gcs_store_backend_run_id
        }

    def _generate_upgrade_checklist(self):
        for (store_name, store) in self.data_context.stores.items():
            if not isinstance(store, (ValidationsStore, MetricStore)):
                continue
            elif isinstance(store, ValidationsStore):
                self._process_validations_store_for_checklist(store_name, store)
            elif isinstance(store, MetricStore):
                self._process_metrics_store_for_checklist(store_name, store)

        sites = self.data_context._project_config_with_variables_substituted.data_docs_sites

        if sites:
            for site_name, site_config in sites.items():
                self._process_docs_site_for_checklist(site_name, site_config)

    def _process_docs_site_for_checklist(self, site_name, site_config):
        site_html_store = HtmlSiteStore(
            store_backend=site_config.get("store_backend"),
            runtime_environment={
                "data_context": self.data_context,
                "root_directory": self.data_context.root_directory,
                "site_name": site_name
            }
        )
        site_validations_store_backend = site_html_store.store_backends[ValidationResultIdentifier]

        if isinstance(site_validations_store_backend, tuple(list(self.run_time_setters_by_backend_type.keys()))):
            self.upgrade_checklist["docs_validations_store_backends"][site_name] = site_validations_store_backend
        else:
            self.upgrade_log["skipped_docs_validations_stores"]["unsupported"].append({
                "site_name": site_name,
                "validations_store_backend_class": type(site_validations_store_backend).__name__
            })

    def _process_validations_store_for_checklist(self, store_name, store):
        store_backend = store.store_backend
        if isinstance(store_backend, DatabaseStoreBackend):
            self.upgrade_log["skipped_validations_stores"]["database_store_backends"].append({
                "store_name": store_name,
                "store_backend_class": type(store_backend).__name__
            })
        elif isinstance(store_backend, tuple(list(self.run_time_setters_by_backend_type.keys()))):
            self.upgrade_checklist["validations_store_backends"][store_name] = store_backend
        else:
            self.upgrade_log["skipped_validations_stores"]["unsupported"].append({
                "store_name": store_name,
                "store_backend_class": type(store_backend).__name__
            })

    def _process_metrics_store_for_checklist(self, store_name, store):
        store_backend = store.store_backend
        if isinstance(store_backend, DatabaseStoreBackend):
            self.upgrade_log["skipped_metrics_stores"]["database_store_backends"].append({
                "store_name": store_name,
                "store_backend_class": type(store_backend).__name__
            })
        elif isinstance(store_backend, InMemoryStoreBackend):
            pass
        else:
            self.upgrade_log["skipped_metrics_stores"]["unsupported"].append({
                "store_name": store_name,
                "store_backend_class": type(store_backend).__name__
            })

    # TODO: add logic that changes old run_id to typed run_id in validation json files
    def upgrade_store_backend(self, store_backend):
        validation_source_keys = store_backend.list_keys()

        for source_key in validation_source_keys:
            run_name = source_key[-2]
            if run_name not in self.validation_run_times:
                self.run_time_setters_by_backend_type.get(type(store_backend))(source_key, store_backend)
            dest_key_list = list(source_key)
            dest_key_list.insert(-1, self.validation_run_times[run_name])
            dest_key = tuple(dest_key_list)
            store_backend.move(source_key, dest_key)

    def _update_tuple_filesystem_store_backend_run_id(self):
        pass

    def _update_tuple_s3_store_backend_run_id(self):
        pass

    def _update_tuple_gcs_store_backend_run_id(self):
        pass

    def _get_tuple_filesystem_store_backend_run_time(self, source_key, store_backend):
        run_name = source_key[-2]
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

    def _get_tuple_s3_store_backend_run_time(self, source_key, store_backend):
        import boto3
        s3 = boto3.resource('s3')
        run_name = source_key[-2]

        try:
            self.validation_run_times[run_name] = parse(run_name).isoformat()
        except ParserError:
            source_path = store_backend._convert_key_to_filepath(source_key)
            if not source_path.startswith(store_backend.prefix):
                source_path = os.path.join(store_backend.prefix, source_path)
            source_object = s3.Object(store_backend.bucket, source_path)
            source_object_last_mod = source_object.last_modified.isoformat()

            self.validation_run_times[run_name] = source_object_last_mod

    def _get_tuple_gcs_store_backend_run_time(self, source_key, store_backend):
        from google.cloud import storage
        gcs = storage.Client(project=store_backend.project)
        bucket = gcs.get_bucket(store_backend.bucket)
        run_name = source_key[-2]

        try:
            self.validation_run_times[run_name] = parse(run_name).isoformat()
        except ParserError:
            source_path = store_backend._convert_key_to_filepath(source_key)
            if not source_path.startswith(store_backend.prefix):
                source_path = os.path.join(store_backend.prefix, source_path)
            source_blob_created_time = bucket.get_blob(source_path).time_created.isoformat()

            self.validation_run_times[run_name] = source_blob_created_time

    def _generate_upgrade_prompt(self):
        stores_with_database_backends = (self.upgrade_log["skipped_validations_stores"]["database_store_backends"] +
                                         self.upgrade_log["skipped_metrics_stores"]["database_store_backends"])
        unsupported_stores = (self.upgrade_log["skipped_validations_stores"]["unsupported"] +
                              self.upgrade_log["skipped_metrics_stores"]["unsupported"])
        unsupported_doc_sites = self.upgrade_log["skipped_docs_validations_stores"]["unsupported"]

    def upgrade_project(self):
        for (store_name, store_backend) in self.upgrade_checklist["validations_store_backends"].items():
            self.upgrade_store_backend(store_backend)
        for (site_name, store_backend) in self.upgrade_checklist["docs_validations_store_backends"].items():
            self.upgrade_store_backend(store_backend)
