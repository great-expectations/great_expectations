"""Example usage stats events.

This file contains example events primarily for usage stats schema validation testing.
By splitting up these events into a separate file, the hope is we can organize them better here and separate
them from tests so that test files are less verbose.
"""


# data_context.__init__ events:
data_context_init_with_dependencies: dict = {
    "event_payload": {
        "platform.system": "Darwin",
        "platform.release": "20.6.0",
        "version_info": "sys.version_info(major=3, minor=8, micro=12, releaselevel='final', serial=0)",
        "anonymized_datasources": [
            {
                "anonymized_name": "19e4cfca480e1f10cc49a4bcef258ea5",
                "parent_class": "Datasource",
                "anonymized_execution_engine": {
                    "anonymized_name": "ea87ef99e2cf6c0df5488767d67e0e2b",
                    "parent_class": "PandasExecutionEngine",
                },
                "anonymized_data_connectors": [
                    {
                        "anonymized_name": "dda8de63d0a83ec358e00cfaf75de7af",
                        "parent_class": "ConfiguredAssetFilesystemDataConnector",
                    }
                ],
            }
        ],
        "anonymized_stores": [
            {
                "anonymized_name": "c224436ff614123dcdc3f8b56089b508",
                "parent_class": "ExpectationsStore",
                "anonymized_store_backend": {
                    "parent_class": "TupleFilesystemStoreBackend"
                },
            },
            {
                "anonymized_name": "588eeebee0d4fdf40bb876f057002d44",
                "parent_class": "ValidationsStore",
                "anonymized_store_backend": {
                    "parent_class": "TupleFilesystemStoreBackend"
                },
            },
            {
                "anonymized_name": "0d7a9326836709e7870ab36e6e1d2db9",
                "parent_class": "EvaluationParameterStore",
                "anonymized_store_backend": {"parent_class": "InMemoryStoreBackend"},
            },
            {
                "anonymized_name": "89e8ce4ad87b2943a95e9507b7183d7a",
                "parent_class": "CheckpointStore",
                "anonymized_store_backend": {
                    "parent_class": "TupleFilesystemStoreBackend"
                },
            },
            {
                "anonymized_name": "67279fd8414944921e2a8413c01df67a",
                "parent_class": "ProfilerStore",
                "anonymized_store_backend": {
                    "parent_class": "TupleFilesystemStoreBackend"
                },
            },
        ],
        "anonymized_validation_operators": [],
        "anonymized_data_docs_sites": [
            {
                "parent_class": "SiteBuilder",
                "anonymized_name": "ad449532a0f1edfef658dc46ba473ed6",
                "anonymized_store_backend": {
                    "parent_class": "TupleFilesystemStoreBackend"
                },
                "anonymized_site_index_builder": {
                    "parent_class": "DefaultSiteIndexBuilder"
                },
            }
        ],
        "anonymized_expectation_suites": [],
        "dependencies": [
            {
                "install_environment": "required",
                "package_name": "Click",
                "installed": False,
                "version": None,
            },
            {
                "install_environment": "required",
                "package_name": "Ipython",
                "installed": False,
                "version": None,
            },
            {
                "install_environment": "required",
                "package_name": "altair",
                "installed": True,
                "version": "4.2.0",
            },
            {
                "install_environment": "required",
                "package_name": "colorama",
                "installed": True,
                "version": "0.4.4",
            },
            {
                "install_environment": "required",
                "package_name": "cryptography",
                "installed": True,
                "version": "3.4.8",
            },
            {
                "install_environment": "required",
                "package_name": "dataclasses",
                "installed": False,
                "version": None,
            },
            {
                "install_environment": "required",
                "package_name": "importlib-metadata",
                "installed": True,
                "version": "4.11.3",
            },
            {
                "install_environment": "required",
                "package_name": "jinja2",
                "installed": False,
                "version": None,
            },
            {
                "install_environment": "required",
                "package_name": "jsonpatch",
                "installed": True,
                "version": "1.32",
            },
            {
                "install_environment": "required",
                "package_name": "jsonschema",
                "installed": True,
                "version": "3.2.0",
            },
            {
                "install_environment": "required",
                "package_name": "mistune",
                "installed": True,
                "version": "0.8.4",
            },
            {
                "install_environment": "required",
                "package_name": "nbformat",
                "installed": True,
                "version": "5.2.0",
            },
            {
                "install_environment": "required",
                "package_name": "numpy",
                "installed": True,
                "version": "1.22.3",
            },
            {
                "install_environment": "required",
                "package_name": "packaging",
                "installed": True,
                "version": "21.3",
            },
            {
                "install_environment": "required",
                "package_name": "pandas",
                "installed": True,
                "version": "1.4.1",
            },
            {
                "install_environment": "required",
                "package_name": "pyparsing",
                "installed": True,
                "version": "2.4.7",
            },
            {
                "install_environment": "required",
                "package_name": "python-dateutil",
                "installed": True,
                "version": "2.8.2",
            },
            {
                "install_environment": "required",
                "package_name": "pytz",
                "installed": True,
                "version": "2021.3",
            },
            {
                "install_environment": "required",
                "package_name": "requests",
                "installed": True,
                "version": "2.27.1",
            },
            {
                "install_environment": "required",
                "package_name": "ruamel.yaml",
                "installed": True,
                "version": "0.17.17",
            },
            {
                "install_environment": "required",
                "package_name": "scipy",
                "installed": True,
                "version": "1.8.0",
            },
            {
                "install_environment": "required",
                "package_name": "tqdm",
                "installed": True,
                "version": "4.63.0",
            },
            {
                "install_environment": "required",
                "package_name": "typing-extensions",
                "installed": False,
                "version": None,
            },
            {
                "install_environment": "required",
                "package_name": "tzlocal",
                "installed": True,
                "version": "4.1",
            },
            {
                "install_environment": "required",
                "package_name": "urllib3",
                "installed": True,
                "version": "1.26.9",
            },
            {
                "install_environment": "dev",
                "package_name": "teradatasqlalchemy",
                "installed": True,
                "version": "17.0.0.1",
            },
            {
                "install_environment": "dev",
                "package_name": "psycopg2-binary",
                "installed": True,
                "version": "2.9.3",
            },
            {
                "install_environment": "dev",
                "package_name": "sqlalchemy",
                "installed": False,
                "version": None,
            },
            {
                "install_environment": "dev",
                "package_name": "snowflake-sqlalchemy",
                "installed": True,
                "version": "1.3.3",
            },
            {
                "install_environment": "dev",
                "package_name": "sqlalchemy-dremio",
                "installed": True,
                "version": "1.2.1",
            },
            {
                "install_environment": "dev",
                "package_name": "xlrd",
                "installed": True,
                "version": "1.2.0",
            },
            {
                "install_environment": "dev",
                "package_name": "PyMySQL",
                "installed": True,
                "version": "0.9.3",
            },
            {
                "install_environment": "dev",
                "package_name": "sqlalchemy-bigquery",
                "installed": True,
                "version": "1.4.1",
            },
            {
                "install_environment": "dev",
                "package_name": "boto3",
                "installed": True,
                "version": "1.17.106",
            },
            {
                "install_environment": "dev",
                "package_name": "sqlalchemy-redshift",
                "installed": True,
                "version": "0.8.9",
            },
            {
                "install_environment": "dev",
                "package_name": "azure-keyvault-secrets",
                "installed": True,
                "version": "4.3.0",
            },
            {
                "install_environment": "dev",
                "package_name": "pyspark",
                "installed": True,
                "version": "3.1.2",
            },
            {
                "install_environment": "dev",
                "package_name": "azure-identity",
                "installed": True,
                "version": "1.8.0",
            },
            {
                "install_environment": "dev",
                "package_name": "google-cloud-storage",
                "installed": True,
                "version": "2.2.1",
            },
            {
                "install_environment": "dev",
                "package_name": "pyodbc",
                "installed": True,
                "version": "4.0.32",
            },
            {
                "install_environment": "dev",
                "package_name": "pypd",
                "installed": True,
                "version": "1.1.0",
            },
            {
                "install_environment": "dev",
                "package_name": "azure-storage-blob",
                "installed": True,
                "version": "12.10.0",
            },
            {
                "install_environment": "dev",
                "package_name": "gcsfs",
                "installed": True,
                "version": "2022.2.0",
            },
            {
                "install_environment": "dev",
                "package_name": "snowflake-connector-python",
                "installed": True,
                "version": "2.5.0",
            },
            {
                "install_environment": "dev",
                "package_name": "openpyxl",
                "installed": True,
                "version": "3.0.9",
            },
            {
                "install_environment": "dev",
                "package_name": "pyathena",
                "installed": True,
                "version": "2.5.1",
            },
            {
                "install_environment": "dev",
                "package_name": "feather-format",
                "installed": True,
                "version": "0.4.1",
            },
            {
                "install_environment": "dev",
                "package_name": "google-cloud-secret-manager",
                "installed": True,
                "version": "2.9.2",
            },
            {
                "install_environment": "dev",
                "package_name": "pyarrow",
                "installed": True,
                "version": "7.0.0",
            },
        ],
    },
    "event": "data_context.__init__",
    "success": True,
    "version": "1.0.0",
    "event_time": "2022-03-29T14:22:53.921Z",
    "data_context_id": "2edf8eba-7de4-46fb-80b0-b8b7f7c7cd24",
    "data_context_instance_id": "cc57338c-44fc-4133-aed1-c7f5f8c577ac",
    "ge_version": "0.14.12+27.g7774964f6.dirty",
}
