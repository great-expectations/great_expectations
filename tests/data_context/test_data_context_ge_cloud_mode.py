import os
import shutil
from unittest import mock
from unittest.mock import PropertyMock, patch

import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context import DataContext
from great_expectations.data_context.util import file_relative_path
from great_expectations.exceptions import DataContextError


def test_data_context_ge_cloud_mode_with_runtime_cloud_config(
    ge_cloud_runtime_base_url,
    ge_cloud_runtime_account_id,
    ge_cloud_runtime_access_token,
    data_context_with_complete_global_config_in_dot_dir_only,
):
    context = DataContext(
        ge_cloud_mode=True,
        ge_cloud_base_url=ge_cloud_runtime_base_url,
        ge_cloud_account_id=ge_cloud_runtime_account_id,
        ge_cloud_access_token=ge_cloud_runtime_access_token,
    )
    global_usage_statistics_url = context._get_global_config_value(
        environment_variable="GE_USAGE_STATISTICS_URL",
        conf_file_section="anonymous_usage_statistics",
        conf_file_option="usage_statistics_url",
    )
    expected_ge_cloud_config = {
        "base_url": ge_cloud_runtime_base_url,
        "account_id": ge_cloud_runtime_account_id,
        "access_token": ge_cloud_runtime_access_token,
    }
    expected_project_config_with_variables_substituted = {
        "anonymous_usage_statistics": {
            "data_context_id": "a8a35168-68d5-4366-90ae-00647463d37e",
            "enabled": False,
            "usage_statistics_url": "https://dev.stats.greatexpectations.io/great_expectations/v1/usage_statistics"
            "/complete/version/1"
            if not global_usage_statistics_url
            else global_usage_statistics_url,
        },
        "checkpoint_store_name": "default_checkpoint_store",
        "config_variables_file_path": None,
        "config_version": 3.0,
        "data_docs_sites": {
            "default_site": {
                "class_name": "SiteBuilder",
                "show_how_to_buttons": True,
                "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
                "site_section_builders": {"profiling": "None"},
                "store_backend": {
                    "class_name": "GeCloudStoreBackend",
                    "ge_cloud_base_url": "https://api.dev.greatexpectations.io/runtime",
                    "ge_cloud_credentials": {
                        "access_token": "b17bc2539062410db0a30e28fb0ee930",
                        "account_id": "a8a35168-68d5-4366-90ae-00647463d37e",
                    },
                    "ge_cloud_resource_type": "rendered_data_doc",
                    "suppress_store_backend_id": True,
                },
            }
        },
        "datasources": {
            "default_pandas_datasource": {
                "class_name": "Datasource",
                "data_connectors": {
                    "default_runtime_data_connector": {
                        "batch_identifiers": None,
                        "class_name": "RuntimeDataConnector",
                    }
                },
                "execution_engine": {
                    "class_name": "PandasExecutionEngine",
                    "module_name": "great_expectations.execution_engine",
                },
                "module_name": "great_expectations.datasource",
            },
            "default_spark_datasource": {
                "class_name": "Datasource",
                "data_connectors": {
                    "default_runtime_data_connector": {
                        "batch_identifiers": None,
                        "class_name": "RuntimeDataConnector",
                    }
                },
                "execution_engine": {
                    "class_name": "SparkDFExecutionEngine",
                    "module_name": "great_expectations.execution_engine",
                },
                "module_name": "great_expectations.datasource",
            },
        },
        "evaluation_parameter_store_name": "default_evaluation_parameter_store",
        "expectations_store_name": "default_expectations_store",
        "notebooks": None,
        "plugins_directory": "/Users/foo/bar/my/plugins/directory/complete/version/1",
        "stores": {
            "default_checkpoint_store": {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "GeCloudStoreBackend",
                    "ge_cloud_base_url": "https://api.dev.greatexpectations.io/runtime",
                    "ge_cloud_credentials": {
                        "access_token": "b17bc2539062410db0a30e28fb0ee930",
                        "account_id": "a8a35168-68d5-4366-90ae-00647463d37e",
                    },
                    "ge_cloud_resource_type": "contract",
                    "suppress_store_backend_id": True,
                },
            },
            "default_evaluation_parameter_store": {
                "class_name": "EvaluationParameterStore"
            },
            "default_expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "GeCloudStoreBackend",
                    "ge_cloud_base_url": "https://api.dev.greatexpectations.io/runtime",
                    "ge_cloud_credentials": {
                        "access_token": "b17bc2539062410db0a30e28fb0ee930",
                        "account_id": "a8a35168-68d5-4366-90ae-00647463d37e",
                    },
                    "ge_cloud_resource_type": "expectation_suite",
                    "suppress_store_backend_id": True,
                },
            },
            "default_validations_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "GeCloudStoreBackend",
                    "ge_cloud_base_url": "https://api.dev.greatexpectations.io/runtime",
                    "ge_cloud_credentials": {
                        "access_token": "b17bc2539062410db0a30e28fb0ee930",
                        "account_id": "a8a35168-68d5-4366-90ae-00647463d37e",
                    },
                    "ge_cloud_resource_type": "suite_validation_result",
                    "suppress_store_backend_id": True,
                },
            },
        },
        "validations_store_name": "default_validations_store",
    }
    assert context.ge_cloud_config.to_json_dict() == expected_ge_cloud_config
    assert (
        context.project_config_with_variables_substituted.to_json_dict()
        == expected_project_config_with_variables_substituted
    )


def test_data_context_ge_cloud_mode_with_env_var_cloud_config(
    data_context_with_empty_global_config_dirs,
):
    ge_cloud_config_env_vars = {
        "GE_CLOUD_ACCOUNT_ID": "cef8f675-a10f-4fa9-86db-0789d9189dee",
        "GE_CLOUD_ACCESS_TOKEN": "61e88b44f91c4109b834e233821a2c59",
        "GE_CLOUD_BASE_URL": "https://my.env.var.base.url",
    }
    with mock.patch.dict(os.environ, ge_cloud_config_env_vars):
        context = DataContext(
            context_root_dir="/my/context/root/dir", ge_cloud_mode=True
        )
        global_usage_statistics_url = context._get_global_config_value(
            environment_variable="GE_USAGE_STATISTICS_URL",
            conf_file_section="anonymous_usage_statistics",
            conf_file_option="usage_statistics_url",
        )
        expected_ge_cloud_config = {
            key[9:].lower(): val for key, val in ge_cloud_config_env_vars.items()
        }
        expected_project_config_with_variables_substituted = {
            "expectations_store_name": "default_expectations_store",
            "evaluation_parameter_store_name": "default_evaluation_parameter_store",
            "datasources": {
                "default_spark_datasource": {
                    "data_connectors": {
                        "default_runtime_data_connector": {
                            "batch_identifiers": None,
                            "class_name": "RuntimeDataConnector",
                        }
                    },
                    "module_name": "great_expectations.datasource",
                    "class_name": "Datasource",
                    "execution_engine": {
                        "module_name": "great_expectations.execution_engine",
                        "class_name": "SparkDFExecutionEngine",
                    },
                },
                "default_pandas_datasource": {
                    "data_connectors": {
                        "default_runtime_data_connector": {
                            "batch_identifiers": None,
                            "class_name": "RuntimeDataConnector",
                        }
                    },
                    "module_name": "great_expectations.datasource",
                    "class_name": "Datasource",
                    "execution_engine": {
                        "module_name": "great_expectations.execution_engine",
                        "class_name": "PandasExecutionEngine",
                    },
                },
            },
            "config_variables_file_path": None,
            "anonymous_usage_statistics": {
                "enabled": False,
                "data_context_id": "cef8f675-a10f-4fa9-86db-0789d9189dee",
                "usage_statistics_url": "https://stats.greatexpectations.io/great_expectations/v1/usage_statistics"
                if not global_usage_statistics_url
                else global_usage_statistics_url,
            },
            "stores": {
                "default_evaluation_parameter_store": {
                    "class_name": "EvaluationParameterStore"
                },
                "default_expectations_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {
                        "class_name": "GeCloudStoreBackend",
                        "ge_cloud_base_url": "https://my.env.var.base.url",
                        "ge_cloud_resource_type": "expectation_suite",
                        "ge_cloud_credentials": {
                            "access_token": "61e88b44f91c4109b834e233821a2c59",
                            "account_id": "cef8f675-a10f-4fa9-86db-0789d9189dee",
                        },
                        "suppress_store_backend_id": True,
                    },
                },
                "default_validations_store": {
                    "class_name": "ValidationsStore",
                    "store_backend": {
                        "class_name": "GeCloudStoreBackend",
                        "ge_cloud_base_url": "https://my.env.var.base.url",
                        "ge_cloud_resource_type": "suite_validation_result",
                        "ge_cloud_credentials": {
                            "access_token": "61e88b44f91c4109b834e233821a2c59",
                            "account_id": "cef8f675-a10f-4fa9-86db-0789d9189dee",
                        },
                        "suppress_store_backend_id": True,
                    },
                },
                "default_checkpoint_store": {
                    "class_name": "CheckpointStore",
                    "store_backend": {
                        "class_name": "GeCloudStoreBackend",
                        "ge_cloud_base_url": "https://my.env.var.base.url",
                        "ge_cloud_resource_type": "contract",
                        "ge_cloud_credentials": {
                            "access_token": "61e88b44f91c4109b834e233821a2c59",
                            "account_id": "cef8f675-a10f-4fa9-86db-0789d9189dee",
                        },
                        "suppress_store_backend_id": True,
                    },
                },
            },
            "notebooks": None,
            "plugins_directory": "/my/context/root/dir/plugins/",
            "validations_store_name": "default_validations_store",
            "data_docs_sites": {
                "default_site": {
                    "class_name": "SiteBuilder",
                    "show_how_to_buttons": True,
                    "store_backend": {
                        "class_name": "GeCloudStoreBackend",
                        "ge_cloud_base_url": "https://my.env.var.base.url",
                        "ge_cloud_resource_type": "rendered_data_doc",
                        "ge_cloud_credentials": {
                            "access_token": "61e88b44f91c4109b834e233821a2c59",
                            "account_id": "cef8f675-a10f-4fa9-86db-0789d9189dee",
                        },
                        "suppress_store_backend_id": True,
                    },
                    "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
                    "site_section_builders": {"profiling": "None"},
                }
            },
            "checkpoint_store_name": "default_checkpoint_store",
            "config_version": 3.0,
        }
        assert context.ge_cloud_config.to_json_dict() == expected_ge_cloud_config
        assert (
            context.project_config_with_variables_substituted.to_json_dict()
            == expected_project_config_with_variables_substituted
        )


def test_data_context_ge_cloud_mode_with_global_config_in_dot_dir(
    data_context_with_complete_global_config_in_dot_dir_only,
):
    context = DataContext(ge_cloud_mode=True)
    global_usage_statistics_url = context._get_global_config_value(
        environment_variable="GE_USAGE_STATISTICS_URL",
        conf_file_section="anonymous_usage_statistics",
        conf_file_option="usage_statistics_url",
    )
    expected_ge_cloud_config = {
        "base_url": "https://api.dev.greatexpectations.io/complete/version-1",
        "account_id": "0ccac18e-7631-4bdd-8a42-3c35cce574c6",
        "access_token": "91bec65bf3fa41b99a98de6f2563eab0",
    }
    expected_project_config_with_variables_substituted = {
        "evaluation_parameter_store_name": "default_evaluation_parameter_store",
        "config_variables_file_path": None,
        "plugins_directory": "/Users/foo/bar/my/plugins/directory/complete/version/1",
        "datasources": {
            "default_spark_datasource": {
                "class_name": "Datasource",
                "module_name": "great_expectations.datasource",
                "execution_engine": {
                    "class_name": "SparkDFExecutionEngine",
                    "module_name": "great_expectations.execution_engine",
                },
                "data_connectors": {
                    "default_runtime_data_connector": {
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": None,
                    }
                },
            },
            "default_pandas_datasource": {
                "class_name": "Datasource",
                "module_name": "great_expectations.datasource",
                "execution_engine": {
                    "class_name": "PandasExecutionEngine",
                    "module_name": "great_expectations.execution_engine",
                },
                "data_connectors": {
                    "default_runtime_data_connector": {
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": None,
                    }
                },
            },
        },
        "stores": {
            "default_evaluation_parameter_store": {
                "class_name": "EvaluationParameterStore"
            },
            "default_expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "GeCloudStoreBackend",
                    "ge_cloud_base_url": "https://api.dev.greatexpectations.io/complete/version-1",
                    "ge_cloud_resource_type": "expectation_suite",
                    "ge_cloud_credentials": {
                        "access_token": "91bec65bf3fa41b99a98de6f2563eab0",
                        "account_id": "0ccac18e-7631-4bdd-8a42-3c35cce574c6",
                    },
                    "suppress_store_backend_id": True,
                },
            },
            "default_validations_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "GeCloudStoreBackend",
                    "ge_cloud_base_url": "https://api.dev.greatexpectations.io/complete/version-1",
                    "ge_cloud_resource_type": "suite_validation_result",
                    "ge_cloud_credentials": {
                        "access_token": "91bec65bf3fa41b99a98de6f2563eab0",
                        "account_id": "0ccac18e-7631-4bdd-8a42-3c35cce574c6",
                    },
                    "suppress_store_backend_id": True,
                },
            },
            "default_checkpoint_store": {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "GeCloudStoreBackend",
                    "ge_cloud_base_url": "https://api.dev.greatexpectations.io/complete/version-1",
                    "ge_cloud_resource_type": "contract",
                    "ge_cloud_credentials": {
                        "access_token": "91bec65bf3fa41b99a98de6f2563eab0",
                        "account_id": "0ccac18e-7631-4bdd-8a42-3c35cce574c6",
                    },
                    "suppress_store_backend_id": True,
                },
            },
        },
        "notebooks": None,
        "validations_store_name": "default_validations_store",
        "data_docs_sites": {
            "default_site": {
                "class_name": "SiteBuilder",
                "show_how_to_buttons": True,
                "store_backend": {
                    "class_name": "GeCloudStoreBackend",
                    "ge_cloud_base_url": "https://api.dev.greatexpectations.io/complete/version-1",
                    "ge_cloud_resource_type": "rendered_data_doc",
                    "ge_cloud_credentials": {
                        "access_token": "91bec65bf3fa41b99a98de6f2563eab0",
                        "account_id": "0ccac18e-7631-4bdd-8a42-3c35cce574c6",
                    },
                    "suppress_store_backend_id": True,
                },
                "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
                "site_section_builders": {"profiling": "None"},
            }
        },
        "anonymous_usage_statistics": {
            "enabled": False,
            "data_context_id": "0ccac18e-7631-4bdd-8a42-3c35cce574c6",
            "usage_statistics_url": "https://dev.stats.greatexpectations.io/great_expectations/v1/usage_statistics"
            "/complete/version/1"
            if not global_usage_statistics_url
            else global_usage_statistics_url,
        },
        "expectations_store_name": "default_expectations_store",
        "config_version": 3.0,
        "checkpoint_store_name": "default_checkpoint_store",
    }
    assert context.ge_cloud_config.to_json_dict() == expected_ge_cloud_config
    assert (
        context.project_config_with_variables_substituted.to_json_dict()
        == expected_project_config_with_variables_substituted
    )


def test_data_context_ge_cloud_mode_with_global_config_in_etc_dir(
    data_context_with_complete_global_config_in_etc_dir_only,
):
    context = DataContext(ge_cloud_mode=True)
    global_usage_statistics_url = context._get_global_config_value(
        environment_variable="GE_USAGE_STATISTICS_URL",
        conf_file_section="anonymous_usage_statistics",
        conf_file_option="usage_statistics_url",
    )
    expected_ge_cloud_config = {
        "base_url": "https://api.dev.greatexpectations.io/complete/version-2",
        "account_id": "31c84fc9-6659-4411-a911-4276bb464583",
        "access_token": "85327f94194e4b5b90db072029fcc474",
    }
    expected_project_config_with_variables_substituted = {
        "expectations_store_name": "default_expectations_store",
        "anonymous_usage_statistics": {
            "enabled": False,
            "usage_statistics_url": "https://dev.stats.greatexpectations.io/great_expectations/v1/usage_statistics"
            "/complete/version/2"
            if not global_usage_statistics_url
            else global_usage_statistics_url,
            "data_context_id": "31c84fc9-6659-4411-a911-4276bb464583",
        },
        "stores": {
            "default_evaluation_parameter_store": {
                "class_name": "EvaluationParameterStore"
            },
            "default_expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "GeCloudStoreBackend",
                    "ge_cloud_base_url": "https://api.dev.greatexpectations.io/complete/version-2",
                    "ge_cloud_resource_type": "expectation_suite",
                    "ge_cloud_credentials": {
                        "access_token": "85327f94194e4b5b90db072029fcc474",
                        "account_id": "31c84fc9-6659-4411-a911-4276bb464583",
                    },
                    "suppress_store_backend_id": True,
                },
            },
            "default_validations_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "GeCloudStoreBackend",
                    "ge_cloud_base_url": "https://api.dev.greatexpectations.io/complete/version-2",
                    "ge_cloud_resource_type": "suite_validation_result",
                    "ge_cloud_credentials": {
                        "access_token": "85327f94194e4b5b90db072029fcc474",
                        "account_id": "31c84fc9-6659-4411-a911-4276bb464583",
                    },
                    "suppress_store_backend_id": True,
                },
            },
            "default_checkpoint_store": {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "GeCloudStoreBackend",
                    "ge_cloud_base_url": "https://api.dev.greatexpectations.io/complete/version-2",
                    "ge_cloud_resource_type": "contract",
                    "ge_cloud_credentials": {
                        "access_token": "85327f94194e4b5b90db072029fcc474",
                        "account_id": "31c84fc9-6659-4411-a911-4276bb464583",
                    },
                    "suppress_store_backend_id": True,
                },
            },
        },
        "evaluation_parameter_store_name": "default_evaluation_parameter_store",
        "data_docs_sites": {
            "default_site": {
                "class_name": "SiteBuilder",
                "show_how_to_buttons": True,
                "store_backend": {
                    "class_name": "GeCloudStoreBackend",
                    "ge_cloud_base_url": "https://api.dev.greatexpectations.io/complete/version-2",
                    "ge_cloud_resource_type": "rendered_data_doc",
                    "ge_cloud_credentials": {
                        "access_token": "85327f94194e4b5b90db072029fcc474",
                        "account_id": "31c84fc9-6659-4411-a911-4276bb464583",
                    },
                    "suppress_store_backend_id": True,
                },
                "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
                "site_section_builders": {"profiling": "None"},
            }
        },
        "validations_store_name": "default_validations_store",
        "config_version": 3.0,
        "config_variables_file_path": None,
        "checkpoint_store_name": "default_checkpoint_store",
        "datasources": {
            "default_spark_datasource": {
                "data_connectors": {
                    "default_runtime_data_connector": {
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": None,
                    }
                },
                "execution_engine": {
                    "class_name": "SparkDFExecutionEngine",
                    "module_name": "great_expectations.execution_engine",
                },
                "class_name": "Datasource",
                "module_name": "great_expectations.datasource",
            },
            "default_pandas_datasource": {
                "data_connectors": {
                    "default_runtime_data_connector": {
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": None,
                    }
                },
                "execution_engine": {
                    "class_name": "PandasExecutionEngine",
                    "module_name": "great_expectations.execution_engine",
                },
                "class_name": "Datasource",
                "module_name": "great_expectations.datasource",
            },
        },
        "notebooks": None,
        "plugins_directory": "/Users/foo/bar/my/plugins/directory/complete/version/2",
    }
    assert context.ge_cloud_config.to_json_dict() == expected_ge_cloud_config
    assert (
        context.project_config_with_variables_substituted.to_json_dict()
        == expected_project_config_with_variables_substituted
    )


def test_data_context_ge_cloud_mode_mixed_cloud_config_precedence(
    data_context_with_complete_global_config_in_dot_and_etc_dirs,
):
    ge_cloud_config_env_vars = {
        "GE_CLOUD_ACCOUNT_ID": "c865b794-5d61-4f7e-8c9c-a60ef5bef785"
    }
    with mock.patch.dict(os.environ, ge_cloud_config_env_vars):
        context = DataContext(
            ge_cloud_mode=True,
            ge_cloud_base_url="https://my/runtime/base/url/takes/top/precedence",
        )
        global_usage_statistics_url = context._get_global_config_value(
            environment_variable="GE_USAGE_STATISTICS_URL",
            conf_file_section="anonymous_usage_statistics",
            conf_file_option="usage_statistics_url",
        )
        expected_ge_cloud_config = {
            "base_url": "https://my/runtime/base/url/takes/top/precedence",
            "account_id": "c865b794-5d61-4f7e-8c9c-a60ef5bef785",
            "access_token": "91bec65bf3fa41b99a98de6f2563eab0",
        }
        expected_project_config_with_variables_substituted = {
            "plugins_directory": "/Users/foo/bar/my/plugins/directory/complete/version/1",
            "data_docs_sites": {
                "default_site": {
                    "class_name": "SiteBuilder",
                    "show_how_to_buttons": True,
                    "store_backend": {
                        "class_name": "GeCloudStoreBackend",
                        "ge_cloud_base_url": "https://my/runtime/base/url/takes/top/precedence",
                        "ge_cloud_resource_type": "rendered_data_doc",
                        "ge_cloud_credentials": {
                            "access_token": "91bec65bf3fa41b99a98de6f2563eab0",
                            "account_id": "c865b794-5d61-4f7e-8c9c-a60ef5bef785",
                        },
                        "suppress_store_backend_id": True,
                    },
                    "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
                    "site_section_builders": {"profiling": "None"},
                }
            },
            "expectations_store_name": "default_expectations_store",
            "evaluation_parameter_store_name": "default_evaluation_parameter_store",
            "notebooks": None,
            "anonymous_usage_statistics": {
                "usage_statistics_url": "https://dev.stats.greatexpectations.io/great_expectations/v1"
                "/usage_statistics/complete/version/1"
                if not global_usage_statistics_url
                else global_usage_statistics_url,
                "data_context_id": "c865b794-5d61-4f7e-8c9c-a60ef5bef785",
                "enabled": False,
            },
            "validations_store_name": "default_validations_store",
            "datasources": {
                "default_spark_datasource": {
                    "execution_engine": {
                        "class_name": "SparkDFExecutionEngine",
                        "module_name": "great_expectations.execution_engine",
                    },
                    "class_name": "Datasource",
                    "data_connectors": {
                        "default_runtime_data_connector": {
                            "batch_identifiers": None,
                            "class_name": "RuntimeDataConnector",
                        }
                    },
                    "module_name": "great_expectations.datasource",
                },
                "default_pandas_datasource": {
                    "execution_engine": {
                        "class_name": "PandasExecutionEngine",
                        "module_name": "great_expectations.execution_engine",
                    },
                    "class_name": "Datasource",
                    "data_connectors": {
                        "default_runtime_data_connector": {
                            "batch_identifiers": None,
                            "class_name": "RuntimeDataConnector",
                        }
                    },
                    "module_name": "great_expectations.datasource",
                },
            },
            "stores": {
                "default_evaluation_parameter_store": {
                    "class_name": "EvaluationParameterStore"
                },
                "default_expectations_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {
                        "class_name": "GeCloudStoreBackend",
                        "ge_cloud_base_url": "https://my/runtime/base/url/takes/top/precedence",
                        "ge_cloud_resource_type": "expectation_suite",
                        "ge_cloud_credentials": {
                            "access_token": "91bec65bf3fa41b99a98de6f2563eab0",
                            "account_id": "c865b794-5d61-4f7e-8c9c-a60ef5bef785",
                        },
                        "suppress_store_backend_id": True,
                    },
                },
                "default_validations_store": {
                    "class_name": "ValidationsStore",
                    "store_backend": {
                        "class_name": "GeCloudStoreBackend",
                        "ge_cloud_base_url": "https://my/runtime/base/url/takes/top/precedence",
                        "ge_cloud_resource_type": "suite_validation_result",
                        "ge_cloud_credentials": {
                            "access_token": "91bec65bf3fa41b99a98de6f2563eab0",
                            "account_id": "c865b794-5d61-4f7e-8c9c-a60ef5bef785",
                        },
                        "suppress_store_backend_id": True,
                    },
                },
                "default_checkpoint_store": {
                    "class_name": "CheckpointStore",
                    "store_backend": {
                        "class_name": "GeCloudStoreBackend",
                        "ge_cloud_base_url": "https://my/runtime/base/url/takes/top/precedence",
                        "ge_cloud_resource_type": "contract",
                        "ge_cloud_credentials": {
                            "access_token": "91bec65bf3fa41b99a98de6f2563eab0",
                            "account_id": "c865b794-5d61-4f7e-8c9c-a60ef5bef785",
                        },
                        "suppress_store_backend_id": True,
                    },
                },
            },
            "config_variables_file_path": None,
            "checkpoint_store_name": "default_checkpoint_store",
            "config_version": 3.0,
        }
        assert context.ge_cloud_config.to_json_dict() == expected_ge_cloud_config
        assert (
            context.project_config_with_variables_substituted.to_json_dict()
            == expected_project_config_with_variables_substituted
        )


def test_data_context_ge_cloud_mode_with_usage_stats_section_in_config(
    data_context_with_complete_global_config_with_usage_stats_section_in_dot_dir_only,
):
    context = DataContext(ge_cloud_mode=True)
    global_usage_statistics_url = context._get_global_config_value(
        environment_variable="GE_USAGE_STATISTICS_URL",
        conf_file_section="anonymous_usage_statistics",
        conf_file_option="usage_statistics_url",
    )
    expected_ge_cloud_config = {
        "access_token": "91bec65bf3fa41b99a98de6f2563eab0",
        "account_id": "0ccac18e-7631-4bdd-8a42-3c35cce574c6",
        "base_url": "https://api.dev.greatexpectations.io/complete/version-1",
    }
    expected_project_config_with_variables_substituted = {
        "validations_store_name": "default_validations_store",
        "checkpoint_store_name": "default_checkpoint_store",
        "datasources": {
            "default_spark_datasource": {
                "module_name": "great_expectations.datasource",
                "data_connectors": {
                    "default_runtime_data_connector": {
                        "batch_identifiers": None,
                        "class_name": "RuntimeDataConnector",
                    }
                },
                "class_name": "Datasource",
                "execution_engine": {
                    "module_name": "great_expectations.execution_engine",
                    "class_name": "SparkDFExecutionEngine",
                },
            },
            "default_pandas_datasource": {
                "module_name": "great_expectations.datasource",
                "data_connectors": {
                    "default_runtime_data_connector": {
                        "batch_identifiers": None,
                        "class_name": "RuntimeDataConnector",
                    }
                },
                "class_name": "Datasource",
                "execution_engine": {
                    "module_name": "great_expectations.execution_engine",
                    "class_name": "PandasExecutionEngine",
                },
            },
        },
        "data_docs_sites": {
            "default_site": {
                "class_name": "SiteBuilder",
                "show_how_to_buttons": True,
                "store_backend": {
                    "class_name": "GeCloudStoreBackend",
                    "ge_cloud_base_url": "https://api.dev.greatexpectations.io/complete/version-1",
                    "ge_cloud_resource_type": "rendered_data_doc",
                    "ge_cloud_credentials": {
                        "access_token": "91bec65bf3fa41b99a98de6f2563eab0",
                        "account_id": "0ccac18e-7631-4bdd-8a42-3c35cce574c6",
                    },
                    "suppress_store_backend_id": True,
                },
                "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
                "site_section_builders": {"profiling": "None"},
            }
        },
        "config_version": 3.0,
        "stores": {
            "default_evaluation_parameter_store": {
                "class_name": "EvaluationParameterStore"
            },
            "default_expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "GeCloudStoreBackend",
                    "ge_cloud_base_url": "https://api.dev.greatexpectations.io/complete/version-1",
                    "ge_cloud_resource_type": "expectation_suite",
                    "ge_cloud_credentials": {
                        "access_token": "91bec65bf3fa41b99a98de6f2563eab0",
                        "account_id": "0ccac18e-7631-4bdd-8a42-3c35cce574c6",
                    },
                    "suppress_store_backend_id": True,
                },
            },
            "default_validations_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "GeCloudStoreBackend",
                    "ge_cloud_base_url": "https://api.dev.greatexpectations.io/complete/version-1",
                    "ge_cloud_resource_type": "suite_validation_result",
                    "ge_cloud_credentials": {
                        "access_token": "91bec65bf3fa41b99a98de6f2563eab0",
                        "account_id": "0ccac18e-7631-4bdd-8a42-3c35cce574c6",
                    },
                    "suppress_store_backend_id": True,
                },
            },
            "default_checkpoint_store": {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "GeCloudStoreBackend",
                    "ge_cloud_base_url": "https://api.dev.greatexpectations.io/complete/version-1",
                    "ge_cloud_resource_type": "contract",
                    "ge_cloud_credentials": {
                        "access_token": "91bec65bf3fa41b99a98de6f2563eab0",
                        "account_id": "0ccac18e-7631-4bdd-8a42-3c35cce574c6",
                    },
                    "suppress_store_backend_id": True,
                },
            },
        },
        "config_variables_file_path": None,
        "notebooks": None,
        "anonymous_usage_statistics": {
            "enabled": False,
            "data_context_id": "0ccac18e-7631-4bdd-8a42-3c35cce574c6",
            "usage_statistics_url": "https://dev.stats.greatexpectations.io/great_expectations/v1/usage_statistics/complete/version/1"
            if not global_usage_statistics_url
            else global_usage_statistics_url,
        },
        "expectations_store_name": "default_expectations_store",
        "plugins_directory": "/Users/foo/bar/my/plugins/directory/complete/version/1",
        "evaluation_parameter_store_name": "default_evaluation_parameter_store",
    }
    assert context.ge_cloud_config.to_json_dict() == expected_ge_cloud_config
    assert (
        context.project_config_with_variables_substituted.to_json_dict()
        == expected_project_config_with_variables_substituted
    )


def test_data_context_ge_cloud_mode_with_incomplete_cloud_config_should_throw_error(
    data_context_with_incomplete_global_config_in_dot_dir_only,
):
    with pytest.raises(DataContextError):
        DataContext(context_root_dir="/my/context/root/dir", ge_cloud_mode=True)
