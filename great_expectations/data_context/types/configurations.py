from six import string_types

from great_expectations.types import Config


class DataContextConfig(Config):
    _allowed_keys = set([
        "config_variables_file_path",
        "plugins_directory",
        "expectations_store",
        "evaluation_parameter_store_name",
        "datasources",
        "stores",
        "data_docs",  # TODO: Rename this to sites, to remove a layer of extraneous nesting
    ])

    _required_keys = set([
        "plugins_directory",
        "expectations_store",
        "evaluation_parameter_store_name",
        "datasources",
        "stores",
        "data_docs",
    ])

    _key_types = {
        "config_variables_file_path": string_types,
        "plugins_directory": string_types,
        "expectations_store": dict,
        "evaluation_parameter_store_name": string_types,
        "datasources": dict,
        "stores": dict,
        "data_docs": dict,
    }
