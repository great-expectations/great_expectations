from __future__ import annotations

import os
import uuid

from ruamel.yaml import YAML
from ruamel.yaml.compat import StringIO

from great_expectations.data_context.types.base import DataContextConfigDefaults


class YAMLToString(YAML):
    """
    Get yaml dump as a string: https://yaml.readthedocs.io/en/latest/example.html#output-of-dump-as-a-string
    """

    def dump(self, data, stream=None, **kw):  # type: ignore[explicit-override] # FIXME
        inefficient = False
        if not stream:
            inefficient = True
            stream = StringIO()
        YAML.dump(self, data, stream, **kw)
        if inefficient:
            return stream.getvalue()


yaml = YAMLToString()
yaml.indent(mapping=2, sequence=4, offset=4)
yaml.default_flow_style = False

# TODO: maybe bring params in via f-strings from base.ConfigDefaults or whatever
#  I end up using for the base level configs. Specifically PROJECT_OPTIONAL_CONFIG_COMMENT
#  and PROJECT_HELP_COMMENT

PROJECT_HELP_COMMENT = f"""
# Welcome to Great Expectations! Always know what to expect from your data.
#
# Here you can define datasources, batch kwargs generators, integrations and
# more. This file is intended to be committed to your repo. For help with
# configuration please:
#   - Read our docs: https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/connect_to_data_overview/#2-configure-your-datasource
#   - Join our slack channel: http://greatexpectations.io/slack

# config_version refers to the syntactic version of this config file, and is used in maintaining backwards compatibility
# It is auto-generated and usually does not need to be changed.
config_version: {DataContextConfigDefaults.DEFAULT_CONFIG_VERSION.value}
"""  # noqa: E501

CONFIG_VARIABLES_INTRO = """
# This config file supports variable substitution which enables: 1) keeping
# secrets out of source control & 2) environment-based configuration changes
# such as staging vs prod.
#
# When GX encounters substitution syntax (like `my_key: ${my_value}` or
# `my_key: $my_value`) in the great_expectations.yml file, it will attempt
# to replace the value of `my_key` with the value from an environment
# variable `my_value` or a corresponding key read from this config file,
# which is defined through the `config_variables_file_path`.
# Environment variables take precedence over variables defined here.
#
# Substitution values defined here can be a simple (non-nested) value,
# nested value such as a dictionary, or an environment variable (i.e. ${ENV_VAR})
#
#
# https://docs.greatexpectations.io/docs/guides/setup/configuring_data_contexts/how_to_configure_credentials

"""

CONFIG_VARIABLES_TEMPLATE = f"{CONFIG_VARIABLES_INTRO}instance_id: {uuid.uuid4()!s}{os.linesep}"

# Create yaml strings
# NOTE: .replace("\n", "\n  ")[:-2] is a hack to indent all lines two spaces,
# and remove the inserted final two spaces.
EXPECTATIONS_STORE_STRING = yaml.dump(
    {"expectations_store": DataContextConfigDefaults.DEFAULT_STORES.value["expectations_store"]}
).replace("\n", "\n  ")[:-2]
VALIDATIONS_STORE_STRING = yaml.dump(
    {
        "validation_results_store": DataContextConfigDefaults.DEFAULT_STORES.value[
            "validation_results_store"
        ]
    }
).replace("\n", "\n  ")[:-2]
CHECKPOINT_STORE_STRING = yaml.dump(
    {"checkpoint_store": DataContextConfigDefaults.DEFAULT_STORES.value["checkpoint_store"]}
).replace("\n", "\n  ")[:-2]
VALIDATION_DEFINITION_STORE_STRING = yaml.dump(
    {
        "validation_definition_store": DataContextConfigDefaults.DEFAULT_STORES.value[
            "validation_definition_store"
        ]
    }
).replace("\n", "\n  ")[:-2]

PROJECT_OPTIONAL_CONFIG_COMMENT = (
    CONFIG_VARIABLES_INTRO
    + f"""
config_variables_file_path: {DataContextConfigDefaults.DEFAULT_CONFIG_VARIABLES_FILEPATH.value}

# The plugins_directory will be added to your python path for custom modules
# used to override and extend Great Expectations.
plugins_directory: {DataContextConfigDefaults.DEFAULT_PLUGINS_DIRECTORY.value}

stores:
# Stores are configurable places to store things like Expectations, Validations
# Data Docs, and more. These are for advanced users only - most users can simply
# leave this section alone.
  {EXPECTATIONS_STORE_STRING}
  {VALIDATIONS_STORE_STRING}
  {CHECKPOINT_STORE_STRING}
  {VALIDATION_DEFINITION_STORE_STRING}
expectations_store_name: expectations_store
validation_results_store_name: validation_results_store
checkpoint_store_name: checkpoint_store

data_docs_sites:
  # Data Docs make it simple to visualize data quality in your project. These
  # include Expectations, Validations & Profiles. The are built for all
  # Datasources from JSON artifacts in the local repo including validations &
  # profiles from the uncommitted directory. Read more at https://docs.greatexpectations.io/docs/terms/data_docs
  local_site:
    class_name: SiteBuilder
    # set to false to hide how-to buttons in Data Docs
    show_how_to_buttons: true
    store_backend:
        class_name: TupleFilesystemStoreBackend
        base_directory: uncommitted/data_docs/local_site/
    site_index_builder:
        class_name: DefaultSiteIndexBuilder
"""
)


PROJECT_TEMPLATE_USAGE_STATISTICS_ENABLED = PROJECT_HELP_COMMENT + PROJECT_OPTIONAL_CONFIG_COMMENT
