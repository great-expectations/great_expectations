import os
import uuid

from ruamel.yaml import YAML
from ruamel.yaml.compat import StringIO

from great_expectations.data_context.types.base import DataContextConfigDefaults


class YAMLToString(YAML):
    """
    Get yaml dump as a string: https://yaml.readthedocs.io/en/latest/example.html#output-of-dump-as-a-string
    """

    def dump(self, data, stream=None, **kw):
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
#   - Read our docs: https://docs.greatexpectations.io/en/latest/how_to_guides/spare_parts/data_context_reference.html#configuration
#   - Join our slack channel: http://greatexpectations.io/slack

# config_version refers to the syntactic version of this config file, and is used in maintaining backwards compatibility
# It is auto-generated and usually does not need to be changed.
config_version: {DataContextConfigDefaults.DEFAULT_CONFIG_VERSION.value}

# Datasources tell Great Expectations where your data lives and how to get it.
# You can use the CLI command `great_expectations datasource new` to help you
# add a new datasource. Read more at https://docs.greatexpectations.io/en/latest/reference/core_concepts/datasource_reference.html
datasources: {{}}
"""

CONFIG_VARIABLES_INTRO = """
# This config file supports variable substitution which enables: 1) keeping
# secrets out of source control & 2) environment-based configuration changes
# such as staging vs prod.
#
# When GE encounters substitution syntax (like `my_key: ${my_value}` or
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
# https://docs.greatexpectations.io/en/latest/guides/how_to_guides/configuring_data_contexts/how_to_use_a_yaml_file_or_environment_variables_to_populate_credentials.html

"""

CONFIG_VARIABLES_TEMPLATE = (
    CONFIG_VARIABLES_INTRO + "instance_id: " + str(uuid.uuid4()) + os.linesep
)

# Create yaml strings
# NOTE: .replace("\n", "\n  ")[:-2] is a hack to indent all lines two spaces,
# and remove the inserted final two spaces.
EXPECTATIONS_STORE_STRING = yaml.dump(
    {
        "expectations_store": DataContextConfigDefaults.DEFAULT_STORES.value[
            "expectations_store"
        ]
    }
).replace("\n", "\n  ")[:-2]
VALIDATIONS_STORE_STRING = yaml.dump(
    {
        "validations_store": DataContextConfigDefaults.DEFAULT_STORES.value[
            "validations_store"
        ]
    }
).replace("\n", "\n  ")[:-2]
EVALUATION_PARAMETER_STORE_STRING = yaml.dump(
    DataContextConfigDefaults.DEFAULT_STORES.value["evaluation_parameter_store"]
).replace("\n", "")

PROJECT_OPTIONAL_CONFIG_COMMENT = (
    CONFIG_VARIABLES_INTRO
    + f"""
config_variables_file_path: {DataContextConfigDefaults.DEFAULT_CONFIG_VARIABLES_FILEPATH.value}

# The plugins_directory will be added to your python path for custom modules
# used to override and extend Great Expectations.
plugins_directory: {DataContextConfigDefaults.DEFAULT_PLUGINS_DIRECTORY.value}

# Validation Operators are customizable workflows that bundle the validation of
# one or more expectation suites and subsequent actions. The example below
# stores validations and send a slack notification. To read more about
# customizing and extending these, read: https://docs.greatexpectations.io/en/latest/reference/core_concepts/validation_operators_and_actions.html
validation_operators:
  action_list_operator:
    # To learn how to configure sending Slack notifications during evaluation
    # (and other customizations), read: https://docs.greatexpectations.io/en/latest/autoapi/great_expectations/validation_operators/index.html#great_expectations.validation_operators.ActionListValidationOperator
    class_name: ActionListValidationOperator
    action_list:
      - name: store_validation_result
        action:
          class_name: StoreValidationResultAction
      - name: store_evaluation_params
        action:
          class_name: StoreEvaluationParametersAction
      - name: update_data_docs
        action:
          class_name: UpdateDataDocsAction
      # - name: send_slack_notification_on_validation_result
      #   action:
      #     class_name: SlackNotificationAction
      #     # put the actual webhook URL in the uncommitted/config_variables.yml file
      #     slack_webhook: ${{validation_notification_slack_webhook}}
      #     notify_on: all # possible values: "all", "failure", "success"
      #     notify_with: # optional list containing the DataDocs sites to include in the notification.
      #     renderer:
      #       module_name: great_expectations.render.renderer.slack_renderer
      #       class_name: SlackRenderer

stores:
# Stores are configurable places to store things like Expectations, Validations
# Data Docs, and more. These are for advanced users only - most users can simply
# leave this section alone.
#
# Three stores are required: expectations, validations, and
# evaluation_parameters, and must exist with a valid store entry. Additional
# stores can be configured for uses such as data_docs, validation_operators, etc.
  {EXPECTATIONS_STORE_STRING}
  {VALIDATIONS_STORE_STRING}
  evaluation_parameter_store:
    # Evaluation Parameters enable dynamic expectations. Read more here:
    # https://docs.greatexpectations.io/en/latest/reference/core_concepts/evaluation_parameters.html
    {EVALUATION_PARAMETER_STORE_STRING}

expectations_store_name: expectations_store
validations_store_name: validations_store
evaluation_parameter_store_name: evaluation_parameter_store

data_docs_sites:
  # Data Docs make it simple to visualize data quality in your project. These
  # include Expectations, Validations & Profiles. The are built for all
  # Datasources from JSON artifacts in the local repo including validations &
  # profiles from the uncommitted directory. Read more at https://docs.greatexpectations.io/en/latest/reference/core_concepts/data_docs.html
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

ANONYMIZED_USAGE_STATISTICS_ENABLED = """
anonymous_usage_statistics:
  enabled: True
"""

ANONYMIZED_USAGE_STATISTICS_DISABLED = """
anonymous_usage_statistics:
  enabled: False
"""

PROJECT_TEMPLATE_USAGE_STATISTICS_ENABLED = (
    PROJECT_HELP_COMMENT
    + PROJECT_OPTIONAL_CONFIG_COMMENT
    + ANONYMIZED_USAGE_STATISTICS_ENABLED
)
PROJECT_TEMPLATE_USAGE_STATISTICS_DISABLED = (
    PROJECT_HELP_COMMENT
    + PROJECT_OPTIONAL_CONFIG_COMMENT
    + ANONYMIZED_USAGE_STATISTICS_DISABLED
)
