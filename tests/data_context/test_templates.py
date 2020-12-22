import pytest

import great_expectations.data_context.templates as templates


@pytest.fixture()
def project_optional_config_comment():
    """
    Default value for PROJECT_OPTIONAL_CONFIG_COMMENT
    """
    PROJECT_OPTIONAL_CONFIG_COMMENT = (
        templates.CONFIG_VARIABLES_INTRO
        + """
config_variables_file_path: uncommitted/config_variables.yml

# The plugins_directory will be added to your python path for custom modules
# used to override and extend Great Expectations.
plugins_directory: plugins/

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
      #     slack_webhook: ${validation_notification_slack_webhook}
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
  expectations_store:
    class_name: ExpectationsStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: expectations/

  validations_store:
    class_name: ValidationsStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: uncommitted/validations/

  evaluation_parameter_store:
    # Evaluation Parameters enable dynamic expectations. Read more here:
    # https://docs.greatexpectations.io/en/latest/reference/core_concepts/evaluation_parameters.html
    class_name: EvaluationParameterStore

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
    return PROJECT_OPTIONAL_CONFIG_COMMENT


@pytest.fixture()
def project_help_comment():
    PROJECT_HELP_COMMENT = """
# Welcome to Great Expectations! Always know what to expect from your data.
#
# Here you can define datasources, batch kwargs generators, integrations and
# more. This file is intended to be committed to your repo. For help with
# configuration please:
#   - Read our docs: https://docs.greatexpectations.io/en/latest/how_to_guides/spare_parts/data_context_reference.html#configuration
#   - Join our slack channel: http://greatexpectations.io/slack

# config_version refers to the syntactic version of this config file, and is used in maintaining backwards compatibility
# It is auto-generated and usually does not need to be changed.
config_version: 2

# Datasources tell Great Expectations where your data lives and how to get it.
# You can use the CLI command `great_expectations datasource new` to help you
# add a new datasource. Read more at https://docs.greatexpectations.io/en/latest/reference/core_concepts/datasource_reference.html
datasources: {}
"""
    return PROJECT_HELP_COMMENT


def test_project_optional_config_comment_matches_default(
    project_optional_config_comment,
):
    """
    What does this test and why?
    Make sure that the templates built on data_context.types.base.DataContextConfigDefaults match the desired default.
    """

    assert templates.PROJECT_OPTIONAL_CONFIG_COMMENT == project_optional_config_comment


def test_project_help_comment_matches_default(project_help_comment):
    """
    What does this test and why?
    Make sure that the templates built on data_context.types.base.DataContextConfigDefaults match the desired default.
    """

    assert templates.PROJECT_HELP_COMMENT == project_help_comment
