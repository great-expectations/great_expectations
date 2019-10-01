# -*- coding: utf-8 -*-

PROJECT_HELP_COMMENT = """
# Welcome to Great Expectations! Always know what to expect from your data.
# 
# This project configuration file allows you to define datasources, generators,
# integrations, and other things to help you crush pipeline debt.
#
# This file is intended to be committed to your source control repo.

# For more help configuring great expectations see the documentation at:
# https://docs.greatexpectations.io/en/latest/core_concepts/data_context.html#configuration
# or join our slack channel: http://greatexpectations.io/slack

# NOTE: GE uses the names of configured `datasources` and `generators` to manage
# how `expectations` and other artifacts are stored in the `expectations/` and 
# `datasources/` folders. If you need to rename an existing `datasource` or 
# `generator`, be sure to also update the relevant directory names.

config_version: 1

"""

CONFIG_VARIABLES_INTRO = """
# This config file supports variable substitution which enables two use cases:
#   1. Secrets are kept out of committed files.
#   2. Configuration parameters can change based on the environment. For
#      example: dev vs staging vs prod.
#
# When GE encounters substitution syntaxes in the config file, like the ones
# below it will attempt to replace the value of “my_key” with the value from an 
# environment variable “my_value” or a corresponding key read from the file
# specified using `config_variables_file_path`. An environment variable will
# always take precedence.
#   - `my_key: ${my_value}
#   - `my_key: $my_value`
#
# If the substitution value comes from the config variables file, it can be a
# simple (non-nested) value or a nested value such as a dictionary. If it comes
# from an environment variable, it must be a simple value."""

PROJECT_OPTIONAL_CONFIG_COMMENT = CONFIG_VARIABLES_INTRO + """
config_variables_file_path: uncommitted/config_variables.yml

# The plugins_directory will be added to your python path for custom modules
# used to override and extend Great Expectations.
plugins_directory: plugins/

expectations_store:
  # This is where expectations are kept.
  class_name: ExpectationStore
  store_backend:
    class_name: FixedLengthTupleFilesystemStoreBackend
    base_directory: expectations/

profiling_store_name: local_validation_result_store
evaluation_parameter_store_name: evaluation_parameter_store

# Stores are where things like expectations, evalutation results, etc are stored.
stores:

  local_validation_result_store:
    class_name: ValidationResultStore
    store_backend:
      class_name: FixedLengthTupleFilesystemStoreBackend
      base_directory: uncommitted/validations/
      filepath_template: '{4}/{0}/{1}/{2}/{3}.json'

  # Uncomment the lines below to enable s3 as a result store. When enabled,
  # validation results will be saved in the store according to `run id`.
  # For S3, ensure that appropriate credentials or assume_role permissions are
  # set where validation happens.
  
  # s3_validation_result_store:
  #   class_name: ValidationStore
  #   store_backend:
  #     class_name: FixedLengthTupleS3StoreBackend
  #     bucket: ???
  #     prefix: ???
  #     file_extension: json
  #     filepath_template: '{4}/{0}/{1}/{2}/{3}.{file_extension}'


  evaluation_parameter_store:
    # Evaluation Parameters enable dynamic expectations. Read more here:
    # https://docs.greatexpectations.io/en/v0.8.x/reference/evaluation_parameters.html
    module_name: great_expectations.data_context.store
    class_name: EvaluationParameterStore
  
  fixture_validation_results_store:
    class_name: ValidationResultStore
    store_backend:
      class_name: FixedLengthTupleFilesystemStoreBackend
      base_directory: fixtures/validations
      filepath_template: '{4}/{0}/{1}/{2}/{3}.json'
  
  local_site_html_store:
    module_name: great_expectations.data_context.store
    class_name: HtmlSiteStore
    base_directory: uncommitted/documentation/local_site/

  team_site_html_store:
    module_name: great_expectations.data_context.store
    class_name: HtmlSiteStore
    base_directory: uncommitted/documentation/team_site/


validation_operators:
  # Read about validation operators at: https://docs.greatexpectations.io/en/latest/guides/validation_operators.html
  perform_action_list_operator:
    class_name: PerformActionListValidationOperator
    action_list:
      - name: store_validation_result
        action:
          class_name: StoreAction
          target_store_name: local_validation_result_store
      - name: store_evaluation_params
        action:
          class_name: ExtractAndStoreEvaluationParamsAction
          target_store_name: evaluation_parameter_store
      - name: store_evaluation_params
        action:
          class_name: SlackNotificationAction
          slack_webhook: ${validation_notification_slack_webhook}
#          notify_on: all
          renderer:
            module_name: great_expectations.render.renderer.slack_renderer
            class_name: SlackRenderer
    
# Uncomment the lines below to enable a result callback.

# result_callback:
#   slack: ${slack_callback_url}

# TODO : Remove the extra layer of yml nesting in v0.8:
data_docs:
  sites:
    local_site: # site name
    # “local_site” renders documentation for all the datasources in the project from GE artifacts in the local repo. 
    # The site includes expectation suites and profiling and validation results from uncommitted directory. 
    # Local site provides the convenience of visualizing all the entities stored in JSON files as HTML.

      # specify a whitelist here if you would like to restrict the datasources to document
      datasource_whitelist: '*'

      module_name: great_expectations.render.renderer.site_builder
      class_name: SiteBuilder
      target_store_name: local_site_html_store
      
      site_index_builder:
        class_name: DefaultSiteIndexBuilder
      
      site_section_builders:
          
        expectations:
          class_name: DefaultSiteSectionBuilder
          source_store_name: expectations_store
          renderer:
            module_name: great_expectations.render.renderer
            class_name: ExpectationSuitePageRenderer

        validations:
          class_name: DefaultSiteSectionBuilder
          source_store_name: local_validation_result_store
          run_id_filter:
            ne: profiling
          renderer:
            module_name: great_expectations.render.renderer
            class_name: ValidationResultsPageRenderer

        profiling:
          class_name: DefaultSiteSectionBuilder
          source_store_name: local_validation_result_store
          run_id_filter:
            eq: profiling
          renderer:
            module_name: great_expectations.render.renderer
            class_name: ProfilingResultsPageRenderer

    team_site:
    # "team_site" is meant to support the "shared source of truth for a team" use case. 
    # By default only the expectations section is enabled.
    #  Users have to configure the profiling and the validations sections (and the corresponding validations_store and profiling_store attributes based on the team's decisions where these are stored (a local filesystem or S3). 
    # Reach out on Slack (https://greatexpectations.io/slack>) if you would like to discuss the best way to configure a team site.

      # specify a whitelist here if you would like to restrict the datasources to document
      datasource_whitelist: '*'
      
      module_name: great_expectations.render.renderer.site_builder
      class_name: SiteBuilder
      target_store_name: team_site_html_store
      
      site_index_builder:
        class_name: DefaultSiteIndexBuilder
      
      site_section_builders:
          
        expectations:
          class_name: DefaultSiteSectionBuilder
          source_store_name: expectations_store
          renderer:
            module_name: great_expectations.render.renderer
            class_name: ExpectationSuitePageRenderer

"""

PROJECT_TEMPLATE = PROJECT_HELP_COMMENT + "datasources: {}\n" + PROJECT_OPTIONAL_CONFIG_COMMENT

CONFIG_VARIABLES_COMMENT = CONFIG_VARIABLES_INTRO

CONFIG_VARIABLES_FILE_TEMPLATE = CONFIG_VARIABLES_INTRO