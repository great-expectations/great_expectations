# -*- coding: utf-8 -*-

PROJECT_HELP_COMMENT = """# Welcome to great expectations. 
# This project configuration file allows you to define datasources, 
# generators, integrations, and other configuration artifacts that
# make it easier to use Great Expectations.

# For more help configuring great expectations, 
# see the documentation at: https://greatexpectations.io/config_file.html

# NOTE: GE uses the names of configured datasources and generators to manage
# how expectations and other configuration artifacts are stored in the 
# expectations/ and datasources/ folders. If you need to rename an existing
# datasource or generator, be sure to also update the paths for related artifacts.

"""

PROJECT_OPTIONAL_CONFIG_COMMENT = """

# The plugins_directory is where the data_context will look for custom_data_assets.py
# and any configured evaluation parameter store

plugins_directory: plugins/

expectations_store:
  class_name: ExpectationStore
  store_backend:
    class_name: FixedLengthTupleFilesystemStoreBackend
    base_directory: expectations/

evaluation_parameter_store_name: evaluation_parameter_store

# Configure additional data context options here.

# Uncomment the lines below to enable s3 as a result store. If a result store is enabled,
# validation results will be saved in the store according to run id.

# For S3, ensure that appropriate credentials or assume_role permissions are set where
# validation happens.

stores:

  local_validation_result_store:
    class_name: ValidationResultStore
    store_backend:
      class_name: FixedLengthTupleFilesystemStoreBackend
      base_directory: uncommitted/validations/
      filepath_template: '{4}/{0}/{1}/{2}/{3}.json'

  # FIXME: These configs are temporarily commented out to facititate refactoring Stores.

  # local_profiling_store:
  #   module_name: great_expectations.data_context.store
  #   class_name: FilesystemStore
  #   store_config:
  #     base_directory: uncommitted/profiling/
  #     serialization_type: json
  #     file_extension: .json

  # local_workbench_site_store:
  #   module_name: great_expectations.data_context.store
  #   class_name: FilesystemStore
  #   store_config:
  #     base_directory: uncommitted/documentation/local_site
  #     file_extension: .html

  # shared_team_site_store:
  #   module_name: great_expectations.data_context.store
  #   class_name: FilesystemStore
  #   store_config:
  #     base_directory: uncommitted/documentation/team_site
  #     file_extension: .html

  # fixture_validation_results_store:
  #   module_name: great_expectations.data_context.store
  #   class_name: FilesystemStore
  #   store_config:
  #     base_directory: fixtures/validations
  #     file_extension: .zzz

  fixture_validation_results_store:
    class_name: ValidationResultStore
    store_backend:
      class_name: FixedLengthTupleFilesystemStoreBackend
      base_directory: fixtures/validations
      filepath_template: '{4}/{0}/{1}/{2}/{3}.json'

#  data_asset_snapshot_store:
#    module_name: great_expectations.data_context.store
#    class_name: S3Store
#    store_config:
#      bucket:
#      key_prefix:

  evaluation_parameter_store:
    module_name: great_expectations.data_context.store
    class_name: EvaluationParameterStore

  local_site_html_store:
    module_name: great_expectations.data_context.store
    class_name: HtmlSiteStore
    base_directory: uncommitted/documentation/local_site/

  team_site_html_store:
    module_name: great_expectations.data_context.store
    class_name: HtmlSiteStore
    base_directory: uncommitted/documentation/team_site/

# Uncomment the lines below to enable a result callback.

# result_callback:
#   slack: https://slack.com/replace_with_your_webhook


data_docs:
  sites:
    local_site: # site name
    # “local_site” renders documentation for all the datasources in the project from GE artifacts in the local repo. 
    # The site includes expectation suites and profiling and validation results from uncommitted directory. 
    # Local site provides the convenience of visualizing all the entities stored in JSON files as HTML.

      module_name: great_expectations.render.renderer.new_site_builder
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
    # Reach out on Slack (https://tinyurl.com/great-expectations-slack>) if you would like to discuss the best way to configure a team site.

      module_name: great_expectations.render.renderer.new_site_builder
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

PROFILE_COMMENT = """This file stores profiles with database access credentials. 
Do not commit this file to version control. 

A profile can optionally have a single parameter called 
"url" which will be passed to sqlalchemy's create_engine.

Otherwise, all credential options specified here for a 
given profile will be passed to sqlalchemy's create URL function.

"""