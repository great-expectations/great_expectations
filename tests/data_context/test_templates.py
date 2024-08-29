import pytest

from great_expectations.data_context import templates


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

stores:
# Stores are configurable places to store things like Expectations, Validations
# Data Docs, and more. These are for advanced users only - most users can simply
# leave this section alone.
  expectations_store:
    class_name: ExpectationsStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: expectations/

  validation_results_store:
    class_name: ValidationResultsStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: uncommitted/validations/

  checkpoint_store:
    class_name: CheckpointStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      suppress_store_backend_id: true
      base_directory: checkpoints/

  validation_definition_store:
    class_name: ValidationDefinitionStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: validation_definitions/

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
    return PROJECT_OPTIONAL_CONFIG_COMMENT


@pytest.fixture()
def project_help_comment():
    PROJECT_HELP_COMMENT = """
# Welcome to Great Expectations! Always know what to expect from your data.
#
# Here you can define datasources, batch kwargs generators, integrations and
# more. This file is intended to be committed to your repo. For help with
# configuration please:
#   - Read our docs: https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/connect_to_data_overview/#2-configure-your-datasource
#   - Join our slack channel: http://greatexpectations.io/slack

# config_version refers to the syntactic version of this config file, and is used in maintaining backwards compatibility
# It is auto-generated and usually does not need to be changed.
config_version: 4
"""  # noqa: E501
    return PROJECT_HELP_COMMENT


@pytest.mark.unit
def test_project_optional_config_comment_matches_default(
    project_optional_config_comment,
):
    """
    What does this test and why?
    Make sure that the templates built on data_context.types.base.DataContextConfigDefaults match the desired default.
    """  # noqa: E501

    assert project_optional_config_comment == templates.PROJECT_OPTIONAL_CONFIG_COMMENT


@pytest.mark.unit
def test_project_help_comment_matches_default(project_help_comment):
    """
    What does this test and why?
    Make sure that the templates built on data_context.types.base.DataContextConfigDefaults match the desired default.
    """  # noqa: E501

    assert project_help_comment == templates.PROJECT_HELP_COMMENT
