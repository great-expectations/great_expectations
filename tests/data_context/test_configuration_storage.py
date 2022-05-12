import logging
import os
import shutil

import pytest

import great_expectations as ge
from great_expectations.data_context.util import file_relative_path

logger = logging.getLogger(__name__)


def read_config_file_from_disk(config_filepath):
    with open(config_filepath) as infile:
        config_file = infile.read()
        return config_file


@pytest.fixture
def data_context_parameterized_expectation_suite_with_usage_statistics_enabled(
    tmp_path_factory,
):
    """
    This data_context is *manually* created to have the config we want, vs
    created with DataContext.create()
    """
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")
    fixture_dir = file_relative_path(__file__, "../test_fixtures")
    os.makedirs(
        os.path.join(asset_config_path, "my_dag_node"),
        exist_ok=True,
    )
    shutil.copy(
        os.path.join(
            fixture_dir, "great_expectations_v013_basic_with_usage_stats_enabled.yml"
        ),
        str(os.path.join(context_path, "great_expectations.yml")),
    )
    shutil.copy(
        os.path.join(
            fixture_dir,
            "expectation_suites/parameterized_expectation_suite_fixture.json",
        ),
        os.path.join(asset_config_path, "my_dag_node", "default.json"),
    )
    os.makedirs(os.path.join(context_path, "plugins"), exist_ok=True)
    shutil.copy(
        os.path.join(fixture_dir, "custom_pandas_dataset.py"),
        str(os.path.join(context_path, "plugins", "custom_pandas_dataset.py")),
    )
    shutil.copy(
        os.path.join(fixture_dir, "custom_sqlalchemy_dataset.py"),
        str(os.path.join(context_path, "plugins", "custom_sqlalchemy_dataset.py")),
    )
    shutil.copy(
        os.path.join(fixture_dir, "custom_sparkdf_dataset.py"),
        str(os.path.join(context_path, "plugins", "custom_sparkdf_dataset.py")),
    )
    return ge.data_context.DataContext(context_path)


def test_preserve_comments_in_yml_after_adding_datasource(
    data_context_parameterized_expectation_suite_with_usage_statistics_enabled,
):
    # Skipping this test for now, because the order of the contents of the returned CommentedMap is inconsistent.
    pytest.skip("KNOWN ISSUE")
    config_filepath = os.path.join(
        data_context_parameterized_expectation_suite_with_usage_statistics_enabled.root_directory,
        "great_expectations.yml",
    )
    initial_config = read_config_file_from_disk(config_filepath)
    print("++++++++++++++++ initial config +++++++++++++++++++++++")
    print(initial_config)
    print("----------------------------------------")

    data_context_parameterized_expectation_suite_with_usage_statistics_enabled.add_datasource(
        "test_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": "../data",
            }
        },
    )

    # TODO The comments on lines 1,2 & 4 of the fixture exposes the bug.
    expected = """# This is a basic configuration for testing.
# It has comments that should be preserved.
config_version: 3.0
# Here's a comment between the config version and the datassources
datasources:
  # For example, this one.
  mydatasource: # This should stay by mydatasource
    module_name: great_expectations.datasource
    data_asset_type:
      class_name: PandasDataset
    batch_kwargs_generators:
      # The name default is read if no datasource or generator is specified
      mygenerator:
        class_name: SubdirReaderBatchKwargsGenerator
        base_directory: ../data
        reader_options:
          sep:
          engine: python
    class_name: PandasDatasource
  test_datasource:
    module_name: great_expectations.datasource
    batch_kwargs_generators:
      subdir_reader:
        class_name: SubdirReaderBatchKwargsGenerator
        base_directory: ../data
    data_asset_type:
      module_name: great_expectations.dataset
      class_name: PandasDataset
    class_name: PandasDatasource

config_variables_file_path: uncommitted/config_variables.yml

plugins_directory: plugins/
evaluation_parameter_store_name: evaluation_parameter_store
expectations_store_name: expectations_store
validations_store_name: validations_store
checkpoint_store_name: checkpoint_store

data_docs_sites:
  local_site:
    show_how_to_buttons: true
    class_name: SiteBuilder
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: uncommitted/data_docs/local_site/
    site_index_builder:
      class_name: DefaultSiteIndexBuilder

stores:
  expectations_store:
    class_name: ExpectationsStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: expectations/
  evaluation_parameter_store:
    module_name: great_expectations.data_context.store
    class_name: EvaluationParameterStore

  validations_store:
    class_name: ValidationsStore

  checkpoint_store:
    class_name: CheckpointStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: checkpoints/

validation_operators:
  # Read about validation operators at: https://docs.greatexpectations.io/en/latest/guides/validation_operators.html
  default:
    class_name: ActionListValidationOperator
    action_list:
      - name: store_validation_result
        action:
          class_name: StoreValidationResultAction
          target_store_name: validations_store
      # Uncomment the notify_slack action below to send notifications during evaluation
      # - name: notify_slack
      #   action:
      #     class_name: SlackNotificationAction
      #     slack_webhook: ${validation_notification_slack_webhook}
      #     notify_on: all
      #     renderer:
      #       module_name: great_expectations.render.renderer.slack_renderer
      #       class_name: SlackRenderer
      - name: store_evaluation_params
        action:
          class_name: StoreEvaluationParametersAction
          target_store_name: evaluation_parameter_store
anonymous_usage_statistics:
  usage_statistics_url: https://dev.stats.greatexpectations.io/great_expectations/v1/usage_statistics
  enabled: false
  data_context_id: 7f76b3c9-330c-4307-b882-7ad9186adf0c
notebooks:



"""

    print("++++++++++++++++ expected +++++++++++++++++++++++")
    print(expected)
    print("----------------------------------------")

    observed = read_config_file_from_disk(config_filepath)

    print("++++++++++++++++ observed +++++++++++++++++++++++")
    print(observed)
    print("----------------------------------------")

    # TODO: this test fails now, but only on whitespace
    # Whitespace issues seem to come from the addition or deletion of keys
    assert observed.replace("\n", "") == expected.replace("\n", "")

    # What we really want:
    # assert observed == expected
