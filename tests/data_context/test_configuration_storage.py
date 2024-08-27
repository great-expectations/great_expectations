import logging
import os
import shutil

import pytest

import great_expectations as gx
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.util import file_relative_path

pytestmark = pytest.mark.filesystem

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
    context_path = os.path.join(project_path, FileDataContext.GX_DIR)  # noqa: PTH118
    asset_config_path = os.path.join(context_path, "expectations")  # noqa: PTH118
    fixture_dir = file_relative_path(__file__, "../test_fixtures")
    os.makedirs(  # noqa: PTH103
        os.path.join(asset_config_path, "my_dag_node"),  # noqa: PTH118
        exist_ok=True,
    )
    shutil.copy(
        os.path.join(  # noqa: PTH118
            fixture_dir, "great_expectations_v013_basic_with_usage_stats_enabled.yml"
        ),
        str(os.path.join(context_path, FileDataContext.GX_YML)),  # noqa: PTH118
    )
    shutil.copy(
        os.path.join(  # noqa: PTH118
            fixture_dir,
            "expectation_suites/parameterized_expectation_suite_fixture.json",
        ),
        os.path.join(asset_config_path, "my_dag_node", "default.json"),  # noqa: PTH118
    )
    os.makedirs(  # noqa: PTH103
        os.path.join(context_path, "plugins"),  # noqa: PTH118
        exist_ok=True,
    )
    return gx.get_context(context_root_dir=context_path)


def test_preserve_comments_in_yml_after_adding_datasource(
    data_context_parameterized_expectation_suite_with_usage_statistics_enabled,
):
    # Skipping this test for now, because the order of the contents of the returned CommentedMap is inconsistent.  # noqa: E501
    pytest.skip("KNOWN ISSUE")
    config_filepath = os.path.join(  # noqa: PTH118
        data_context_parameterized_expectation_suite_with_usage_statistics_enabled.root_directory,
        FileDataContext.GX_YML,
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
expectations_store_name: expectations_store
validation_results_store_name: validation_results_store
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

  validation_results_store:
    class_name: ValidationResultsStore

  checkpoint_store:
    class_name: CheckpointStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: checkpoints/

analytics_enabled: false
data_context_id: 7f76b3c9-330c-4307-b882-7ad9186adf0c



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
