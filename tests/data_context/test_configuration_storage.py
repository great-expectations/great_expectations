import logging
logger = logging.getLogger(__name__)
import pytest
from six import PY2
import os
import json

@pytest.fixture()
def data_context_config_string():
    config_str = \
        """
# This is a comment
# it should be preserved.
datasources:
  # this comments should also be preserved
  default:
    type: pandas
    generators:
      # The name default is read if no datasource or generator is specified
      default:
        type: filesystem
        base_dir: /data
"""


def test_preserve_comments(data_context):
    print(data_context._project_config)
    context_root_dir = data_context.root_directory

    config_filepath = os.path.join(context_root_dir, "great_expectations.yml")
    print(config_filepath)

    with open(config_filepath, "r") as infile:
        content = infile.read()

        # Test that the lines occur, but don't insist on exact ordering.
        # FIXME: This will be a problem for usability.
        # Developers shouldn't have to deal with their yml getting scrambled every time they edit the datacontext.
        content_lines = set(content.split("\n"))
        test_content_lines = set("""\
datasources:
  # For example, this one.
  mydatasource:
    module_name: great_expectations.datasource
    class_name: PandasDatasource
    data_asset_type:
      class_name: PandasDataset
    generators:
      # The name default is read if no datasource or generator is specified
      mygenerator:
        class_name: SubdirReaderGenerator
        base_directory: ../data
        reader_options:
          sep:
          engine: python


config_variables_file_path: uncommitted/config_variables.yml
expectations_store:
  class_name: ExpectationStore
  store_backend:
    class_name: FixedLengthTupleFilesystemStoreBackend
    base_directory: expectations/

plugins_directory: plugins/
evaluation_parameter_store_name: evaluation_parameter_store
data_docs:
  sites:
stores:
  evaluation_parameter_store:
    module_name: great_expectations.data_context.store
    class_name: EvaluationParameterStore

""".split("\n"))
        assert content_lines == test_content_lines


    print("++++++++++++++++++++++++++++++++++++++++")
    print(content)
    print("----------------------------------------")

    data_context.add_datasource("test_datasource",
                                module_name="great_expectations.datasource",
                                class_name="PandasDatasource",
                                base_directory="../data")

    with open(config_filepath, "r") as infile:

        content = infile.read()

        print("++++++++++++++++++++++++++++++++++++++++")
        print(content)
        print("----------------------------------------")

        # Test that the lines occur, but don't insist on exact ordering.
        # FIXME: This will be a problem for usability.
        # Developers shouldn't have to deal with their yml getting scrambled every time they edit the datacontext.
        content_lines = set(content.split("\n"))
        test_content_lines = set("""\
datasources:
  # For example, this one.
  mydatasource:
    module_name: great_expectations.datasource
    class_name: PandasDatasource
    data_asset_type:
      class_name: PandasDataset
    generators:
      # The name default is read if no datasource or generator is specified
      mygenerator:
        class_name: SubdirReaderGenerator
        base_directory: ../data
        reader_options:
          sep:
          engine: python


  test_datasource:
    module_name: great_expectations.datasource
    class_name: PandasDatasource
    data_asset_type:
      class_name: PandasDataset
    generators:
      default:
        class_name: SubdirReaderGenerator
        base_directory: ../data
        reader_options:
          sep:
          engine: python
config_variables_file_path: uncommitted/config_variables.yml
expectations_store:
  class_name: ExpectationStore
  store_backend:
    class_name: FixedLengthTupleFilesystemStoreBackend
    base_directory: expectations/

plugins_directory: plugins/
evaluation_parameter_store_name: evaluation_parameter_store
data_docs:
  sites:
stores:
  evaluation_parameter_store:
    module_name: great_expectations.data_context.store
    class_name: EvaluationParameterStore
""".split("\n"))
        assert content_lines == test_content_lines
