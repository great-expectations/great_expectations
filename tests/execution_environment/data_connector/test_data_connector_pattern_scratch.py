import pytest
import yaml
import json

from typing import List

from great_expectations.execution_environment.data_connector import DataConnector

from great_expectations.execution_environment.data_connector.sorter import (
    Sorter,
    LexicographicSorter,
    DateTimeSorter,
    NumericSorter,
)

from great_expectations.data_context.util import (
    instantiate_class_from_config,
)
from tests.test_utils import (
    create_files_in_directory,
)

from great_expectations.core.batch import (
    BatchRequest,
    BatchDefinition,
    PartitionRequest,
    PartitionDefinition,
)


# TODO: Abe 20201028 : This test should actually be implemented with a FilesDataConnector, not a SinglePartitionDataConnector
# moved over
def test_example_with_explicit_data_asset_names(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("basic_data_connector__filesystem_data_connector"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "alpha/files/go/here/alpha-202001.csv",
            "alpha/files/go/here/alpha-202002.csv",
            "alpha/files/go/here/alpha-202003.csv",
            "beta_here/beta-202001.txt",
            "beta_here/beta-202002.txt",
            "beta_here/beta-202003.txt",
            "beta_here/beta-202004.txt",
            "gamma-202001.csv",
            "gamma-202002.csv",
            "gamma-202003.csv",
            "gamma-202004.csv",
            "gamma-202005.csv",    ]
    )

    my_data_connector_yaml = yaml.load(f"""
            class_name: FilesDataConnector
            execution_environment_name: test_environment
            execution_engine:
                BASE_ENGINE:
                class_name: PandasExecutionEngine
            class_name: FilesDataConnector
            base_directory: {base_directory}
            glob_directive: '*'
            default_regex:
                pattern: ^(.+)-(\\d{{4}})(\\d{{2}})\\.(csv|txt)$
                group_names:
                    - data_asset_name
                    - year_dir
                    - month_dir
            assets:
                alpha:
                    base_directory: alpha/files/go/here/
                beta:
                    base_directory: beta_here/
                    glob_directive: '*.txt'
                gamma:
                    base_directory: ""
        """, Loader=yaml.FullLoader)

    my_data_connector: DataConnector = instantiate_class_from_config(
        config=my_data_connector_yaml,
        runtime_environment={
            "name": "my_data_connector",
            "execution_environment_name": "test_environment",
            "data_context_root_directory": base_directory,
            "execution_engine": "BASE_ENGINE",
        },
        config_defaults={
            "module_name": "great_expectations.execution_environment.data_connector"
        },
    )
    my_data_connector.refresh_data_references_cache()
    report = my_data_connector.self_check()
    #
    # # I'm starting to think we might want to separate out this behavior into a different class.
    assert len(my_data_connector.get_unmatched_data_references()) == 0
    assert len(my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
        data_connector_name="my_data_connector",
        data_asset_name="alpha",
    ))) == 3

    assert len(my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
        data_connector_name="my_data_connector",
        data_asset_name="beta",
    ))) == 4
    assert len(my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
        data_connector_name="my_data_connector",
        data_asset_name="gamma",
    ))) == 5

