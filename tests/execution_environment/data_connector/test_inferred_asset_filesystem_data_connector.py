import pytest
import yaml

from typing import List

from great_expectations.execution_environment.data_connector import InferredAssetFilesystemDataConnector
from great_expectations.core.batch import (
    BatchDefinition,
    BatchRequest,
    PartitionDefinition,
)
from great_expectations.data_context.util import instantiate_class_from_config
from tests.test_utils import (
    create_fake_data_frame,
    create_files_in_directory,
)
import great_expectations.exceptions.exceptions as ge_exceptions


# TODO: Abe 20201028 : This test should actually be implemented with a ConfiguredAssetFilesystemDataConnector, not a InferredAssetFilesystemDataConnector
# def test_example_with_explicit_data_asset_names(tmp_path_factory):
#     data_reference_dict = dict([
#         (data_reference, create_fake_data_frame)
#         for data_reference in [
#             "my_base_directory/alpha/files/go/here/alpha-202001.csv",
#             "my_base_directory/alpha/files/go/here/alpha-202002.csv",
#             "my_base_directory/alpha/files/go/here/alpha-202003.csv",
#             "my_base_directory/beta_here/beta-202001.txt",
#             "my_base_directory/beta_here/beta-202002.txt",
#             "my_base_directory/beta_here/beta-202003.txt",
#             "my_base_directory/beta_here/beta-202004.txt",
#             "my_base_directory/gamma-202001.csv",
#             "my_base_directory/gamma-202002.csv",
#             "my_base_directory/gamma-202003.csv",
#             "my_base_directory/gamma-202004.csv",
#             "my_base_directory/gamma-202005.csv",
#         ]
#     ])

#     yaml_string = """
# class_name: InferredAssetFilesystemDataConnector
# execution_environment_name: FAKE_EXECUTION_ENVIRONMENT_NAME
# base_directory: my_base_directory/
# # glob_directive: "*.csv"
# default_regex:
#     pattern: ^.*\\/(.+)-(\\d{4})(\\d{2})\\.(csv|txt)$
#     group_names:
#         - data_asset_name
#         - year_dir
#         - month_dir
# assets:
#     alpha:
#         base_directory: alpha/files/go/here/
#     beta:
#         base_directory: beta_here/
#         # glob_directive: "*.txt"
#     gamma:
#         base_directory: ""

#     """
#     config = yaml.load(yaml_string, Loader=yaml.FullLoader)
#     config["data_reference_dict"] = data_reference_dict
#     my_data_connector = instantiate_class_from_config(
#         config,
#         config_defaults={"module_name": "great_expectations.execution_environment.data_connector"},
#         runtime_environment={"name": "my_data_connector"},
#     )

#     noinspection PyProtectedMember
#     my_data_connector._refresh_data_references_cache()

#     # I'm starting to think we might want to separate out this behavior into a different class.
#     assert len(my_data_connector.get_unmatched_data_references()) == 0
#     assert len(my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
#         data_connector_name="my_data_connector",
#         data_asset_name="alpha",
#     ))) == 3

#     assert len(my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
#         data_connector_name="my_data_connector",
#         data_asset_name="beta",
#     ))) == 4
#     assert len(my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
#         data_connector_name="my_data_connector",
#         data_asset_name="gamma",
#     ))) == 5


def test_test_yaml_config_(empty_data_context, tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("test_test_yaml_config"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "2020/01/alpha-1001.csv",
            "2020/01/beta-1002.csv",
            "2020/02/alpha-1003.csv",
            "2020/02/beta-1004.csv",
            "2020/03/alpha-1005.csv",
            "2020/03/beta-1006.csv",
            "2020/04/beta-1007.csv",
        ],
    )

    return_object = empty_data_context.test_yaml_config(f"""
module_name: great_expectations.execution_environment.data_connector
class_name: InferredAssetFilesystemDataConnector
execution_environment_name: FAKE_EXECUTION_ENVIRONMENT
name: TEST_DATA_CONNECTOR

base_directory: {base_directory}/
glob_directive: "*/*/*.csv"

default_regex:
    pattern: (\\d{{4}})/(\\d{{2}})/(.*)-.*\\.csv
    group_names:
        - year_dir
        - month_dir
        - data_asset_name
    """, return_mode="return_object")

    assert return_object == {
        "class_name": "InferredAssetFilesystemDataConnector",
        "data_asset_count": 2,
        "example_data_asset_names": [
            "alpha",
            "beta"
        ],
        "data_assets": {
            "alpha": {
                "example_data_references": ["2020/01/alpha-*.csv", "2020/02/alpha-*.csv", "2020/03/alpha-*.csv"],
                "batch_definition_count": 3
            },
            "beta": {
                "example_data_references": ["2020/01/beta-*.csv", "2020/02/beta-*.csv", "2020/03/beta-*.csv"],
                "batch_definition_count": 4
            }
        },
        "example_unmatched_data_references": [],
        "unmatched_data_reference_count": 0,
    }


def test_test_yaml_config_excluding_non_regex_matching_files(
    empty_data_context, tmp_path_factory
):
    base_directory = str(tmp_path_factory.mktemp("test_something_needs_a_better_name"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "2020/01/alpha-1001.csv",
            "2020/01/beta-1002.csv",
            "2020/02/alpha-1003.csv",
            "2020/02/beta-1004.csv",
            "2020/03/alpha-1005.csv",
            "2020/03/beta-1006.csv",
            "2020/04/beta-1007.csv",
            "gamma-202001.csv",
            "gamma-202002.csv",
        ],
    )

    # gamma-202001.csv and gamma-202002.csv do not match regex (which includes 2020/month directory).

    return_object = empty_data_context.test_yaml_config(
        f"""
module_name: great_expectations.execution_environment.data_connector
class_name: InferredAssetFilesystemDataConnector
execution_environment_name: FAKE_EXECUTION_ENVIRONMENT
name: TEST_DATA_CONNECTOR

base_directory: {base_directory}/
glob_directive: "*/*/*.csv"

default_regex:
    pattern: (\\d{{4}})/(\\d{{2}})/(.*)-.*\\.csv
    group_names:
        - year_dir
        - month_dir
        - data_asset_name
    """,
        return_mode="return_object",
    )

    assert return_object == {
        "class_name": "InferredAssetFilesystemDataConnector",
        "data_asset_count": 2,
        "example_data_asset_names": [
            "alpha",
            "beta"
        ],
        "data_assets": {
            "alpha": {
                "example_data_references": ["2020/01/alpha-*.csv", "2020/02/alpha-*.csv", "2020/03/alpha-*.csv"],
                "batch_definition_count": 3
            },
            "beta": {
                "example_data_references": ["2020/01/beta-*.csv", "2020/02/beta-*.csv", "2020/03/beta-*.csv"],
                "batch_definition_count": 4
            }
        },
        "example_unmatched_data_references": [],
        "unmatched_data_reference_count": 0,
    }


def test_nested_directory_data_asset_name_in_folder(empty_data_context, tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("test_dir_charlie"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "A/A-1.csv",
            "A/A-2.csv",
            "A/A-3.csv",
            "B/B-1.csv",
            "B/B-2.csv",
            "B/B-3.csv",
            "C/C-1.csv",
            "C/C-2.csv",
            "C/C-3.csv",
            "D/D-1.csv",
            "D/D-2.csv",
            "D/D-3.csv",
        ]
    )

    return_object = empty_data_context.test_yaml_config(f"""
    module_name: great_expectations.execution_environment.data_connector
    class_name: InferredAssetFilesystemDataConnector
    execution_environment_name: FAKE_EXECUTION_ENVIRONMENT
    name: TEST_DATA_CONNECTOR
    base_directory: {base_directory}/
    glob_directive: "*/*.csv"
    default_regex:
        group_names:
            - data_asset_name
            - letter
            - number
        pattern: (\\w{{1}})\\/(\\w{{1}})-(\\d{{1}})\\.csv
        """, return_mode="return_object")

    assert return_object == {
        "class_name": "InferredAssetFilesystemDataConnector",
        "data_asset_count": 4,
        "example_data_asset_names": [
             "A",
             "B",
             "C"
        ],
        "data_assets": {
            "A": {
                "batch_definition_count": 3,
                "example_data_references": ["A/A-1.csv", "A/A-2.csv", "A/A-3.csv"]
            },
            "B": {
                "batch_definition_count": 3,
                "example_data_references": ["B/B-1.csv", "B/B-2.csv", "B/B-3.csv"]
            },
            "C": {
                "batch_definition_count": 3,
                "example_data_references": ["C/C-1.csv", "C/C-2.csv", "C/C-3.csv"]
            }
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": []
    }


def test_redundant_information_in_naming_convention_random_hash(empty_data_context, tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("logs"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "2021/01/01/log_file-2f1e94b40f310274b485e72050daf591.txt.gz",
            "2021/01/02/log_file-7f5d35d4f90bce5bf1fad680daac48a2.txt.gz",
            "2021/01/03/log_file-99d5ed1123f877c714bbe9a2cfdffc4b.txt.gz",
            "2021/01/04/log_file-885d40a5661bbbea053b2405face042f.txt.gz",
            "2021/01/05/log_file-d8e478f817b608729cfc8fb750ebfc84.txt.gz",
            "2021/01/06/log_file-b1ca8d1079c00fd4e210f7ef31549162.txt.gz",
            "2021/01/07/log_file-d34b4818c52e74b7827504920af19a5c.txt.gz",
        ]
    )

    return_object = empty_data_context.test_yaml_config(f"""
          module_name: great_expectations.execution_environment.data_connector
          class_name: InferredAssetFilesystemDataConnector
          execution_environment_name: FAKE_EXECUTION_ENVIRONMENT
          name: TEST_DATA_CONNECTOR
          base_directory: {base_directory}/
          glob_directive: "*/*/*/*.txt.gz"
          default_regex:
              group_names:
                - year
                - month
                - day
                - data_asset_name
              pattern: (\\d{{4}})/(\\d{{2}})/(\\d{{2}})/(log_file)-.*\\.txt\\.gz

              """, return_mode="return_object")

    assert return_object == {
        "class_name": "InferredAssetFilesystemDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": [
            "log_file"
        ],
        "data_assets": {
            "log_file": {
                "batch_definition_count": 7,
                "example_data_references": ["2021/01/01/log_file-*.txt.gz",
                                            "2021/01/02/log_file-*.txt.gz",
                                            "2021/01/03/log_file-*.txt.gz"]
            }
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": []
    }


# def test_redundant_information_in_naming_convention_random_hash_asset_name_is_date(empty_data_context, tmp_path_factory):
#     base_directory = str(tmp_path_factory.mktemp("logs"))
#     create_files_in_directory(
#         directory=base_directory,
#         file_name_list=[
#             "2021/01/01/log_file-2f1e94b40f310274b485e72050daf591.txt.gz",
#             "2021/01/01/log_file-8277e0910d750195b448797616e091ad.txt.gz",
#             "2021/01/02/log_file-7f5d35d4f90bce5bf1fad680daac48a2.txt.gz",
#             "2021/01/02/log_file-912ec803b2ce49e4a541068d495ab570.txt.gz",
#             "2021/01/03/log_file-99d5ed1123f877c714bbe9a2cfdffc4b.txt.gz",
#             "2021/01/03/log_file-45901b519281e201d4535cb90678d870.txt.gz",
#             "2021/01/04/log_file-885d40a5661bbbea053b2405face042f.txt.gz",
#             "2021/01/04/log_file-103935fb414d693ba3a5f01a9d9399d3.txt.gz",
#             "2021/01/05/log_file-6e232cfb9357a98911d9794d0b0eb804.txt.gz",
#             "2021/01/05/log_file-d8e478f817b608729cfc8fb750ebfc84.txt.gz",
#             "2021/01/06/log_file-4786f3282f04de5b5c7317c490c6d922.txt.gz",
#             "2021/01/06/log_file-b1ca8d1079c00fd4e210f7ef31549162.txt.gz",
#             "2021/01/07/log_file-d34b4818c52e74b7827504920af19a5c.txt.gz",
#             "2021/01/07/log_file-a21075a36eeddd084e17611a238c7101.txt.gz",
#         ]
#     )
#     return_object = empty_data_context.test_yaml_config(f
#           module_name: great_expectations.execution_environment.data_connector
#           class_name: InferredAssetFilesystemDataConnector
#           execution_environment_name: FAKE_EXECUTION_ENVIRONMENT
#           name: TEST_DATA_CONNECTOR
#           base_directory: {base_directory}/
#           glob_directive: "*/*/*/*.txt.gz"
#           partitioner:
#               class_name: RegexPartitioner
#               group_names:
#                 - data_asset_name
#               pattern: (\\d{{4}}\\/\\d{{2}}\\/\\d{{2}})/log_file-.*\\.txt\\.gz
#               , return_mode="return_object")
#
#     return_object == {
#         "class_name": "InferredAssetFilesystemDataConnector",
#         "data_asset_count": 7,
#         "example_data_asset_names": [
#             "2021/01/01",
#             "2021/01/02",
#             "2021/01/03"
#         ],
#         "data_assets": {
#             "2021/01/01": {
#                 "batch_definition_count": 2,
#                 "example_data_references": ["2021/01/01/log_file-*.txt.gz", "2021/01/01/log_file-*.txt.gz"]
#             },
#             "2021/01/02": {
#                 "batch_definition_count": 2,
#                 "example_data_references": ["2021/01/02/log_file-*.txt.gz", "2021/01/02/log_file-*.txt.gz"]
#             },
#             "2021/01/03": {
#                 "batch_definition_count": 2,
#                 "example_data_references": ["2021/01/03/log_file-*.txt.gz", "2021/01/03/log_file-*.txt.gz"]
#             }
#         },
#         "unmatched_data_reference_count": 0,
#         "example_unmatched_data_references": []
#     }


def test_redundant_information_in_naming_convention_timestamp(empty_data_context, tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("logs"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "log_file-2021-01-01-035419.163324.txt.gz",
            "log_file-2021-01-02-035513.905752.txt.gz",
            "log_file-2021-01-03-035455.848839.txt.gz",
            "log_file-2021-01-04-035251.47582.txt.gz",
            "log_file-2021-01-05-033034.289789.txt.gz",
            "log_file-2021-01-06-034958.505688.txt.gz",
            "log_file-2021-01-07-033545.600898.txt.gz",
        ]
    )

    return_object = empty_data_context.test_yaml_config(f"""
          module_name: great_expectations.execution_environment.data_connector
          class_name: InferredAssetFilesystemDataConnector
          execution_environment_name: FAKE_EXECUTION_ENVIRONMENT
          name: TEST_DATA_CONNECTOR
          base_directory: {base_directory}/
          glob_directive: "*.txt.gz"
          default_regex:
              group_names:
                - data_asset_name
                - year
                - month
                - day
              pattern: (log_file)-(\\d{{4}})-(\\d{{2}})-(\\d{{2}})-.*\\.*\\.txt\\.gz
      """, return_mode="return_object")
    assert return_object == {
        "class_name": "InferredAssetFilesystemDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": [
            "log_file"
        ],
        "data_assets": {
            "log_file": {
                "batch_definition_count": 7,
                "example_data_references": [
                    "log_file-2021-01-01-*.txt.gz", "log_file-2021-01-02-*.txt.gz", "log_file-2021-01-03-*.txt.gz"
                ]
            }
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": []
    }


def test_redundant_information_in_naming_convention_bucket(empty_data_context, tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("logs"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "some_bucket/2021/01/01/log_file-20210101.txt.gz",
            "some_bucket/2021/01/02/log_file-20210102.txt.gz",
            "some_bucket/2021/01/03/log_file-20210103.txt.gz",
            "some_bucket/2021/01/04/log_file-20210104.txt.gz",
            "some_bucket/2021/01/05/log_file-20210105.txt.gz",
            "some_bucket/2021/01/06/log_file-20210106.txt.gz",
            "some_bucket/2021/01/07/log_file-20210107.txt.gz",
        ]
    )

    return_object = empty_data_context.test_yaml_config(f"""
          module_name: great_expectations.execution_environment.data_connector
          class_name: InferredAssetFilesystemDataConnector
          execution_environment_name: FAKE_EXECUTION_ENVIRONMENT
          name: TEST_DATA_CONNECTOR
          base_directory: {base_directory}/
          glob_directive: "*/*/*/*/*.txt.gz"
          default_regex:
              group_names:
                  - data_asset_name
                  - year
                  - month
                  - day
              pattern: (\\w{{11}})/(\\d{{4}})/(\\d{{2}})/(\\d{{2}})/log_file-.*\\.txt\\.gz
              """, return_mode="return_object")

    assert return_object == {
        "class_name": "InferredAssetFilesystemDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": [
            "some_bucket"
        ],
        "data_assets": {
            "some_bucket": {
                "batch_definition_count": 7,
                "example_data_references": [
                    "some_bucket/2021/01/01/log_file-*.txt.gz",
                    "some_bucket/2021/01/02/log_file-*.txt.gz",
                    "some_bucket/2021/01/03/log_file-*.txt.gz"
                ]
            }
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": []
    }


def test_redundant_information_in_naming_convention_bucket_sorted(empty_data_context, tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("logs"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "some_bucket/2021/01/01/log_file-20210101.txt.gz",
            "some_bucket/2021/01/02/log_file-20210102.txt.gz",
            "some_bucket/2021/01/03/log_file-20210103.txt.gz",
            "some_bucket/2021/01/04/log_file-20210104.txt.gz",
            "some_bucket/2021/01/05/log_file-20210105.txt.gz",
            "some_bucket/2021/01/06/log_file-20210106.txt.gz",
            "some_bucket/2021/01/07/log_file-20210107.txt.gz",
        ]
    )

    my_data_connector_yaml = yaml.load(f"""
          module_name: great_expectations.execution_environment.data_connector
          class_name: InferredAssetFilesystemDataConnector
          execution_environment_name: test_environment
          name: single_partitioner_data_connector
          base_directory: {base_directory}/
          glob_directive: "*/*/*/*/*.txt.gz"
          default_regex:
              group_names:
                  - data_asset_name
                  - year
                  - month
                  - day
                  - full_date
              pattern: (\\w{{11}})/(\\d{{4}})/(\\d{{2}})/(\\d{{2}})/log_file-(.*)\\.txt\\.gz
          sorters:
              - orderby: desc
                class_name: DateTimeSorter
                name: full_date

          """, Loader=yaml.FullLoader)

    my_data_connector: InferredAssetFilesystemDataConnector = instantiate_class_from_config(
        config=my_data_connector_yaml,
        runtime_environment={
            "name": "single_partitioner_data_connector",
            "execution_environment_name": "test_environment",
            "data_context_root_directory": base_directory,
            "execution_engine": "BASE_ENGINE",
        },
        config_defaults={
            "module_name": "great_expectations.execution_environment.data_connector"
        },
    )

    sorted_batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
        execution_environment_name="test_environment",
        data_connector_name="single_partitioner_data_connector",
        data_asset_name="some_bucket",
    ))

    expected = [
        BatchDefinition(execution_environment_name="test_environment",
                        data_connector_name="single_partitioner_data_connector",
                        data_asset_name="some_bucket",
                        partition_definition=PartitionDefinition(
                            {'year': '2021', 'month': '01', 'day': '07', 'full_date': '20210107'}
                        )),
        BatchDefinition(execution_environment_name="test_environment",
                        data_connector_name="single_partitioner_data_connector",
                        data_asset_name="some_bucket",
                        partition_definition=PartitionDefinition(
                            {'year': '2021', 'month': '01', 'day': '06', 'full_date': '20210106'}
                        )),
        BatchDefinition(execution_environment_name="test_environment",
                        data_connector_name="single_partitioner_data_connector",
                        data_asset_name="some_bucket",
                        partition_definition=PartitionDefinition(
                            {'year': '2021', 'month': '01', 'day': '05', 'full_date': '20210105'}
                        )),
        BatchDefinition(execution_environment_name="test_environment",
                        data_connector_name="single_partitioner_data_connector",
                        data_asset_name="some_bucket",
                        partition_definition=PartitionDefinition(
                            {'year': '2021', 'month': '01', 'day': '04', 'full_date': '20210104'}
                        )),
        BatchDefinition(execution_environment_name="test_environment",
                        data_connector_name="single_partitioner_data_connector",
                        data_asset_name="some_bucket",
                        partition_definition=PartitionDefinition(
                            {'year': '2021', 'month': '01', 'day': '03', 'full_date': '20210103'}
                        )),
        BatchDefinition(execution_environment_name="test_environment",
                        data_connector_name="single_partitioner_data_connector",
                        data_asset_name="some_bucket",
                        partition_definition=PartitionDefinition(
                            {'year': '2021', 'month': '01', 'day': '02', 'full_date': '20210102'}
                        )),
        BatchDefinition(execution_environment_name="test_environment",
                        data_connector_name="single_partitioner_data_connector",
                        data_asset_name="some_bucket",
                        partition_definition=PartitionDefinition(
                            {'year': '2021', 'month': '01', 'day': '01', 'full_date': '20210101'}
                        ))
    ]
    assert expected == sorted_batch_definition_list


def test_redundant_information_in_naming_convention_bucket_sorter_does_not_match_group(empty_data_context, tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("logs"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "some_bucket/2021/01/01/log_file-20210101.txt.gz",
            "some_bucket/2021/01/02/log_file-20210102.txt.gz",
            "some_bucket/2021/01/03/log_file-20210103.txt.gz",
            "some_bucket/2021/01/04/log_file-20210104.txt.gz",
            "some_bucket/2021/01/05/log_file-20210105.txt.gz",
            "some_bucket/2021/01/06/log_file-20210106.txt.gz",
            "some_bucket/2021/01/07/log_file-20210107.txt.gz",
        ]
    )

    my_data_connector_yaml = yaml.load(f"""
          module_name: great_expectations.execution_environment.data_connector
          class_name: InferredAssetFilesystemDataConnector
          execution_environment_name: test_environment
          name: single_partitioner_data_connector
          base_directory: {base_directory}/
          glob_directive: "*/*/*/*/*.txt.gz"
          default_regex:
              group_names:
                  - data_asset_name
                  - year
                  - month
                  - day
                  - full_date
              pattern: (\\w{{11}})/(\\d{{4}})/(\\d{{2}})/(\\d{{2}})/log_file-(.*)\\.txt\\.gz
          sorters:
              - orderby: desc
                class_name: DateTimeSorter
                name: not_matching_anything

          """, Loader=yaml.FullLoader)

    my_data_connector: InferredAssetFilesystemDataConnector = instantiate_class_from_config(
        config=my_data_connector_yaml,
        runtime_environment={
            "name": "single_partitioner_data_connector",
            "execution_environment_name": "test_environment",
            "data_context_root_directory": base_directory,
            "execution_engine": "BASE_ENGINE",
        },
        config_defaults={
            "module_name": "great_expectations.execution_environment.data_connector"
        },
    )

    with pytest.raises(ge_exceptions.DataConnectorError):
        # noinspection PyUnusedLocal
        sorted_batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
            execution_environment_name="test_environment",
            data_connector_name="single_partitioner_data_connector",
            data_asset_name="some_bucket",
        ))


def test_redundant_information_in_naming_convention_bucket_too_many_sorters(empty_data_context, tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("logs"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "some_bucket/2021/01/01/log_file-20210101.txt.gz",
            "some_bucket/2021/01/02/log_file-20210102.txt.gz",
            "some_bucket/2021/01/03/log_file-20210103.txt.gz",
            "some_bucket/2021/01/04/log_file-20210104.txt.gz",
            "some_bucket/2021/01/05/log_file-20210105.txt.gz",
            "some_bucket/2021/01/06/log_file-20210106.txt.gz",
            "some_bucket/2021/01/07/log_file-20210107.txt.gz",
        ]
    )

    my_data_connector_yaml = yaml.load(f"""
        module_name: great_expectations.execution_environment.data_connector
        class_name: InferredAssetFilesystemDataConnector
        execution_environment_name: test_environment
        name: single_partitioner_data_connector
        base_directory: {base_directory}/
        glob_directive: "*/*/*/*/*.txt.gz"
        default_regex:
            group_names:
                - data_asset_name
                - year
                - month
                - day
                - full_date
            pattern: (\\w{{11}})/(\\d{{4}})/(\\d{{2}})/(\\d{{2}})/log_file-(.*)\\.txt\\.gz
        sorters:
            - datetime_format: '%Y%m%d'
              orderby: desc
              class_name: DateTimeSorter
              name: timestamp
            - orderby: desc
              class_name: NumericSorter
              name: price
          """, Loader=yaml.FullLoader)

    my_data_connector: InferredAssetFilesystemDataConnector = instantiate_class_from_config(
        config=my_data_connector_yaml,
        runtime_environment={
            "name": "single_partitioner_data_connector",
            "execution_environment_name": "test_environment",
            "data_context_root_directory": base_directory,
            "execution_engine": "BASE_ENGINE",
        },
        config_defaults={
            "module_name": "great_expectations.execution_environment.data_connector"
        },
    )

    with pytest.raises(ge_exceptions.DataConnectorError):
        # noinspection PyUnusedLocal
        sorted_batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
            execution_environment_name="test_environment",
            data_connector_name="single_partitioner_data_connector",
            data_asset_name="some_bucket",
        ))
