import yaml
from great_expectations.execution_environment.data_connector import (
    SinglePartitionDictDataConnector,
)

from great_expectations.core.batch import (
    BatchRequest,
    BatchDefinition,
    PartitionDefinition,
)

from great_expectations.data_context.util import (
    instantiate_class_from_config
)
from tests.test_utils import (
    create_fake_data_frame,
    create_files_in_directory,
)





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
          class_name: SinglePartitionFileDataConnector
          execution_environment_name: FAKE_EXECUTION_ENVIRONMENT
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



    my_data_connector = instantiate_class_from_config(
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

    self_check_report = my_data_connector.self_check()

    sorted_batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
        execution_environment_name="test_environment",
        data_connector_name="single_partitioner_data_connector",
    ))


    print(sorted_batch_definition_list)


# <WILL> this wont work because the date time spans over multipe groups
# should this be allowed? if so how should it be configured?
def redundant_information_in_naming_convention_bucket_sorted(empty_data_context, tmp_path_factory):
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
          class_name: SinglePartitionFileDataConnector
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
          # <WILL> this wont work
