from __future__ import annotations

import os
import pathlib
from typing import TYPE_CHECKING, List

import boto3
import botocore
import pytest

from great_expectations.core.batch import BatchDefinition, BatchRequest
from great_expectations.core.batch_spec import PathBatchSpec
from great_expectations.core.id_dict import BatchSpec
from great_expectations.datasource.data_connector import InferredAssetDBFSDataConnector
from great_expectations.execution_engine import PandasExecutionEngine
from tests.test_utils import create_files_in_directory

if TYPE_CHECKING:
    from pyfakefs.fake_filesystem import FakeFilesystem


@pytest.mark.integration
def test__get_full_file_path_pandas(fs: FakeFilesystem):
    """
    What does this test and why?
    File paths in DBFS need to use the `dbfs:/` protocol base instead of `/dbfs/` when
    being read using the `spark.read` method in the ExecutionEngine. HOWEVER when using a
    PandasExecutionEngine the file semantic `/dbfs/` version must be used instead.
    This test verifies that a config using a `/dbfs/` path is NOT translated to `dbfs:/`
    when preparing the PathBatchSpec for the PandasExecutionEngine.
    """
    # Copy boto modules into fake filesystem (see https://github.com/spulec/moto/issues/1682#issuecomment-645016188)
    for module in [boto3, botocore]:
        module_dir = pathlib.Path(module.__file__).parent
        fs.add_real_directory(module_dir, lazy_read=False)

    # Copy google credentials into fake filesystem if they exist on your filesystem
    google_cred_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if google_cred_file:
        fs.add_real_file(google_cred_file)

    base_directory: str = "/dbfs/great_expectations"
    base_directory_colon: str = "dbfs:/great_expectations"  # noqa: F841
    fs.create_dir(base_directory)

    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "path/A-100.csv",
            "path/A-101.csv",
            "directory/B-1.csv",
            "directory/B-2.csv",
        ],
    )

    my_data_connector: InferredAssetDBFSDataConnector = InferredAssetDBFSDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine=PandasExecutionEngine(),
        default_regex={
            "pattern": r"(.+)/(.+)-(\d+)\.csv",
            "group_names": ["data_asset_name", "letter", "number"],
        },
        glob_directive="*/*.csv",
        base_directory=base_directory,
    )

    # noinspection PyProtectedMember
    my_data_connector._refresh_data_references_cache()

    assert my_data_connector.get_data_reference_count() == 4
    assert my_data_connector.get_unmatched_data_references() == []

    my_batch_definition_list: List[
        BatchDefinition
    ] = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="FAKE_DATASOURCE_NAME",
            data_connector_name="my_data_connector",
            data_asset_name="path",
        )
    )
    assert len(my_batch_definition_list) == 2

    my_batch_definition: BatchDefinition = my_batch_definition_list[0]
    batch_spec: BatchSpec = my_data_connector.build_batch_spec(
        batch_definition=my_batch_definition
    )

    assert isinstance(batch_spec, PathBatchSpec)
    assert batch_spec.path == f"{base_directory}/path/A-100.csv"


@pytest.mark.integration
def test__get_full_file_path_spark(basic_spark_df_execution_engine, fs):
    """
    What does this test and why?
    File paths in DBFS need to use the `dbfs:/` protocol base instead of `/dbfs/` when
    being read using the `spark.read` method in the ExecutionEngine. In the data connector
    config however, the `/dbfs` version must be used. This test verifies that a config
    using a `/dbfs/` path is translated to `dbfs:/` when preparing the PathBatchSpec for the
    SparkDFExecutionEngine.
    """

    base_directory: str = "/dbfs/great_expectations"
    base_directory_colon: str = "dbfs:/great_expectations"
    fs.create_dir(base_directory)

    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "path/A-100.csv",
            "path/A-101.csv",
            "directory/B-1.csv",
            "directory/B-2.csv",
        ],
    )

    my_data_connector: InferredAssetDBFSDataConnector = InferredAssetDBFSDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine=basic_spark_df_execution_engine,
        default_regex={
            "pattern": r"(.+)/(.+)-(\d+)\.csv",
            "group_names": ["data_asset_name", "letter", "number"],
        },
        glob_directive="*/*.csv",
        base_directory=base_directory,
    )

    # noinspection PyProtectedMember
    my_data_connector._refresh_data_references_cache()

    assert my_data_connector.get_data_reference_count() == 4
    assert my_data_connector.get_unmatched_data_references() == []

    my_batch_definition_list: List[
        BatchDefinition
    ] = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="FAKE_DATASOURCE_NAME",
            data_connector_name="my_data_connector",
            data_asset_name="path",
        )
    )
    assert len(my_batch_definition_list) == 2

    my_batch_definition: BatchDefinition = my_batch_definition_list[0]
    batch_spec: BatchSpec = my_data_connector.build_batch_spec(
        batch_definition=my_batch_definition
    )

    assert isinstance(batch_spec, PathBatchSpec)
    assert batch_spec.path == f"{base_directory_colon}/path/A-100.csv"
