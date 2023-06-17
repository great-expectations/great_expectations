from __future__ import annotations

import os
import pathlib
import re
from typing import TYPE_CHECKING, cast

import boto3
import botocore
import pytest

from great_expectations.core.util import DBFSPath
from great_expectations.datasource.fluent.data_asset.data_connector import (
    DBFSDataConnector,
)
from tests.test_utils import create_files_in_directory

if TYPE_CHECKING:
    from pyfakefs.fake_filesystem import FakeFilesystem

    from great_expectations.datasource.fluent.data_asset.data_connector import (
        DataConnector,
    )


@pytest.mark.integration
@pytest.mark.slow  # 1.05s
def test__get_full_file_path_pandas(fs: FakeFilesystem):
    """
    What does this test and why?
    File paths in DBFS need to use the `dbfs:/` protocol base instead of `/dbfs/` when
    being read using the `spark.read` method in the ExecutionEngine.  HOWEVER, when using a
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
    fs.create_dir(base_directory)

    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "test_dir_0/A/B/C/logfile_0.csv",
            "test_dir_0/A/B/C/bigfile_1.csv",
            "test_dir_0/A/filename2.csv",
            "test_dir_0/A/filename3.csv",
        ],
    )

    my_data_connector: DataConnector = DBFSDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_filesystem_data_asset",
        batching_regex=re.compile(r"(?P<name>.+)_(?P<number>\d+)\.csv"),
        base_directory=pathlib.Path(f"{base_directory}/test_dir_0/A/B/C"),
        glob_directive="*.csv",
        file_path_template_map_fn=DBFSPath.convert_to_file_semantics_version,
    )

    assert (
        cast(DBFSDataConnector, my_data_connector)._get_full_file_path(
            path="bigfile_1.csv"
        )
        == f"{base_directory}/test_dir_0/A/B/C/bigfile_1.csv"
    )

    assert my_data_connector.get_data_reference_count() == 2
    assert sorted(my_data_connector.get_data_references()[:3]) == [
        "bigfile_1.csv",
        "logfile_0.csv",
    ]
    assert my_data_connector.get_matched_data_reference_count() == 2
    assert sorted(my_data_connector.get_matched_data_references()[:3]) == [
        "bigfile_1.csv",
        "logfile_0.csv",
    ]
    assert my_data_connector.get_unmatched_data_references()[:3] == []
    assert my_data_connector.get_unmatched_data_reference_count() == 0


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
            "test_dir_0/A/B/C/logfile_0.csv",
            "test_dir_0/A/B/C/bigfile_1.csv",
            "test_dir_0/A/filename2.csv",
            "test_dir_0/A/filename3.csv",
        ],
    )

    my_data_connector: DataConnector = DBFSDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_filesystem_data_asset",
        batching_regex=re.compile(r"(?P<name>.+)_(?P<number>\d+)\.csv"),
        base_directory=pathlib.Path(f"{base_directory}/test_dir_0/A/B/C"),
        glob_directive="*.csv",
        file_path_template_map_fn=DBFSPath.convert_to_protocol_version,
    )

    assert (
        cast(DBFSDataConnector, my_data_connector)._get_full_file_path(
            path="bigfile_1.csv"
        )
        == f"{base_directory_colon}/test_dir_0/A/B/C/bigfile_1.csv"
    )

    assert my_data_connector.get_data_reference_count() == 2
    assert sorted(my_data_connector.get_data_references()[:3]) == [
        "bigfile_1.csv",
        "logfile_0.csv",
    ]
    assert my_data_connector.get_matched_data_reference_count() == 2
    assert sorted(my_data_connector.get_matched_data_references()[:3]) == [
        "bigfile_1.csv",
        "logfile_0.csv",
    ]
    assert my_data_connector.get_unmatched_data_references()[:3] == []
    assert my_data_connector.get_unmatched_data_reference_count() == 0
