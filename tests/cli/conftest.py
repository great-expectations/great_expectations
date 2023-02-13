import os
import shutil

import pytest
from moto import mock_athena
from great_expectations.data_context.util import file_relative_path


@pytest.fixture
def empty_context_with_checkpoint_v1_stats_enabled(
    empty_data_context_stats_enabled, monkeypatch
):
    try:
        monkeypatch.delenv("VAR")
        monkeypatch.delenv("MY_PARAM")
        monkeypatch.delenv("OLD_PARAM")
    except:
        pass

    monkeypatch.setenv("VAR", "test")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("OLD_PARAM", "2")

    context = empty_data_context_stats_enabled
    root_dir = context.root_directory
    fixture_name = "my_v1_checkpoint.yml"
    fixture_path = file_relative_path(
        __file__, f"../data_context/fixtures/contexts/{fixture_name}"
    )
    checkpoints_file = os.path.join(root_dir, "checkpoints", fixture_name)
    shutil.copy(fixture_path, checkpoints_file)
    # # noinspection PyProtectedMember
    context._save_project_config()
    return context


@pytest.fixture
def v10_project_directory(tmp_path_factory):
    """
    GX 0.10.x project for testing upgrade helper
    """
    project_path = str(tmp_path_factory.mktemp("v10_project"))
    context_root_dir = os.path.join(project_path, "great_expectations")
    shutil.copytree(
        file_relative_path(
            __file__, "../test_fixtures/upgrade_helper/great_expectations_v10_project/"
        ),
        context_root_dir,
    )
    shutil.copy(
        file_relative_path(
            __file__, "../test_fixtures/upgrade_helper/great_expectations_v1_basic.yml"
        ),
        os.path.join(context_root_dir, "great_expectations.yml"),
    )
    return context_root_dir


@pytest.fixture(scope="function")
def misc_directory(tmp_path):
    misc_dir = tmp_path / "random"
    misc_dir.mkdir()
    assert os.path.isabs(misc_dir)
    return misc_dir


@pytest.fixture()
def empty_athena_db():
    # try:
    #     import sqlalchemy as sa
    #     from sqlalchemy import create_engine
    #     os.environ["AWS_ACCESS_KEY_ID"] = "test"
    #     os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    #     region_test = "sa-east-1"
    #     connection_string = f"awsathena+rest://@athena.{region_test}.amazonaws.com/test?s3_staging_dir=s3://YOUR_S3_BUCKET/path/to/"

    #     engine = create_engine(connection_string)
    #     return engine
    # except ImportError:
    #     raise ValueError("athena tests require sqlalchemy to be installed")
    try:
        import boto3
    except ImportError:
        raise ValueError(
            "AWS Athena Data Connector tests are requested, but boto3 is not installed"
        )

    with mock_athena():
        os.environ["AWS_ACCESS_KEY_ID"] = "test"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
        region_name: str = "sa-east-1"
        client = boto3.client("athena", region_name=region_name)
        database_name = "bi"


        client.start_query_execution(
        QueryString=f'create database {database_name}',)

        create_table_string = f"""CREATE EXTERNAL TABLE IF NOT EXISTS
{database_name}.test_table (
  Id string,
  Name string,
  
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://YOUR_S3_BUCKET/path/to/';"""

        client.start_query_execution(
        QueryString=create_table_string,
        )
        
        return client
