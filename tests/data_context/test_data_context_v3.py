import datetime

import pytest

from great_expectations.data_context.types.base import DataContextConfig
from tests.test_utils import create_files_in_directory


def test_get_config(empty_data_context_v3):
    context = empty_data_context_v3

    # We can call get_config in several different modes
    assert type(context.get_config()) == DataContextConfig
    assert type(context.get_config(mode="typed")) == DataContextConfig
    assert type(context.get_config(mode="dict")) == dict
    assert type(context.get_config(mode="yaml")) == str
    with pytest.raises(ValueError):
        context.get_config(mode="foobar")

    print(context.get_config(mode="yaml"))
    print(context.get_config("dict").keys())

    assert set(context.get_config("dict").keys()) == {
        "config_version",
        "datasources",
        "config_variables_file_path",
        "plugins_directory",
        "validation_operators",
        "stores",
        "expectations_store_name",
        "validations_store_name",
        "evaluation_parameter_store_name",
        "data_docs_sites",
        "anonymous_usage_statistics",
    }


def test_config_variables(empty_data_context_v3):
    context = empty_data_context_v3
    assert type(context.config_variables) == dict
    assert set(context.config_variables.keys()) == {"instance_id"}


def test_conveying_splitting_and_sampling_directives_from_data_context_to_pandas_execution_engine(
    empty_data_context_v3, test_df, tmp_path_factory
):
    base_directory = str(
        tmp_path_factory.mktemp(
            "test_conveying_splitting_and_sampling_directives_from_data_context_to_pandas_execution_engine"
        )
    )

    create_files_in_directory(
        directory=base_directory,
        file_name_list=["somme_file.csv",],
        file_content_fn=lambda: test_df.to_csv(header=True, index=False),
    )

    context = empty_data_context_v3

    yaml_config = f"""
class_name: Datasource

execution_engine:
    class_name: PandasExecutionEngine

data_connectors:
    my_filesystem_data_connector:
        class_name: ConfiguredAssetFilesystemDataConnector
        base_directory: {base_directory}

        default_regex:
            pattern: (.+)\\.csv
            group_names:
                - alphanumeric

        assets:
            A:
"""
    # noinspection PyUnusedLocal
    report_object = context.test_yaml_config(
        name="my_directory_datasource",
        yaml_config=yaml_config,
        return_mode="report_object",
    )
    # print(json.dumps(report_object, indent=2))
    # print(context.datasources)

    my_batch = context.get_batch(
        datasource_name="my_directory_datasource",
        data_connector_name="my_filesystem_data_connector",
        data_asset_name="A",
        batch_spec_passthrough={
            "sampling_method": "_sample_using_hash",
            "sampling_kwargs": {
                "column_name": "date",
                "hash_function_name": "md5",
                "hash_value": "f",
            },
        },
    )
    assert my_batch.batch_definition["data_asset_name"] == "A"

    df_data = my_batch.data
    assert df_data.shape == (10, 10)
    df_data["date"] = df_data.apply(
        lambda row: datetime.datetime.strptime(row["date"], "%Y-%m-%d").date(), axis=1
    )
    assert (
        test_df[
            (test_df["date"] == datetime.date(2020, 1, 15))
            | (test_df["date"] == datetime.date(2020, 1, 29))
        ]
        .drop("timestamp", axis=1)
        .equals(df_data.drop("timestamp", axis=1))
    )

    my_batch = context.get_batch(
        datasource_name="my_directory_datasource",
        data_connector_name="my_filesystem_data_connector",
        data_asset_name="A",
        batch_spec_passthrough={
            "splitter_method": "_split_on_multi_column_values",
            "splitter_kwargs": {
                "column_names": ["y", "m", "d"],
                "partition_definition": {"y": 2020, "m": 1, "d": 5},
            },
        },
    )
    df_data = my_batch.data
    assert df_data.shape == (4, 10)
    df_data["date"] = df_data.apply(
        lambda row: datetime.datetime.strptime(row["date"], "%Y-%m-%d").date(), axis=1
    )
    df_data["belongs_in_split"] = df_data.apply(
        lambda row: row["date"] == datetime.date(2020, 1, 5), axis=1
    )
    df_data = df_data[df_data["belongs_in_split"]]
    assert df_data.drop("belongs_in_split", axis=1).shape == (4, 10)
