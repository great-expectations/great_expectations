import random
from unittest import mock

import pytest

from great_expectations.core.batch import (
    Batch,
    BatchRequest,
    IDDict,
    LegacyBatchDefinition,
)
from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context import AbstractDataContext
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource import Datasource
from great_expectations.datasource.data_connector import (
    ConfiguredAssetSqlDataConnector,
    InferredAssetSqlDataConnector,
)
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.execution_engine.partition_and_sample.data_partitioner import (
    DatePart,
)

try:
    sqlalchemy = pytest.importorskip("sqlalchemy")
except ImportError:
    sqlalchemy = None
from great_expectations.validator.validator import Validator

yaml = YAMLHandler()


@pytest.mark.sqlite
def get_data_context_for_datasource_and_execution_engine(
    context: AbstractDataContext,
    connection_url: str,
    sql_alchemy_execution_engine: SqlAlchemyExecutionEngine,
) -> AbstractDataContext:
    context.datasources["my_test_datasource"] = Datasource(
        name="my_test_datasource",
        # Configuration for "execution_engine" here is largely placeholder to comply with "Datasource" constructor.  # noqa: E501
        execution_engine={
            "class_name": "SqlAlchemyExecutionEngine",
            "url": connection_url,
        },
        data_connectors={
            "my_sql_data_connector": {
                "class_name": "ConfiguredAssetSqlDataConnector",
                "assets": {
                    "my_asset": {
                        "table_name": "table_partitioned_by_date_column__A",
                    },
                },
            },
        },
    )
    # Updating "execution_engine" to insure peculiarities, incorporated herein, propagate to "ExecutionEngine" itself.  # noqa: E501
    context.datasources["my_test_datasource"]._execution_engine = sql_alchemy_execution_engine  # type: ignore[union-attr]
    return context


@pytest.mark.sqlite
@pytest.mark.parametrize("partitioner_method_name_prefix", ["_", ""])
def test_get_batch_definition_list_from_batch_request(
    partitioner_method_name_prefix,
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    random.seed(0)
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    config = yaml.load(
        f"""
    name: my_sql_data_connector
    datasource_name: FAKE_Datasource_NAME

    assets:
        table_partitioned_by_date_column__A:
            partitioner_method: {partitioner_method_name_prefix}partition_on_column_value
            partitioner_kwargs:
                column_name: date

    """,
    )
    config["execution_engine"] = execution_engine

    my_data_connector = ConfiguredAssetSqlDataConnector(**config)

    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="FAKE_Datasource_NAME",
            data_connector_name="my_sql_data_connector",
            data_asset_name="table_partitioned_by_date_column__A",
            data_connector_query={"batch_filter_parameters": {"date": "2020-01-01"}},
        )
    )
    assert len(batch_definition_list) == 1

    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="FAKE_Datasource_NAME",
            data_connector_name="my_sql_data_connector",
            data_asset_name="table_partitioned_by_date_column__A",
            data_connector_query={"batch_filter_parameters": {}},
        )
    )
    assert len(batch_definition_list) == 34

    # Note: Abe 20201109: It would be nice to put in safeguards for mistakes like this.
    # In this case, "date" should go inside "batch_identifiers".
    # Currently, the method ignores "date" entirely, and matches on too many partitions.
    # I don't think this is unique to ConfiguredAssetSqlDataConnector.
    # with pytest.raises(gx_exceptions.DataConnectorError) as e:
    #     batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
    #         batch_request=BatchRequest(
    #             datasource_name="FAKE_Datasource_NAME",
    #             data_connector_name="my_sql_data_connector",
    #             data_asset_name="table_partitioned_by_date_column__A",
    #             data_connector_query={
    #                 "batch_filter_parameters": {},
    #                 "date" : "2020-01-01",
    #             }
    #         )
    #     )
    # assert "Unmatched key" in e.value.message

    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="FAKE_Datasource_NAME",
            data_connector_name="my_sql_data_connector",
            data_asset_name="table_partitioned_by_date_column__A",
        )
    )
    assert len(batch_definition_list) == 34

    with pytest.raises(TypeError):
        # noinspection PyArgumentList
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_sql_data_connector",
            )
        )

    with pytest.raises(TypeError):
        # noinspection PyArgumentList
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(datasource_name="FAKE_Datasource_NAME")
        )

    with pytest.raises(TypeError):
        # noinspection PyArgumentList
        my_data_connector.get_batch_definition_list_from_batch_request(batch_request=BatchRequest())


@pytest.mark.sqlite
@pytest.mark.parametrize("sampler_method_name_prefix", ["_", ""])
def test_get_batch_data_and_markers_sampling_method__limit(
    in_memory_runtime_context,
    sampler_method_name_prefix,
    test_cases_for_sql_data_connector_sqlite_connection_url,
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    context = get_data_context_for_datasource_and_execution_engine(
        context=in_memory_runtime_context,
        connection_url=test_cases_for_sql_data_connector_sqlite_connection_url,
        sql_alchemy_execution_engine=execution_engine,
    )

    batch_data, _batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=SqlAlchemyDatasourceBatchSpec(
            {
                "table_name": "table_partitioned_by_date_column__A",
                "batch_identifiers": {},
                "partitioner_method": "_partition_on_whole_table",
                "partitioner_kwargs": {},
                "sampling_method": f"{sampler_method_name_prefix}sample_using_limit",
                "sampling_kwargs": {"n": 20},
            }
        )
    )

    batch_definition = LegacyBatchDefinition(
        datasource_name="my_test_datasource",
        data_connector_name="my_sql_data_connector",
        data_asset_name="my_asset",
        batch_identifiers=IDDict({}),
    )

    batch = Batch(data=batch_data, batch_definition=batch_definition)

    validator = Validator(
        execution_engine=execution_engine,
        data_context=context,
        batches=[batch],
    )
    assert len(validator.head(fetch_all=True)) == 20

    assert not validator.expect_column_values_to_be_in_set("date", value_set=["2020-01-02"]).success


@pytest.mark.sqlite
@pytest.mark.parametrize("sampler_method_name_prefix", ["_", ""])
def test_get_batch_data_and_markers_sampling_method__random(
    sampler_method_name_prefix,
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    # noinspection PyUnusedLocal
    _batch_data, _batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=SqlAlchemyDatasourceBatchSpec(
            {
                "table_name": "table_partitioned_by_date_column__A",
                "batch_identifiers": {},
                "partitioner_method": "_partition_on_whole_table",
                "partitioner_kwargs": {},
                "sampling_method": f"{sampler_method_name_prefix}sample_using_random",
                "sampling_kwargs": {"p": 1.0},
            }
        )
    )

    # random.seed() is no good here: the random number generator is in the database, not python
    # assert len(batch_data.head(fetch_all=True)) == 63
    pass


@pytest.mark.sqlite
@pytest.mark.parametrize("sampler_method_name_prefix", ["_", ""])
def test_get_batch_data_and_markers_sampling_method__mod(
    sampler_method_name_prefix,
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    batch_data, _batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=SqlAlchemyDatasourceBatchSpec(
            {
                "table_name": "table_partitioned_by_date_column__A",
                "batch_identifiers": {},
                "partitioner_method": "_partition_on_whole_table",
                "partitioner_kwargs": {},
                "sampling_method": f"{sampler_method_name_prefix}sample_using_mod",
                "sampling_kwargs": {
                    "column_name": "id",
                    "mod": 10,
                    "value": 8,
                },
            }
        )
    )
    execution_engine.load_batch_data("__", batch_data)
    validator = Validator(execution_engine)
    assert len(validator.head(fetch_all=True)) == 12


@pytest.mark.sqlite
@pytest.mark.parametrize("sampler_method_name_prefix", ["_", ""])
def test_get_batch_data_and_markers_sampling_method__a_list(
    sampler_method_name_prefix,
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    batch_data, _batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=SqlAlchemyDatasourceBatchSpec(
            {
                "table_name": "table_partitioned_by_date_column__A",
                "batch_identifiers": {},
                "partitioner_method": "_partition_on_whole_table",
                "partitioner_kwargs": {},
                "sampling_method": f"{sampler_method_name_prefix}sample_using_a_list",
                "sampling_kwargs": {
                    "column_name": "id",
                    "value_list": [10, 20, 30, 40],
                },
            }
        )
    )
    execution_engine.load_batch_data("__", batch_data)
    validator = Validator(execution_engine)
    assert len(validator.head(fetch_all=True)) == 4


@pytest.mark.sqlite
def test_get_batch_data_and_markers_to_make_sure_partitioner_and_sampler_methods_are_optional(
    in_memory_runtime_context,
    test_cases_for_sql_data_connector_sqlite_connection_url,
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    context = get_data_context_for_datasource_and_execution_engine(
        context=in_memory_runtime_context,
        connection_url=test_cases_for_sql_data_connector_sqlite_connection_url,
        sql_alchemy_execution_engine=execution_engine,
    )

    batch_data, _batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=SqlAlchemyDatasourceBatchSpec(
            {
                "table_name": "table_partitioned_by_date_column__A",
                "batch_identifiers": {},
                "sampling_method": "_sample_using_mod",
                "sampling_kwargs": {
                    "column_name": "id",
                    "mod": 10,
                    "value": 8,
                },
            }
        )
    )
    execution_engine.load_batch_data("_0", batch_data)

    validator = Validator(
        execution_engine=execution_engine,
        data_context=in_memory_runtime_context,
    )
    assert len(validator.head(fetch_all=True)) == 12

    batch_data, _batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=SqlAlchemyDatasourceBatchSpec(
            {
                "table_name": "table_partitioned_by_date_column__A",
                "batch_identifiers": {},
            }
        )
    )
    execution_engine.load_batch_data("_1", batch_data)

    validator = Validator(
        execution_engine=execution_engine,
        data_context=context,
    )
    assert len(validator.head(fetch_all=True)) == 123

    batch_data, _batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=SqlAlchemyDatasourceBatchSpec(
            {
                "table_name": "table_partitioned_by_date_column__A",
                "batch_identifiers": {},
                "partitioner_method": "_partition_on_whole_table",
                "partitioner_kwargs": {},
            }
        )
    )

    execution_engine.load_batch_data("_2", batch_data)
    validator = Validator(
        execution_engine=execution_engine,
        data_context=context,
    )
    assert len(validator.head(fetch_all=True)) == 123


@pytest.mark.sqlite
@pytest.mark.parametrize("sampler_method_name_prefix", ["_", ""])
def test_ConfiguredAssetSqlDataConnector_assets_sampling_method__limit(
    in_memory_runtime_context,
    sampler_method_name_prefix,
    test_cases_for_sql_data_connector_sqlite_connection_url,
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    random.seed(0)
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    context = get_data_context_for_datasource_and_execution_engine(
        context=in_memory_runtime_context,
        connection_url=test_cases_for_sql_data_connector_sqlite_connection_url,
        sql_alchemy_execution_engine=execution_engine,
    )

    my_data_connector: ConfiguredAssetSqlDataConnector = ConfiguredAssetSqlDataConnector(
        name="my_sql_data_connector",
        datasource_name="my_test_datasource",
        execution_engine=execution_engine,
        assets={
            "my_asset": {
                "partitioner_method": "_partition_on_whole_table",
                "partitioner_kwargs": {},
                "sampling_method": f"{sampler_method_name_prefix}sample_using_limit",
                "sampling_kwargs": {"n": 20},
                "include_schema_name": True,
                "schema_name": "main",
                "table_name": "table_partitioned_by_date_column__A",
                "data_asset_name_prefix": "taxi__",
                "data_asset_name_suffix": "__asset",
            },
        },
    )
    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="my_sql_data_connector",
            data_asset_name="taxi__main.my_asset__asset",
        )
    )
    assert len(batch_definition_list) == 1

    batch_spec: SqlAlchemyDatasourceBatchSpec = my_data_connector.build_batch_spec(
        batch_definition=batch_definition_list[0]
    )
    batch_data, _batch_markers = execution_engine.get_batch_data_and_markers(batch_spec=batch_spec)
    batch = Batch(data=batch_data, batch_definition=batch_definition_list[0])
    validator = Validator(
        execution_engine=execution_engine,
        data_context=context,
        batches=[batch],
    )
    assert len(validator.head(fetch_all=True)) == 20
    assert not validator.expect_column_values_to_be_in_set("date", value_set=["2020-01-02"]).success


@pytest.mark.sqlite
@pytest.mark.parametrize("sampler_method_name_prefix", ["_", ""])
def test_ConfiguredAssetSqlDataConnector_assets_sampling_method__random(
    sampler_method_name_prefix,
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    random.seed(0)
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    my_data_connector: ConfiguredAssetSqlDataConnector = ConfiguredAssetSqlDataConnector(
        name="my_sql_data_connector",
        datasource_name="my_test_datasource",
        execution_engine=execution_engine,
        assets={
            "my_asset": {
                "partitioner_method": "_partition_on_whole_table",
                "partitioner_kwargs": {},
                "sampling_method": f"{sampler_method_name_prefix}sample_using_random",
                "sampling_kwargs": {"p": 1.0},
                "include_schema_name": True,
                "schema_name": "main",
                "table_name": "table_partitioned_by_date_column__A",
                "data_asset_name_prefix": "taxi__",
                "data_asset_name_suffix": "__asset",
            },
        },
    )
    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="my_sql_data_connector",
            data_asset_name="taxi__main.my_asset__asset",
        )
    )
    assert len(batch_definition_list) == 1

    batch_spec: SqlAlchemyDatasourceBatchSpec = my_data_connector.build_batch_spec(
        batch_definition=batch_definition_list[0]
    )
    batch_data, _batch_markers = execution_engine.get_batch_data_and_markers(batch_spec=batch_spec)
    batch = Batch(data=batch_data)
    validator = Validator(execution_engine, batches=[batch])
    assert len(validator.head(fetch_all=True)) == 123


@pytest.mark.sqlite
@pytest.mark.parametrize("sampler_method_name_prefix", ["_", ""])
def test_ConfiguredAssetSqlDataConnector_assets_sampling_method__mod(
    sampler_method_name_prefix,
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    random.seed(0)
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    my_data_connector: ConfiguredAssetSqlDataConnector = ConfiguredAssetSqlDataConnector(
        name="my_sql_data_connector",
        datasource_name="my_test_datasource",
        execution_engine=execution_engine,
        assets={
            "my_asset": {
                "partitioner_method": "_partition_on_whole_table",
                "partitioner_kwargs": {},
                "sampling_method": f"{sampler_method_name_prefix}sample_using_mod",
                "sampling_kwargs": {
                    "column_name": "id",
                    "mod": 10,
                    "value": 8,
                },
                "include_schema_name": True,
                "schema_name": "main",
                "table_name": "table_partitioned_by_date_column__A",
                "data_asset_name_prefix": "taxi__",
                "data_asset_name_suffix": "__asset",
            },
        },
    )
    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="my_sql_data_connector",
            data_asset_name="taxi__main.my_asset__asset",
        )
    )
    assert len(batch_definition_list) == 1

    batch_spec: SqlAlchemyDatasourceBatchSpec = my_data_connector.build_batch_spec(
        batch_definition=batch_definition_list[0]
    )
    batch_data, _batch_markers = execution_engine.get_batch_data_and_markers(batch_spec=batch_spec)
    batch = Batch(data=batch_data)
    validator = Validator(execution_engine, batches=[batch])
    assert len(validator.head(fetch_all=True)) == 12


@pytest.mark.sqlite
@pytest.mark.parametrize("sampler_method_name_prefix", ["_", ""])
def test_ConfiguredAssetSqlDataConnector_assets_sampling_method__a_list(
    sampler_method_name_prefix,
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    random.seed(0)
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    my_data_connector: ConfiguredAssetSqlDataConnector = ConfiguredAssetSqlDataConnector(
        name="my_sql_data_connector",
        datasource_name="my_test_datasource",
        execution_engine=execution_engine,
        assets={
            "my_asset": {
                "partitioner_method": "_partition_on_whole_table",
                "partitioner_kwargs": {},
                "sampling_method": f"{sampler_method_name_prefix}sample_using_a_list",
                "sampling_kwargs": {
                    "column_name": "id",
                    "value_list": [10, 20, 30, 40],
                },
                "include_schema_name": True,
                "schema_name": "main",
                "table_name": "table_partitioned_by_date_column__A",
                "data_asset_name_prefix": "taxi__",
                "data_asset_name_suffix": "__asset",
            },
        },
    )
    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="my_sql_data_connector",
            data_asset_name="taxi__main.my_asset__asset",
        )
    )
    assert len(batch_definition_list) == 1

    batch_spec: SqlAlchemyDatasourceBatchSpec = my_data_connector.build_batch_spec(
        batch_definition=batch_definition_list[0]
    )
    batch_data, _batch_markers = execution_engine.get_batch_data_and_markers(batch_spec=batch_spec)
    batch = Batch(data=batch_data)
    validator = Validator(execution_engine, batches=[batch])
    assert len(validator.head(fetch_all=True)) == 4


@pytest.mark.sqlite
@pytest.mark.parametrize("sampler_method_name_prefix", ["_", ""])
def test_ConfiguredAssetSqlDataConnector_assets_sampling_method_default__a_list(
    sampler_method_name_prefix,
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    random.seed(0)
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    my_data_connector: ConfiguredAssetSqlDataConnector = ConfiguredAssetSqlDataConnector(
        name="my_sql_data_connector",
        datasource_name="my_test_datasource",
        execution_engine=execution_engine,
        sampling_method=f"{sampler_method_name_prefix}sample_using_a_list",
        sampling_kwargs={
            "column_name": "id",
            "value_list": [10, 20, 30, 40],
        },
        assets={
            "my_asset": {
                "include_schema_name": True,
                "schema_name": "main",
                "table_name": "table_partitioned_by_date_column__A",
                "data_asset_name_prefix": "taxi__",
                "data_asset_name_suffix": "__asset",
            },
        },
    )
    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="my_sql_data_connector",
            data_asset_name="taxi__main.my_asset__asset",
        )
    )
    assert len(batch_definition_list) == 1

    batch_spec: SqlAlchemyDatasourceBatchSpec = my_data_connector.build_batch_spec(
        batch_definition=batch_definition_list[0]
    )
    batch_data, _batch_markers = execution_engine.get_batch_data_and_markers(batch_spec=batch_spec)
    batch = Batch(data=batch_data)
    validator = Validator(execution_engine, batches=[batch])
    assert len(validator.head(fetch_all=True)) == 4


@pytest.mark.sqlite
@pytest.mark.parametrize("sampler_method_name_prefix", ["_", ""])
def test_ConfiguredAssetSqlDataConnector_assets_sampling_method_default__random_asset_override__a_list(  # noqa: E501
    sampler_method_name_prefix,
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    random.seed(0)
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    my_data_connector: ConfiguredAssetSqlDataConnector = ConfiguredAssetSqlDataConnector(
        name="my_sql_data_connector",
        datasource_name="my_test_datasource",
        execution_engine=execution_engine,
        sampling_method=f"{sampler_method_name_prefix}sample_using_random",
        sampling_kwargs={"p": 1.0},
        assets={
            "my_asset": {
                "sampling_method": f"{sampler_method_name_prefix}sample_using_a_list",
                "sampling_kwargs": {
                    "column_name": "id",
                    "value_list": [10, 20, 30, 40],
                },
                "include_schema_name": True,
                "schema_name": "main",
                "table_name": "table_partitioned_by_date_column__A",
                "data_asset_name_prefix": "taxi__",
                "data_asset_name_suffix": "__asset",
            },
        },
    )
    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="my_sql_data_connector",
            data_asset_name="taxi__main.my_asset__asset",
        )
    )
    assert len(batch_definition_list) == 1

    batch_spec: SqlAlchemyDatasourceBatchSpec = my_data_connector.build_batch_spec(
        batch_definition=batch_definition_list[0]
    )
    batch_data, _batch_markers = execution_engine.get_batch_data_and_markers(batch_spec=batch_spec)
    batch = Batch(data=batch_data)
    validator = Validator(execution_engine, batches=[batch])
    assert len(validator.head(fetch_all=True)) == 4


@pytest.mark.sqlite
def test_default_behavior_with_no_partitioner(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    config = yaml.load(
        """
    name: my_sql_data_connector
    datasource_name: FAKE_Datasource_NAME

    assets:
        table_partitioned_by_date_column__A: {}
    """,
    )
    config["execution_engine"] = execution_engine

    my_data_connector = ConfiguredAssetSqlDataConnector(**config)

    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="FAKE_Datasource_NAME",
            data_connector_name="my_sql_data_connector",
            data_asset_name="table_partitioned_by_date_column__A",
        )
    )
    assert len(batch_definition_list) == 1
    assert batch_definition_list[0]["batch_identifiers"] == {}

    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="FAKE_Datasource_NAME",
            data_connector_name="my_sql_data_connector",
            data_asset_name="table_partitioned_by_date_column__A",
            data_connector_query={},
        )
    )
    assert len(batch_definition_list) == 1
    assert batch_definition_list[0]["batch_identifiers"] == {}

    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="FAKE_Datasource_NAME",
            data_connector_name="my_sql_data_connector",
            data_asset_name="table_partitioned_by_date_column__A",
            data_connector_query={"batch_filter_parameters": {}},
        )
    )
    assert len(batch_definition_list) == 1
    assert batch_definition_list[0]["batch_identifiers"] == {}


@pytest.mark.sqlite
def test_behavior_with_whole_table_partitioner(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    config = yaml.load(
        """
    name: my_sql_data_connector
    datasource_name: FAKE_Datasource_NAME

    assets:
        table_partitioned_by_date_column__A:
            partitioner_method : "_partition_on_whole_table"
            partitioner_kwargs : {}
    """,
    )
    config["execution_engine"] = execution_engine

    my_data_connector = ConfiguredAssetSqlDataConnector(**config)

    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="FAKE_Datasource_NAME",
            data_connector_name="my_sql_data_connector",
            data_asset_name="table_partitioned_by_date_column__A",
        )
    )
    assert len(batch_definition_list) == 1
    assert batch_definition_list[0]["batch_identifiers"] == {}

    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="FAKE_Datasource_NAME",
            data_connector_name="my_sql_data_connector",
            data_asset_name="table_partitioned_by_date_column__A",
            data_connector_query={},
        )
    )
    assert len(batch_definition_list) == 1
    assert batch_definition_list[0]["batch_identifiers"] == {}

    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="FAKE_Datasource_NAME",
            data_connector_name="my_sql_data_connector",
            data_asset_name="table_partitioned_by_date_column__A",
            data_connector_query={"batch_filter_parameters": {}},
        )
    )
    assert len(batch_definition_list) == 1
    assert batch_definition_list[0]["batch_identifiers"] == {}


@pytest.mark.sqlite
def test_basic_instantiation_of_InferredAssetSqlDataConnector(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    my_data_connector = instantiate_class_from_config(
        config={
            "class_name": "InferredAssetSqlDataConnector",
            "name": "whole_table",
            "data_asset_name_prefix": "prexif__",
            "data_asset_name_suffix": "__xiffus",
        },
        runtime_environment={
            "execution_engine": test_cases_for_sql_data_connector_sqlite_execution_engine,
            "datasource_name": "my_test_datasource",
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )

    assert my_data_connector.get_available_data_asset_names() == [
        "prexif__table_containing_id_spacers_for_D__xiffus",
        "prexif__table_full__I__xiffus",
        "prexif__table_partitioned_by_date_column__A__xiffus",
        "prexif__table_partitioned_by_foreign_key__F__xiffus",
        "prexif__table_partitioned_by_incrementing_batch_id__E__xiffus",
        "prexif__table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D__xiffus",
        "prexif__table_partitioned_by_multiple_columns__G__xiffus",
        "prexif__table_partitioned_by_regularly_spaced_incrementing_id_column__C__xiffus",
        "prexif__table_partitioned_by_timestamp_column__B__xiffus",
        "prexif__table_that_should_be_partitioned_by_random_hash__H__xiffus",
        "prexif__table_with_fk_reference_from_F__xiffus",
        "prexif__view_by_date_column__A__xiffus",
        "prexif__view_by_incrementing_batch_id__E__xiffus",
        "prexif__view_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D__xiffus",
        "prexif__view_by_multiple_columns__G__xiffus",
        "prexif__view_by_regularly_spaced_incrementing_id_column__C__xiffus",
        "prexif__view_by_timestamp_column__B__xiffus",
        "prexif__view_containing_id_spacers_for_D__xiffus",
        "prexif__view_partitioned_by_foreign_key__F__xiffus",
        "prexif__view_that_should_be_partitioned_by_random_hash__H__xiffus",
        "prexif__view_with_fk_reference_from_F__xiffus",
    ]

    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="whole_table",
            data_asset_name="prexif__table_that_should_be_partitioned_by_random_hash__H__xiffus",
        )
    )
    assert len(batch_definition_list) == 1


@pytest.mark.sqlite
def test_more_complex_instantiation_of_InferredAssetSqlDataConnector(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    my_data_connector = instantiate_class_from_config(
        config={
            "class_name": "InferredAssetSqlDataConnector",
            "name": "whole_table",
            "data_asset_name_suffix": "__whole",
            "include_schema_name": True,
        },
        runtime_environment={
            "execution_engine": test_cases_for_sql_data_connector_sqlite_execution_engine,
            "datasource_name": "my_test_datasource",
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )

    assert my_data_connector.get_available_data_asset_names() == [
        "main.table_containing_id_spacers_for_D__whole",
        "main.table_full__I__whole",
        "main.table_partitioned_by_date_column__A__whole",
        "main.table_partitioned_by_foreign_key__F__whole",
        "main.table_partitioned_by_incrementing_batch_id__E__whole",
        "main.table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D__whole",
        "main.table_partitioned_by_multiple_columns__G__whole",
        "main.table_partitioned_by_regularly_spaced_incrementing_id_column__C__whole",
        "main.table_partitioned_by_timestamp_column__B__whole",
        "main.table_that_should_be_partitioned_by_random_hash__H__whole",
        "main.table_with_fk_reference_from_F__whole",
        "main.view_by_date_column__A__whole",
        "main.view_by_incrementing_batch_id__E__whole",
        "main.view_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D__whole",
        "main.view_by_multiple_columns__G__whole",
        "main.view_by_regularly_spaced_incrementing_id_column__C__whole",
        "main.view_by_timestamp_column__B__whole",
        "main.view_containing_id_spacers_for_D__whole",
        "main.view_partitioned_by_foreign_key__F__whole",
        "main.view_that_should_be_partitioned_by_random_hash__H__whole",
        "main.view_with_fk_reference_from_F__whole",
    ]

    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="whole_table",
            data_asset_name="main.table_that_should_be_partitioned_by_random_hash__H__whole",
        )
    )
    assert len(batch_definition_list) == 1


@pytest.mark.sqlite
@mock.patch("great_expectations.execution_engine.SqlAlchemyExecutionEngine.__init__")
@pytest.mark.parametrize("partitioner_method_name_prefix", ["_", ""])
def test_more_complex_instantiation_of_ConfiguredAssetSqlDataConnector_include_schema_name(
    mock_sql_alchemy_execution_engine: mock.MagicMock,  # noqa: TID251
    partitioner_method_name_prefix,
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    my_data_connector: ConfiguredAssetSqlDataConnector = ConfiguredAssetSqlDataConnector(
        name="my_sql_data_connector",
        datasource_name="my_test_datasource",
        execution_engine=mock_sql_alchemy_execution_engine,
        assets={
            "table_partitioned_by_date_column__A": {
                "partitioner_method": f"{partitioner_method_name_prefix}partition_on_column_value",
                "partitioner_kwargs": {"column_name": "date"},
                "include_schema_name": True,
                "schema_name": "main",
            },
        },
    )
    assert "main.table_partitioned_by_date_column__A" in my_data_connector.assets

    my_data_connector: ConfiguredAssetSqlDataConnector = ConfiguredAssetSqlDataConnector(
        name="my_sql_data_connector",
        datasource_name="my_test_datasource",
        execution_engine=mock_sql_alchemy_execution_engine,
        assets={
            "table_partitioned_by_date_column__A": {
                "partitioner_method": f"{partitioner_method_name_prefix}partition_on_column_value",
                "partitioner_kwargs": {"column_name": "date"},
                "include_schema_name": False,
                "schema_name": "main",
            },
        },
    )
    assert "table_partitioned_by_date_column__A" in my_data_connector.assets


@pytest.mark.sqlite
@mock.patch("great_expectations.execution_engine.SqlAlchemyExecutionEngine.__init__")
@pytest.mark.parametrize("partitioner_method_name_prefix", ["_", ""])
def test_more_complex_instantiation_of_ConfiguredAssetSqlDataConnector_include_schema_name_prefix_suffix(  # noqa: E501
    mock_sql_alchemy_execution_engine: mock.MagicMock,  # noqa: TID251
    partitioner_method_name_prefix,
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    my_data_connector: ConfiguredAssetSqlDataConnector = ConfiguredAssetSqlDataConnector(
        name="my_sql_data_connector",
        datasource_name="my_test_datasource",
        execution_engine=mock_sql_alchemy_execution_engine,
        assets={
            "table_partitioned_by_date_column__A": {
                "partitioner_method": f"{partitioner_method_name_prefix}partition_on_column_value",
                "partitioner_kwargs": {"column_name": "date"},
                "include_schema_name": True,
                "schema_name": "main",
                "data_asset_name_prefix": "taxi__",
                "data_asset_name_suffix": "__asset",
            },
        },
    )
    assert "taxi__main.table_partitioned_by_date_column__A__asset" in my_data_connector.assets


@pytest.mark.sqlite
@mock.patch("great_expectations.execution_engine.SqlAlchemyExecutionEngine.__init__")
@pytest.mark.parametrize("partitioner_method_name_prefix", ["_", ""])
def test_more_complex_instantiation_of_ConfiguredAssetSqlDataConnector_include_schema_name_prefix_suffix_table_name(  # noqa: E501
    mock_sql_alchemy_execution_engine: mock.MagicMock,  # noqa: TID251
    partitioner_method_name_prefix,
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    my_data_connector: ConfiguredAssetSqlDataConnector = ConfiguredAssetSqlDataConnector(
        name="my_sql_data_connector",
        datasource_name="my_test_datasource",
        execution_engine=mock_sql_alchemy_execution_engine,
        assets={
            "my_asset": {
                "partitioner_method": f"{partitioner_method_name_prefix}partition_on_column_value",
                "partitioner_kwargs": {"column_name": "date"},
                "include_schema_name": True,
                "schema_name": "main",
                "table_name": "table_partitioned_by_date_column__A",
                "data_asset_name_prefix": "taxi__",
                "data_asset_name_suffix": "__asset",
            },
        },
    )
    assert "taxi__main.my_asset__asset" in my_data_connector.assets


@pytest.mark.sqlite
@pytest.mark.parametrize("partitioner_method_name_prefix", ["_", ""])
def test_more_complex_instantiation_of_ConfiguredAssetSqlDataConnector_include_schema_name_prefix_suffix_table_name_asset_partitioner(  # noqa: E501
    partitioner_method_name_prefix,
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    random.seed(0)
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    my_data_connector: ConfiguredAssetSqlDataConnector = ConfiguredAssetSqlDataConnector(
        name="my_sql_data_connector",
        datasource_name="my_test_datasource",
        execution_engine=execution_engine,
        assets={
            "my_asset": {
                "partitioner_method": f"{partitioner_method_name_prefix}partition_on_column_value",
                "partitioner_kwargs": {"column_name": "date"},
                "include_schema_name": True,
                "schema_name": "main",
                "table_name": "table_partitioned_by_date_column__A",
                "data_asset_name_prefix": "taxi__",
                "data_asset_name_suffix": "__asset",
            },
        },
    )
    assert "taxi__main.my_asset__asset" in my_data_connector.assets

    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="my_sql_data_connector",
            data_asset_name="taxi__main.my_asset__asset",
        )
    )
    assert len(batch_definition_list) == 34


@pytest.mark.sqlite
@pytest.mark.parametrize("partitioner_method_name_prefix", ["_", ""])
def test_more_complex_instantiation_of_ConfiguredAssetSqlDataConnector_include_schema_name_prefix_suffix_table_name_default_partitioner(  # noqa: E501
    partitioner_method_name_prefix,
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    random.seed(0)
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    my_data_connector: ConfiguredAssetSqlDataConnector = ConfiguredAssetSqlDataConnector(
        name="my_sql_data_connector",
        datasource_name="my_test_datasource",
        execution_engine=execution_engine,
        partitioner_method=f"{partitioner_method_name_prefix}partition_on_column_value",
        partitioner_kwargs={"column_name": "date"},
        assets={
            "my_asset": {
                "include_schema_name": True,
                "schema_name": "main",
                "table_name": "table_partitioned_by_date_column__A",
                "data_asset_name_prefix": "taxi__",
                "data_asset_name_suffix": "__asset",
            },
        },
    )
    assert "taxi__main.my_asset__asset" in my_data_connector.assets

    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="my_sql_data_connector",
            data_asset_name="taxi__main.my_asset__asset",
        )
    )
    assert len(batch_definition_list) == 34


@pytest.mark.sqlite
@pytest.mark.parametrize("partitioner_method_name_prefix", ["_", ""])
def test_more_complex_instantiation_of_ConfiguredAssetSqlDataConnector_include_schema_name_prefix_suffix_table_name_default_partitioner_asset_override(  # noqa: E501
    partitioner_method_name_prefix,
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    random.seed(0)
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    my_data_connector: ConfiguredAssetSqlDataConnector = ConfiguredAssetSqlDataConnector(
        name="my_sql_data_connector",
        datasource_name="my_test_datasource",
        execution_engine=execution_engine,
        partitioner_method=f"{partitioner_method_name_prefix}partition_on_column_value",
        partitioner_kwargs={"column_name": "date"},
        assets={
            "my_asset": {
                "partitioner_method": f"{partitioner_method_name_prefix}partition_on_whole_table",
                "partitioner_kwargs": {},
                "include_schema_name": True,
                "schema_name": "main",
                "table_name": "table_partitioned_by_date_column__A",
                "data_asset_name_prefix": "taxi__",
                "data_asset_name_suffix": "__asset",
            },
        },
    )
    assert "taxi__main.my_asset__asset" in my_data_connector.assets

    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="my_sql_data_connector",
            data_asset_name="taxi__main.my_asset__asset",
        )
    )
    assert len(batch_definition_list) == 1


@pytest.mark.sqlite
@pytest.mark.parametrize("partitioner_method_name_prefix", ["_", ""])
@pytest.mark.parametrize("sampler_method_name_prefix", ["_", ""])
def test_simple_instantiation_and_execution_of_ConfiguredAssetSqlDataConnector_with_no_partitioner_no_sampler(  # noqa: E501
    partitioner_method_name_prefix,
    sampler_method_name_prefix,
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    random.seed(0)
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    my_data_connector: ConfiguredAssetSqlDataConnector = ConfiguredAssetSqlDataConnector(
        name="my_sql_data_connector",
        datasource_name="my_test_datasource",
        execution_engine=execution_engine,
        assets={
            "table_partitioned_by_date_column__A": {},
        },
    )
    assert "table_partitioned_by_date_column__A" in my_data_connector.assets

    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="my_sql_data_connector",
            data_asset_name="table_partitioned_by_date_column__A",
        )
    )
    assert len(batch_definition_list) == 1

    batch_spec: SqlAlchemyDatasourceBatchSpec = my_data_connector.build_batch_spec(
        batch_definition=batch_definition_list[0]
    )
    batch_data, _batch_markers = execution_engine.get_batch_data_and_markers(batch_spec=batch_spec)
    batch = Batch(data=batch_data)
    validator = Validator(execution_engine, batches=[batch])
    assert len(validator.head(fetch_all=True)) == 123


@pytest.mark.sqlite
@pytest.mark.parametrize("partitioner_method_name_prefix", ["_", ""])
@pytest.mark.parametrize("sampler_method_name_prefix", ["_", ""])
def test_full_config_instantiation_and_execution_of_ConfiguredAssetSqlDataConnector_with_default_partitioner_sampler_asset_override(  # noqa: E501
    partitioner_method_name_prefix,
    sampler_method_name_prefix,
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    random.seed(0)
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    my_data_connector: ConfiguredAssetSqlDataConnector = ConfiguredAssetSqlDataConnector(
        name="my_sql_data_connector",
        datasource_name="my_test_datasource",
        execution_engine=execution_engine,
        partitioner_method=f"{partitioner_method_name_prefix}partition_on_column_value",
        partitioner_kwargs={"column_name": "date"},
        sampling_method=f"{sampler_method_name_prefix}sample_using_random",
        sampling_kwargs={"p": 1.0},
        assets={
            "my_asset": {
                "partitioner_method": f"{partitioner_method_name_prefix}partition_on_whole_table",
                "partitioner_kwargs": {},
                "sampling_method": f"{sampler_method_name_prefix}sample_using_a_list",
                "sampling_kwargs": {
                    "column_name": "id",
                    "value_list": [10, 20, 30, 40],
                },
                "include_schema_name": True,
                "schema_name": "main",
                "table_name": "table_partitioned_by_date_column__A",
                "data_asset_name_prefix": "taxi__",
                "data_asset_name_suffix": "__asset",
            },
        },
    )
    assert "taxi__main.my_asset__asset" in my_data_connector.assets

    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="my_sql_data_connector",
            data_asset_name="taxi__main.my_asset__asset",
        )
    )
    assert len(batch_definition_list) == 1

    batch_spec: SqlAlchemyDatasourceBatchSpec = my_data_connector.build_batch_spec(
        batch_definition=batch_definition_list[0]
    )
    batch_data, _batch_markers = execution_engine.get_batch_data_and_markers(batch_spec=batch_spec)
    batch = Batch(data=batch_data)
    validator = Validator(execution_engine, batches=[batch])
    assert len(validator.head(fetch_all=True)) == 4


@pytest.mark.sqlite
@pytest.mark.parametrize("partitioner_method_name_prefix", ["_", ""])
@pytest.mark.parametrize("sampler_method_name_prefix", ["_", ""])
def test_full_config_instantiation_and_execution_of_InferredAssetSqlDataConnector_with_default_partitioner_sampler_asset_override(  # noqa: E501
    partitioner_method_name_prefix,
    sampler_method_name_prefix,
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    random.seed(0)
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    my_data_connector: InferredAssetSqlDataConnector = InferredAssetSqlDataConnector(
        name="my_sql_data_connector",
        datasource_name="my_test_datasource",
        execution_engine=execution_engine,
        data_asset_name_prefix="taxi__",
        data_asset_name_suffix="__asset",
        include_schema_name=True,
        partitioner_method=f"{partitioner_method_name_prefix}partition_on_column_value",
        partitioner_kwargs={"column_name": "date"},
        sampling_method=f"{sampler_method_name_prefix}sample_using_limit",
        sampling_kwargs={
            "n": 5,
        },
        excluded_tables=None,
        included_tables=None,
        skip_inapplicable_tables=True,
        introspection_directives={
            "schema_name": "main",
            "ignore_information_schemas_and_system_tables": True,
            "information_schemas": None,
            "system_tables": None,
            "include_views": True,
        },
        batch_spec_passthrough=None,
    )
    assert "taxi__main.table_partitioned_by_date_column__A__asset" in my_data_connector.assets

    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="my_sql_data_connector",
            data_asset_name="taxi__main.table_partitioned_by_date_column__A__asset",
        )
    )
    assert len(batch_definition_list) == 34

    batch_spec: SqlAlchemyDatasourceBatchSpec = my_data_connector.build_batch_spec(
        batch_definition=batch_definition_list[1]
    )
    batch_data, _batch_markers = execution_engine.get_batch_data_and_markers(batch_spec=batch_spec)
    batch = Batch(data=batch_data)
    validator = Validator(execution_engine, batches=[batch])
    assert len(validator.head(fetch_all=True)) == 5


@pytest.mark.sqlite
@pytest.mark.parametrize(
    "partitioner_method,partitioner_kwargs,table_name,first_3_batch_identifiers_expected,last_3_batch_identifiers_expected",
    [
        (
            "partition_on_year",
            {"column_name": "date"},
            "table_partitioned_by_date_column__A",
            [
                {"date": {"year": 2020}},
                {"date": {"year": 2021}},
                {"date": {"year": 2022}},
            ],
            [
                {"date": {"year": 2021}},
                {"date": {"year": 2022}},
                {"date": {"year": 2023}},
            ],
        ),
        (
            "partition_on_year_and_month",
            {"column_name": "date"},
            "table_partitioned_by_date_column__A",
            [
                {"date": {"month": 1, "year": 2020}},
                {"date": {"month": 3, "year": 2020}},
                {"date": {"month": 1, "year": 2021}},
            ],
            [
                {"date": {"month": 1, "year": 2021}},
                {"date": {"month": 1, "year": 2022}},
                {"date": {"month": 1, "year": 2023}},
            ],
        ),
        (
            "partition_on_year_and_month_and_day",
            {"column_name": "date"},
            "table_partitioned_by_date_column__A",
            [
                {"date": {"day": 1, "month": 1, "year": 2020}},
                {"date": {"day": 2, "month": 1, "year": 2020}},
                {"date": {"day": 3, "month": 1, "year": 2020}},
            ],
            [
                {"date": {"day": 1, "month": 1, "year": 2021}},
                {"date": {"day": 1, "month": 1, "year": 2022}},
                {"date": {"day": 1, "month": 1, "year": 2023}},
            ],
        ),
        (
            "partition_on_date_parts",
            {"column_name": "date", "date_parts": [DatePart.MONTH]},
            "table_partitioned_by_date_column__A",
            [{"date": {"month": 1}}, {"date": {"month": 3}}],
            [{"date": {"month": 1}}, {"date": {"month": 3}}],
        ),
        (
            "partition_on_whole_table",
            {},
            "table_partitioned_by_date_column__A",
            [{}],
            [{}],
        ),
        (
            "partition_on_column_value",
            {"column_name": "date"},
            "table_partitioned_by_date_column__A",
            [{"date": "2020-01-01"}, {"date": "2020-01-02"}, {"date": "2020-01-03"}],
            [{"date": "2021-01-01"}, {"date": "2022-01-01"}, {"date": "2023-01-01"}],
        ),
        (
            "partition_on_converted_datetime",
            {"column_name": "date"},
            "table_partitioned_by_date_column__A",
            [
                {"date": "2020-01-01"},
                {"date": "2020-01-02"},
                {"date": "2020-01-03"},
            ],
            [
                {"date": "2021-01-01"},
                {"date": "2022-01-01"},
                {"date": "2023-01-01"},
            ],
        ),
        (
            "partition_on_divided_integer",
            {"column_name": "id", "divisor": 10},
            "table_partitioned_by_regularly_spaced_incrementing_id_column__C",
            [{"id": 0}, {"id": 1}, {"id": 2}],
            [{"id": 9}, {"id": 10}, {"id": 11}],
        ),
        (
            "partition_on_mod_integer",
            {"column_name": "id", "mod": 10},
            "table_partitioned_by_regularly_spaced_incrementing_id_column__C",
            [{"id": 0}, {"id": 1}, {"id": 2}],
            [{"id": 7}, {"id": 8}, {"id": 9}],
        ),
        (
            "partition_on_multi_column_values",
            {"column_names": ["y", "m", "d"]},
            "table_partitioned_by_multiple_columns__G",
            [
                {"d": 1, "m": 1, "y": 2020},
                {"d": 2, "m": 1, "y": 2020},
                {"d": 3, "m": 1, "y": 2020},
            ],
            [
                {"d": 28, "m": 1, "y": 2020},
                {"d": 29, "m": 1, "y": 2020},
                {"d": 30, "m": 1, "y": 2020},
            ],
        ),
        pytest.param(
            "partition_on_hashed_column",
            {"column_name": "id", "hash_digits": 2},
            "table_that_should_be_partitioned_by_random_hash__H",
            [],
            [],
            marks=pytest.mark.xfail(strict=True, reason="sqlite does not support MD5 hashing"),
        ),
    ],
)
@pytest.mark.parametrize("partitioner_method_name_prefix", ["_", ""])
def test_ConfiguredAssetSqlDataConnector_sorting(
    partitioner_method,
    partitioner_kwargs,
    table_name,
    first_3_batch_identifiers_expected,
    last_3_batch_identifiers_expected,
    partitioner_method_name_prefix,
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    my_data_connector = ConfiguredAssetSqlDataConnector(
        name="my_sql_data_connector",
        datasource_name="my_test_datasource",
        execution_engine=execution_engine,
        assets={
            "my_asset": {
                "partitioner_method": f"{partitioner_method_name_prefix}{partitioner_method}",
                "partitioner_kwargs": partitioner_kwargs,
                "include_schema_name": True,
                "schema_name": "main",
                "table_name": table_name,
                "data_asset_name_prefix": "taxi__",
                "data_asset_name_suffix": "__asset",
            },
        },
    )

    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="my_sql_data_connector",
            data_asset_name="taxi__main.my_asset__asset",
        )
    )
    first_3_batch_identifiers_actual = [
        batch_definition.batch_identifiers for batch_definition in batch_definition_list[:3]
    ]
    assert first_3_batch_identifiers_actual == first_3_batch_identifiers_expected
    last_3_batch_identifiers_actual = [
        batch_definition.batch_identifiers for batch_definition in batch_definition_list[-3:]
    ]
    assert last_3_batch_identifiers_actual == last_3_batch_identifiers_expected


@pytest.mark.sqlite
@pytest.mark.parametrize(
    "data_connector_yaml,expected_batch_identifiers_list",
    [
        (
            """
    name: my_sql_data_connector
    datasource_name: my_test_datasource
    assets:
        table_partitioned_by_date_column__A:
            partitioner_method: partition_on_date_parts
            partitioner_kwargs:
                column_name: date
                date_parts:
                    - month
            """,
            [{"date": {"month": 1}}, {"date": {"month": 3}}],
        ),
        (
            """
    name: my_sql_data_connector
    datasource_name: my_test_datasource
    assets:
        table_partitioned_by_date_column__A:
            partitioner_method: partition_on_date_parts
            partitioner_kwargs:
                column_name: date
                date_parts:
                    - month
    sorters:
        - class_name: DictionarySorter
          name: date
          orderby: desc
            """,
            [{"date": {"month": 3}}, {"date": {"month": 1}}],
        ),
        (
            """
    name: my_sql_data_connector
    datasource_name: my_test_datasource
    assets:
        table_partitioned_by_date_column__A:
            partitioner_method: partition_on_date_parts
            partitioner_kwargs:
                column_name: date
                date_parts:
                    - month
            sorters:
                - class_name: DictionarySorter
                  name: date
                  orderby: desc
            """,
            [{"date": {"month": 3}}, {"date": {"month": 1}}],
        ),
    ],
)
def test_ConfiguredAssetSqlDataConnector_return_all_batch_definitions_sorted(
    data_connector_yaml,
    expected_batch_identifiers_list,
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine
    data_connector_config = yaml.load(data_connector_yaml)
    data_connector_config["execution_engine"] = execution_engine

    my_data_connector = ConfiguredAssetSqlDataConnector(**data_connector_config)

    sorted_batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="my_sql_data_connector",
            data_asset_name="table_partitioned_by_date_column__A",
        )
    )

    expected = [
        LegacyBatchDefinition(
            datasource_name="my_test_datasource",
            data_connector_name="my_sql_data_connector",
            data_asset_name="table_partitioned_by_date_column__A",
            batch_identifiers=IDDict(batch_identifiers),
        )
        for batch_identifiers in expected_batch_identifiers_list
    ]

    assert expected == sorted_batch_definition_list


@pytest.mark.sqlite
def test_introspect_db(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    my_data_connector = instantiate_class_from_config(
        config={
            "class_name": "InferredAssetSqlDataConnector",
            "name": "my_test_data_connector",
        },
        runtime_environment={
            "execution_engine": test_cases_for_sql_data_connector_sqlite_execution_engine,
            "datasource_name": "my_test_datasource",
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )

    assert my_data_connector._introspect_db() == [
        {
            "schema_name": "main",
            "table_name": "table_containing_id_spacers_for_D",
            "type": "table",
        },
        {"schema_name": "main", "table_name": "table_full__I", "type": "table"},
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_date_column__A",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_foreign_key__F",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_incrementing_batch_id__E",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D",  # noqa: E501
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_multiple_columns__G",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_regularly_spaced_incrementing_id_column__C",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_timestamp_column__B",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_that_should_be_partitioned_by_random_hash__H",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_with_fk_reference_from_F",
            "type": "table",
        },
        {"schema_name": "main", "table_name": "view_by_date_column__A", "type": "view"},
        {
            "schema_name": "main",
            "table_name": "view_by_incrementing_batch_id__E",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D",  # noqa: E501
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_by_multiple_columns__G",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_by_regularly_spaced_incrementing_id_column__C",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_by_timestamp_column__B",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_containing_id_spacers_for_D",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_partitioned_by_foreign_key__F",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_that_should_be_partitioned_by_random_hash__H",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_with_fk_reference_from_F",
            "type": "view",
        },
    ]

    assert my_data_connector._introspect_db(schema_name="main") == [
        {
            "schema_name": "main",
            "table_name": "table_containing_id_spacers_for_D",
            "type": "table",
        },
        {"schema_name": "main", "table_name": "table_full__I", "type": "table"},
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_date_column__A",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_foreign_key__F",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_incrementing_batch_id__E",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D",  # noqa: E501
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_multiple_columns__G",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_regularly_spaced_incrementing_id_column__C",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_timestamp_column__B",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_that_should_be_partitioned_by_random_hash__H",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_with_fk_reference_from_F",
            "type": "table",
        },
        {"schema_name": "main", "table_name": "view_by_date_column__A", "type": "view"},
        {
            "schema_name": "main",
            "table_name": "view_by_incrementing_batch_id__E",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D",  # noqa: E501
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_by_multiple_columns__G",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_by_regularly_spaced_incrementing_id_column__C",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_by_timestamp_column__B",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_containing_id_spacers_for_D",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_partitioned_by_foreign_key__F",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_that_should_be_partitioned_by_random_hash__H",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_with_fk_reference_from_F",
            "type": "view",
        },
    ]

    assert my_data_connector._introspect_db(schema_name="waffle") == []

    # This is a weak test, since this db doesn't have any additional schemas or system tables to show.  # noqa: E501
    assert my_data_connector._introspect_db(ignore_information_schemas_and_system_tables=False) == [
        {
            "schema_name": "main",
            "table_name": "table_containing_id_spacers_for_D",
            "type": "table",
        },
        {"schema_name": "main", "table_name": "table_full__I", "type": "table"},
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_date_column__A",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_foreign_key__F",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_incrementing_batch_id__E",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D",  # noqa: E501
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_multiple_columns__G",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_regularly_spaced_incrementing_id_column__C",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_timestamp_column__B",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_that_should_be_partitioned_by_random_hash__H",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_with_fk_reference_from_F",
            "type": "table",
        },
        {"schema_name": "main", "table_name": "view_by_date_column__A", "type": "view"},
        {
            "schema_name": "main",
            "table_name": "view_by_incrementing_batch_id__E",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D",  # noqa: E501
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_by_multiple_columns__G",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_by_regularly_spaced_incrementing_id_column__C",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_by_timestamp_column__B",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_containing_id_spacers_for_D",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_partitioned_by_foreign_key__F",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_that_should_be_partitioned_by_random_hash__H",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_with_fk_reference_from_F",
            "type": "view",
        },
    ]
