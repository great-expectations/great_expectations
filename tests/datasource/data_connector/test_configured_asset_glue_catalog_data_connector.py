import random

import pytest
from moto import mock_glue
from ruamel.yaml import YAML

from great_expectations import DataContext
from great_expectations.core.batch import Batch, BatchDefinition, BatchRequest
from great_expectations.core.id_dict import IDDict
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource.data_connector import (
    ConfiguredAssetGlueCatalogDataConnector,
)
from great_expectations.validator.validator import Validator

yaml = YAML(typ="safe")


@mock_glue
def test_basic_instantiation():
    random.seed(0)

    config = yaml.load(
        """
    name: my_glue_catalog_data_connector
    datasource_name: FAKE_Datasource_NAME

    assets:
        db_test.tb_titanic:
            #table_name: events # If table_name is omitted, then the table_name defaults to the asset name
            splitter_method: split_on_column_value
            splitter_kwargs:
                column_name: PClass
        asset2:
            table_name: tb_titanic
            database_name: db_test
            sampling_method: _sample_using_random
            sampling_kwargs:
                p: 0.5
                seed: 0
    """,
    )
    config["execution_engine"] = "execution_engine"

    my_data_connector = ConfiguredAssetGlueCatalogDataConnector(**config)

    report = my_data_connector.self_check()
    assert report == {
        "class_name": "ConfiguredAssetGlueCatalogDataConnector",
        "data_asset_count": 2,
        "example_data_asset_names": ["asset2", "db_test.tb_titanic"],
        "data_assets": {
            "asset2": {
                "batch_definition_count": 1,
                "example_data_references": [{}],
            },
            "db_test.tb_titanic": {
                "batch_definition_count": 1,
                "example_data_references": [{}],
            },
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
    }
    # noinspection PyProtectedMember
    assert len(my_data_connector.get_available_data_asset_names()) == 2
    assert my_data_connector.get_unmatched_data_references() == []


@mock_glue
def test_instantiation_from_a_config(empty_data_context_stats_enabled):
    context: DataContext = empty_data_context_stats_enabled
    report_object = context.test_yaml_config(
        f"""
        module_name: great_expectations.datasource.data_connector
        class_name: ConfiguredAssetGlueCatalogDataConnector
        name: my_glue_catalog_data_connector
        datasource_name: FAKE_Datasource_NAME

        assets:
            db_test.tb_titanic:
                #table_name: events # If table_name is omitted, then the table_name defaults to the asset name
                splitter_method: split_on_column_value
                splitter_kwargs:
                    column_name: PClass
            asset2:
                table_name: tb_titanic
                database_name: db_test
                sampling_method: _sample_using_random
                sampling_kwargs:
                    p: 0.5
                    seed: 0
        """,
        runtime_environment={
            "execution_engine": "execution_engine",
        },
        return_mode="report_object",
    )

    assert report_object == {
        "class_name": "ConfiguredAssetGlueCatalogDataConnector",
        "data_asset_count": 2,
        "example_data_asset_names": [
            "asset2",
            "db_test.tb_titanic",
        ],
        "data_assets": {
            "asset2": {"batch_definition_count": 1, "example_data_references": [{}]},
            "db_test.tb_titanic": {
                "batch_definition_count": 1,
                "example_data_references": [{}],
            },
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
    }


@mock_glue
def test_get_batch_definition_list_from_batch_request():
    my_data_connector_yaml = yaml.load(
        f"""
        module_name: great_expectations.datasource.data_connector
        class_name: ConfiguredAssetGlueCatalogDataConnector
        datasource_name: FAKE_Datasource_NAME
        name: my_glue_catalog_data_connector
        assets:
            db_test.tb_titanic:
                splitter_method: split_on_column_value
                splitter_kwargs:
                    column_name: PClass
        """,
    )
    my_data_connector: ConfiguredAssetGlueCatalogDataConnector = (
        instantiate_class_from_config(
            config=my_data_connector_yaml,
            runtime_environment={
                "execution_engine": "execution_engine",
            },
            config_defaults={
                "module_name": "great_expectations.datasource.data_connector"
            },
        )
    )
    my_data_connector._refresh_data_references_cache()

    batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_glue_catalog_data_connector",
                data_asset_name="db_test.tb_titanic",
                data_connector_query={"batch_filter_parameters": {"PClass": "1"}},
            )
        )
    )
    assert len(batch_definition_list) == 0

    batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_glue_catalog_data_connector",
                data_asset_name="db_test.tb_titanic",
                data_connector_query={"batch_filter_parameters": {}},
            )
        )
    )
    assert len(batch_definition_list) == 1
    assert batch_definition_list[0]["batch_identifiers"] == {}

    batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_glue_catalog_data_connector",
                data_asset_name="db_test.tb_titanic",
            )
        )
    )
    assert len(batch_definition_list) == 1
    assert batch_definition_list[0]["batch_identifiers"] == {}

    with pytest.raises(KeyError):
        # noinspection PyArgumentList
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_glue_catalog_data_connector",
                data_asset_name="db_test.fake",
            )
        )

    with pytest.raises(TypeError):
        # noinspection PyArgumentList
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_glue_catalog_data_connector",
            )
        )

    with pytest.raises(TypeError):
        # noinspection PyArgumentList
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(datasource_name="FAKE_Datasource_NAME")
        )

    with pytest.raises(TypeError):
        # noinspection PyArgumentList
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest()
        )


@mock_glue
def test_instantiation_with_minimal_options():
    my_data_connector = ConfiguredAssetGlueCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine="execution_engine",
    )
    assert len(my_data_connector.assets) == 0


@mock_glue
def test_instantiation_with_batch_spec_passthrough(
    test_cases_for_glue_catalog_data_connector_spark_execution_engine,
):
    execution_engine = test_cases_for_glue_catalog_data_connector_spark_execution_engine
    my_data_connector = ConfiguredAssetGlueCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine=execution_engine,
        assets={
            "titanic_asset": {
                "table_name": "tb_titanic",
                "database_name": "db_test",
            },
        },
        batch_spec_passthrough={
            "splitter_method": "_split_on_whole_table",
            "splitter_kwargs": {},
            "sampling_method": "_sample_using_random",
            "sampling_kwargs": {"p": 0.5, "seed": 0},
        },
    )

    batch_data, _, __ = my_data_connector.get_batch_data_and_metadata(
        batch_definition=BatchDefinition(
            datasource_name="FAKE_Datasource_NAME",
            data_connector_name="my_glue_catalog_data_connector",
            data_asset_name="titanic_asset",
            batch_identifiers=IDDict(),
        )
    )

    batch = Batch(data=batch_data)

    validator = Validator(execution_engine, batches=[batch])
    # The sampling probability "p" used in "SparkDFExecutionEngine._sample_using_random()" is 0.5 (the equivalent of a
    # fair coin with the 50% chance of coming up as "heads").  Hence, on average we should get as much as 60% of the rows.
    assert len(validator.head(fetch_all=True)) < 788


@mock_glue
def test_instantiation_with_asset_suffix_and_prefix():
    my_data_connector = ConfiguredAssetGlueCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine="execution_engine",
        assets={
            "db_test.tb_titanic": {
                "data_asset_name_prefix": "prefix__",
                "data_asset_name_suffix": "__suffix",
            },
        },
    )
    assert "prefix__db_test.tb_titanic__suffix" in my_data_connector.assets


@mock_glue
@pytest.mark.parametrize("splitter_method_name_prefix", ["_", ""])
def test_instantiation_with_splitter_sampling_and_prefix(splitter_method_name_prefix):
    my_data_connector = ConfiguredAssetGlueCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine="execution_engine",
        assets={
            "db_test.tb_titanic": {
                "splitter_method": f"{splitter_method_name_prefix}split_on_column_value",
                "splitter_kwargs": {
                    "column_name": "PClass",
                    "batch_identifiers": {"PClass": "1st"},
                },
                "sampling_method": "_sample_using_random",
                "sampling_kwargs": {"p": 0.5, "seed": 0},
                "data_asset_name_prefix": "prefix__",
                "data_asset_name_suffix": "__suffix",
            },
        },
    )

    data_asset_name = "prefix__db_test.tb_titanic__suffix"
    asset_config = my_data_connector.assets.get(data_asset_name, None)
    assert asset_config and asset_config == {
        "splitter_method": f"{splitter_method_name_prefix}split_on_column_value",
        "splitter_kwargs": {
            "column_name": "PClass",
            "batch_identifiers": {"PClass": "1st"},
        },
        "sampling_method": "_sample_using_random",
        "sampling_kwargs": {"p": 0.5, "seed": 0},
        "data_asset_name_prefix": "prefix__",
        "data_asset_name_suffix": "__suffix",
    }


@mock_glue
@pytest.mark.parametrize("splitter_method_name_prefix", ["_", ""])
def test_build_batch_spec_with_all_options(splitter_method_name_prefix):
    my_data_connector = ConfiguredAssetGlueCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine="execution_engine",
        assets={
            "asset_titanic": {
                "table_name": "tb_titanic",
                "database_name": "db_test",
                "splitter_method": f"{splitter_method_name_prefix}split_on_column_value",
                "splitter_kwargs": {
                    "column_name": "PClass",
                    "batch_identifiers": {"PClass": "1st"},
                },
                "sampling_method": "_sample_using_random",
                "sampling_kwargs": {"p": 0.5, "seed": 0},
                "data_asset_name_prefix": "prefix__",
                "data_asset_name_suffix": "__suffix",
            },
        },
    )
    batch_definition = BatchDefinition(
        datasource_name="FAKE_Datasource_NAME",
        data_connector_name="my_glue_catalog_data_connector",
        data_asset_name="prefix__asset_titanic__suffix",
        batch_identifiers=IDDict(),
        batch_spec_passthrough={"query_condition": "PClass = '1st' AND Age = 25"},
    )

    batch_spec = my_data_connector.build_batch_spec(batch_definition)
    assert batch_spec.database_name == "db_test"
    assert batch_spec.table_name == "tb_titanic"
    assert batch_spec.query_condition == "PClass = '1st' AND Age = 25"
    assert batch_spec.get("data_asset_name", None) == "prefix__asset_titanic__suffix"
    assert (
        batch_spec.get("splitter_method", None)
        == f"{splitter_method_name_prefix}split_on_column_value"
    )
    assert batch_spec.get("splitter_kwargs", {}) == {
        "column_name": "PClass",
        "batch_identifiers": {"PClass": "1st"},
    }
    assert batch_spec.get("sampling_method", None) == f"_sample_using_random"
    assert batch_spec.get("sampling_kwargs", {}) == {"p": 0.5, "seed": 0}
    assert batch_spec.get("data_asset_name_prefix", None) == "prefix__"
    assert batch_spec.get("data_asset_name_suffix", None) == "__suffix"


@mock_glue
def test_build_batch_spec_with_minimal_options():
    my_data_connector = ConfiguredAssetGlueCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine="test_cases_for_glue_catalog_data_connector_spark_execution_engine",
        assets={
            "db_test.tb_titanic": {},
        },
    )
    batch_definition = BatchDefinition(
        datasource_name="FAKE_Datasource_NAME",
        data_connector_name="my_glue_catalog_data_connector",
        data_asset_name="db_test.tb_titanic",
        batch_identifiers=IDDict(),
    )

    batch_spec = my_data_connector.build_batch_spec(batch_definition)
    assert batch_spec.database_name == None
    assert batch_spec.table_name == "db_test.tb_titanic"
    assert batch_spec.query_condition == None
    assert batch_spec.get("data_asset_name", None) == "db_test.tb_titanic"


@mock_glue
def test_build_batch_spec_with_database_name():
    my_data_connector = ConfiguredAssetGlueCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine="test_cases_for_glue_catalog_data_connector_spark_execution_engine",
        assets={
            "tb_titanic": {"database_name": "db_test"},
        },
    )
    batch_definition = BatchDefinition(
        datasource_name="FAKE_Datasource_NAME",
        data_connector_name="my_glue_catalog_data_connector",
        data_asset_name="tb_titanic",
        batch_identifiers=IDDict(),
    )

    batch_spec = my_data_connector.build_batch_spec(batch_definition)
    assert batch_spec.database_name == "db_test"
    assert batch_spec.table_name == "tb_titanic"
    assert batch_spec.query_condition == None
    assert batch_spec.get("data_asset_name", None) == "tb_titanic"


@mock_glue
def test_build_batch_spec_with_database_and_table_name():
    my_data_connector = ConfiguredAssetGlueCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine="test_cases_for_glue_catalog_data_connector_spark_execution_engine",
        assets={
            "titanic_asset": {"table_name": "tb_titanic", "database_name": "db_test"},
        },
    )
    batch_definition = BatchDefinition(
        datasource_name="FAKE_Datasource_NAME",
        data_connector_name="my_glue_catalog_data_connector",
        data_asset_name="titanic_asset",
        batch_identifiers=IDDict(),
    )

    batch_spec = my_data_connector.build_batch_spec(batch_definition)
    assert batch_spec.database_name == "db_test"
    assert batch_spec.table_name == "tb_titanic"
    assert batch_spec.query_condition == None
    assert batch_spec.get("data_asset_name", None) == "titanic_asset"


@mock_glue
def test_build_batch_spec_with_database_name_in_asset_name():
    my_data_connector = ConfiguredAssetGlueCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine="test_cases_for_glue_catalog_data_connector_spark_execution_engine",
        assets={
            "db_test.tb_titanic": {"database_name": "db_test"},
        },
    )
    batch_definition = BatchDefinition(
        datasource_name="FAKE_Datasource_NAME",
        data_connector_name="my_glue_catalog_data_connector",
        data_asset_name="db_test.tb_titanic",
        batch_identifiers=IDDict(),
    )

    batch_spec = my_data_connector.build_batch_spec(batch_definition)
    assert batch_spec.database_name == "db_test"
    assert batch_spec.table_name == "tb_titanic"


@mock_glue
def test_get_unmatched_data_references():
    my_data_connector = ConfiguredAssetGlueCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine="test_cases_for_glue_catalog_data_connector_spark_execution_engine",
        assets={
            "db_test.tb_titanic": {"database_name": "db_test"},
        },
    )
    unmatched_references = my_data_connector.get_unmatched_data_references()
    assert len(unmatched_references) == 0


@mock_glue
def test_get_available_data_asset_names():
    my_data_connector = ConfiguredAssetGlueCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine="test_cases_for_glue_catalog_data_connector_spark_execution_engine",
        assets={
            "asset1": {"table_name": "tb_titanic", "database_name": "db_test"},
            "asset2": {},
        },
    )
    data_asset_names = my_data_connector.get_available_data_asset_names()
    assert "asset1" in data_asset_names
    assert "asset2" in data_asset_names


@mock_glue
def test_get_batch_data_and_metadata_with_sampling_method__random_in_asset_config(
    test_cases_for_glue_catalog_data_connector_spark_execution_engine,
):
    execution_engine = test_cases_for_glue_catalog_data_connector_spark_execution_engine
    my_data_connector = ConfiguredAssetGlueCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine=execution_engine,
        assets={
            "titanic_asset": {
                "table_name": "tb_titanic",
                "database_name": "db_test",
                "splitter_method": "_split_on_whole_table",
                "splitter_kwargs": {},
                "sampling_method": "_sample_using_random",
                "sampling_kwargs": {"p": 0.5, "seed": 0},
            },
        },
    )

    batch_data, _, __ = my_data_connector.get_batch_data_and_metadata(
        batch_definition=BatchDefinition(
            datasource_name="FAKE_Datasource_NAME",
            data_connector_name="my_glue_catalog_data_connector",
            data_asset_name="titanic_asset",
            batch_identifiers=IDDict(),
            batch_spec_passthrough={},
        )
    )

    batch = Batch(data=batch_data)

    validator = Validator(execution_engine, batches=[batch])
    # The sampling probability "p" used in "SparkDFExecutionEngine._sample_using_random()" is 0.5 (the equivalent of a
    # fair coin with the 50% chance of coming up as "heads").  Hence, on average we should get as much as 60% of the rows.
    assert len(validator.head(fetch_all=True)) < 788


@mock_glue
def test_get_batch_data_and_metadata_with_sampling_method__random_in_batch_spec_passthrough(
    test_cases_for_glue_catalog_data_connector_spark_execution_engine,
):
    execution_engine = test_cases_for_glue_catalog_data_connector_spark_execution_engine
    my_data_connector = ConfiguredAssetGlueCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine=execution_engine,
        assets={
            "titanic_asset": {
                "table_name": "tb_titanic",
                "database_name": "db_test",
            },
        },
    )

    batch_data, _, __ = my_data_connector.get_batch_data_and_metadata(
        batch_definition=BatchDefinition(
            datasource_name="FAKE_Datasource_NAME",
            data_connector_name="my_glue_catalog_data_connector",
            data_asset_name="titanic_asset",
            batch_identifiers=IDDict(),
            batch_spec_passthrough={
                "splitter_method": "_split_on_whole_table",
                "splitter_kwargs": {},
                "sampling_method": "_sample_using_random",
                "sampling_kwargs": {"p": 0.5, "seed": 0},
            },
        )
    )

    batch = Batch(data=batch_data)

    validator = Validator(execution_engine, batches=[batch])
    # The sampling probability "p" used in "SparkDFExecutionEngine._sample_using_random()" is 0.5 (the equivalent of a
    # fair coin with the 50% chance of coming up as "heads").  Hence, on average we should get as much as 60% of the rows.
    assert len(validator.head(fetch_all=True)) < 788


@mock_glue
def test_get_batch_data_and_metadata_with_sampling_method__mod(
    test_cases_for_glue_catalog_data_connector_spark_execution_engine,
):
    execution_engine = test_cases_for_glue_catalog_data_connector_spark_execution_engine
    my_data_connector = ConfiguredAssetGlueCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine=execution_engine,
        assets={"db_test.tb_titanic": {}},
    )

    batch_data, _, __ = my_data_connector.get_batch_data_and_metadata(
        batch_definition=BatchDefinition(
            datasource_name="FAKE_Datasource_NAME",
            data_connector_name="my_glue_catalog_data_connector",
            data_asset_name="db_test.tb_titanic",
            batch_identifiers=IDDict(),
            batch_spec_passthrough={
                "splitter_method": "_split_on_whole_table",
                "splitter_kwargs": {},
                "sampling_method": "_sample_using_mod",
                "sampling_kwargs": {"column_name": "Age", "mod": 25, "value": 1},
            },
        )
    )

    batch = Batch(data=batch_data)

    validator = Validator(execution_engine, batches=[batch])

    assert validator.expect_column_values_to_not_be_in_set("Age", [25, 50]).success

    assert len(validator.head(fetch_all=True)) == 38


@mock_glue
def test_get_batch_data_and_metadata_with_sampling_method__list(
    test_cases_for_glue_catalog_data_connector_spark_execution_engine,
):
    execution_engine = test_cases_for_glue_catalog_data_connector_spark_execution_engine
    my_data_connector = ConfiguredAssetGlueCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine=execution_engine,
        assets={"db_test.tb_titanic": {}},
    )

    batch_data, _, __ = my_data_connector.get_batch_data_and_metadata(
        batch_definition=BatchDefinition(
            datasource_name="FAKE_Datasource_NAME",
            data_connector_name="my_glue_catalog_data_connector",
            data_asset_name="db_test.tb_titanic",
            batch_identifiers=IDDict(),
            batch_spec_passthrough={
                "splitter_method": "_split_on_whole_table",
                "splitter_kwargs": {},
                "sampling_method": "_sample_using_a_list",
                "sampling_kwargs": {"column_name": "PClass", "value_list": ["1st"]},
            },
        )
    )

    batch = Batch(data=batch_data)

    validator = Validator(execution_engine, batches=[batch])

    assert validator.expect_column_values_to_be_in_set("PClass", ["1st"]).success

    assert len(validator.head(fetch_all=True)) == 322


@mock_glue
def test_get_batch_data_and_metadata_with_sampling_method__hash(
    test_cases_for_glue_catalog_data_connector_spark_execution_engine,
):
    execution_engine = test_cases_for_glue_catalog_data_connector_spark_execution_engine
    my_data_connector = ConfiguredAssetGlueCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine=execution_engine,
        assets={"db_test.tb_titanic": {}},
    )

    batch_data, _, __ = my_data_connector.get_batch_data_and_metadata(
        batch_definition=BatchDefinition(
            datasource_name="FAKE_Datasource_NAME",
            data_connector_name="my_glue_catalog_data_connector",
            data_asset_name="db_test.tb_titanic",
            batch_identifiers=IDDict(),
            batch_spec_passthrough={
                "splitter_method": "_split_on_whole_table",
                "splitter_kwargs": {},
                "sampling_method": "_sample_using_hash",
                "sampling_kwargs": {
                    "column_name": "Name",
                    "hash_function_name": "md5",
                },
            },
        )
    )
    batch = Batch(data=batch_data)

    validator = Validator(execution_engine, batches=[batch])

    assert len(validator.head(fetch_all=True)) == 77


@mock_glue
@pytest.mark.parametrize("splitter_method_name_prefix", ["_", ""])
def test_get_batch_data_and_metadata_with_splitting_method__whole_table(
    splitter_method_name_prefix,
    test_cases_for_glue_catalog_data_connector_spark_execution_engine,
):
    execution_engine = test_cases_for_glue_catalog_data_connector_spark_execution_engine
    my_data_connector = ConfiguredAssetGlueCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine=execution_engine,
        assets={"db_test.tb_titanic": {}},
    )

    batch_data, _, __ = my_data_connector.get_batch_data_and_metadata(
        batch_definition=BatchDefinition(
            datasource_name="FAKE_Datasource_NAME",
            data_connector_name="my_glue_catalog_data_connector",
            data_asset_name="db_test.tb_titanic",
            batch_identifiers=IDDict(),
            batch_spec_passthrough={
                "splitter_method": f"{splitter_method_name_prefix}split_on_whole_table",
            },
        )
    )

    batch = Batch(data=batch_data)

    validator = Validator(execution_engine, batches=[batch])

    assert len(validator.head(fetch_all=True)) == 1313


@mock_glue
@pytest.mark.parametrize("splitter_method_name_prefix", ["_", ""])
def test_get_batch_data_and_metadata_with_splitting_method__column_value(
    splitter_method_name_prefix,
    test_cases_for_glue_catalog_data_connector_spark_execution_engine,
):
    execution_engine = test_cases_for_glue_catalog_data_connector_spark_execution_engine
    my_data_connector = ConfiguredAssetGlueCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine=execution_engine,
        assets={"db_test.tb_titanic": {}},
    )

    batch_data, _, __ = my_data_connector.get_batch_data_and_metadata(
        batch_definition=BatchDefinition(
            datasource_name="FAKE_Datasource_NAME",
            data_connector_name="my_glue_catalog_data_connector",
            data_asset_name="db_test.tb_titanic",
            batch_identifiers=IDDict(),
            batch_spec_passthrough={
                "splitter_method": f"{splitter_method_name_prefix}split_on_column_value",
                "splitter_kwargs": {
                    "column_name": "PClass",
                    "batch_identifiers": {"PClass": "1st"},
                },
            },
        )
    )

    batch = Batch(data=batch_data)

    validator = Validator(execution_engine, batches=[batch])

    assert validator.expect_column_values_to_be_in_set("PClass", ["1st"]).success

    assert len(validator.head(fetch_all=True)) == 322


@mock_glue
@pytest.mark.parametrize("splitter_method_name_prefix", ["_", ""])
def test_get_batch_data_and_metadata_with_splitting_method__divided_integer(
    splitter_method_name_prefix,
    test_cases_for_glue_catalog_data_connector_spark_execution_engine,
):
    execution_engine = test_cases_for_glue_catalog_data_connector_spark_execution_engine
    my_data_connector = ConfiguredAssetGlueCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine=execution_engine,
        assets={"db_test.tb_titanic": {}},
    )

    batch_data, _, __ = my_data_connector.get_batch_data_and_metadata(
        batch_definition=BatchDefinition(
            datasource_name="FAKE_Datasource_NAME",
            data_connector_name="my_glue_catalog_data_connector",
            data_asset_name="db_test.tb_titanic",
            batch_identifiers=IDDict(),
            batch_spec_passthrough={
                "splitter_method": f"{splitter_method_name_prefix}split_on_divided_integer",
                "splitter_kwargs": {
                    "column_name": "Age",
                    "divisor": 20,
                    "batch_identifiers": {"Age": 1},
                },
            },
        )
    )

    batch = Batch(data=batch_data)

    validator = Validator(execution_engine, batches=[batch])

    assert validator.expect_column_values_to_be_in_set(
        "Age", list(range(20, 40))
    ).success

    assert len(validator.head(fetch_all=True)) == 420


@mock_glue
@pytest.mark.parametrize("splitter_method_name_prefix", ["_", ""])
def test_get_batch_data_and_metadata_with_splitting_method__mod_integer(
    splitter_method_name_prefix,
    test_cases_for_glue_catalog_data_connector_spark_execution_engine,
):
    execution_engine = test_cases_for_glue_catalog_data_connector_spark_execution_engine
    my_data_connector = ConfiguredAssetGlueCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine=execution_engine,
        assets={"db_test.tb_titanic": {}},
    )

    batch_data, _, __ = my_data_connector.get_batch_data_and_metadata(
        batch_definition=BatchDefinition(
            datasource_name="FAKE_Datasource_NAME",
            data_connector_name="my_glue_catalog_data_connector",
            data_asset_name="db_test.tb_titanic",
            batch_identifiers=IDDict(),
            batch_spec_passthrough={
                "splitter_method": f"{splitter_method_name_prefix}split_on_mod_integer",
                "splitter_kwargs": {
                    "column_name": "Age",
                    "mod": 25,
                    "batch_identifiers": {"Age": 1},
                },
            },
        )
    )

    batch = Batch(data=batch_data)

    validator = Validator(execution_engine, batches=[batch])

    assert validator.expect_column_values_to_be_in_set("Age", [26, 1, 1.5, 51]).success

    assert len(validator.head(fetch_all=True)) == 38


@mock_glue
@pytest.mark.parametrize("splitter_method_name_prefix", ["_", ""])
def test_get_batch_data_and_metadata_with_splitting_method__multi_column_values(
    splitter_method_name_prefix,
    test_cases_for_glue_catalog_data_connector_spark_execution_engine,
):
    execution_engine = test_cases_for_glue_catalog_data_connector_spark_execution_engine
    my_data_connector = ConfiguredAssetGlueCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine=execution_engine,
        assets={"db_test.tb_titanic": {}},
    )

    batch_data, _, __ = my_data_connector.get_batch_data_and_metadata(
        batch_definition=BatchDefinition(
            datasource_name="FAKE_Datasource_NAME",
            data_connector_name="my_glue_catalog_data_connector",
            data_asset_name="db_test.tb_titanic",
            batch_identifiers=IDDict(),
            batch_spec_passthrough={
                "splitter_method": f"{splitter_method_name_prefix}split_on_multi_column_values",
                "splitter_kwargs": {
                    "column_names": ["Age", "PClass"],
                    "batch_identifiers": {"Age": 25, "PClass": "1st"},
                },
            },
        )
    )

    batch = Batch(data=batch_data)

    validator = Validator(execution_engine, batches=[batch])

    assert validator.expect_column_values_to_be_in_set("Age", [25]).success
    assert validator.expect_column_values_to_be_in_set("PClass", ["1st"]).success

    assert len(validator.head(fetch_all=True)) == 4


@mock_glue
@pytest.mark.parametrize("splitter_method_name_prefix", ["_", ""])
def test_get_batch_data_and_metadata_with_splitting_method__hashed_column(
    splitter_method_name_prefix,
    test_cases_for_glue_catalog_data_connector_spark_execution_engine,
):
    execution_engine = test_cases_for_glue_catalog_data_connector_spark_execution_engine
    my_data_connector = ConfiguredAssetGlueCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine=execution_engine,
        assets={"db_test.tb_titanic": {}},
    )

    batch_data, _, __ = my_data_connector.get_batch_data_and_metadata(
        batch_definition=BatchDefinition(
            datasource_name="FAKE_Datasource_NAME",
            data_connector_name="my_glue_catalog_data_connector",
            data_asset_name="db_test.tb_titanic",
            batch_identifiers=IDDict(),
            batch_spec_passthrough={
                "splitter_method": f"{splitter_method_name_prefix}split_on_hashed_column",
                "splitter_kwargs": {
                    "column_name": "Name",
                    "hash_digits": 1,
                    "hash_function_name": "md5",
                    "batch_identifiers": {"hash_value": "f"},
                },
            },
        )
    )

    batch = Batch(data=batch_data)

    validator = Validator(execution_engine, batches=[batch])

    assert len(validator.head(fetch_all=True)) == 77
