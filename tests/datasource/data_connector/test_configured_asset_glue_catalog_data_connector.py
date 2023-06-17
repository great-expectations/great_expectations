import pytest
from moto import mock_glue

from great_expectations import DataContext
from great_expectations.core.batch import Batch, BatchDefinition, BatchRequest
from great_expectations.core.id_dict import IDDict
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource import Datasource
from great_expectations.datasource.data_connector import (
    ConfiguredAssetAWSGlueDataCatalogDataConnector,
)
from great_expectations.exceptions import DataConnectorError
from great_expectations.validator.validator import Validator

yaml = YAMLHandler()


def test_instantiation_with_partitions_in_connector(glue_titanic_catalog):
    config = yaml.load(
        """
    name: my_glue_catalog_data_connector
    datasource_name: FAKE_Datasource_NAME
    partitions:
        - PClass
        - SexCode

    assets:
        db_test.tb_titanic:
            table_name: tb_titanic_with_partitions
            database_name: db_test
            splitter_method: split_on_column_value
            splitter_kwargs:
                column_name: PClass
            partitions:
                - SexCode
        asset2:
            table_name: tb_titanic_with_partitions
            database_name: db_test
            sampling_method: _sample_using_random
            sampling_kwargs:
                p: 0.5
                seed: 0
            partitions:
                - PClass
        asset3:
            table_name: tb_titanic_with_partitions
            database_name: db_test
    """,
    )
    config["execution_engine"] = "execution_engine"

    my_data_connector = ConfiguredAssetAWSGlueDataCatalogDataConnector(**config)

    report = my_data_connector.self_check()
    assert report == {
        "class_name": "ConfiguredAssetAWSGlueDataCatalogDataConnector",
        "data_asset_count": 3,
        "example_data_asset_names": ["asset2", "asset3", "db_test.tb_titanic"],
        "data_assets": {
            "asset2": {
                "batch_definition_count": 3,
                "example_data_references": [
                    {"PClass": "1st"},
                    {"PClass": "2nd"},
                    {"PClass": "3rd"},
                ],
            },
            "asset3": {
                "batch_definition_count": 6,
                "example_data_references": [
                    {"PClass": "1st", "SexCode": "0"},
                    {"PClass": "1st", "SexCode": "1"},
                    {"PClass": "2nd", "SexCode": "0"},
                ],
            },
            "db_test.tb_titanic": {
                "batch_definition_count": 2,
                "example_data_references": [
                    {"SexCode": "0"},
                    {"SexCode": "1"},
                ],
            },
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
    }
    # noinspection PyProtectedMember
    assert len(my_data_connector.get_available_data_asset_names()) == 3
    assert my_data_connector.get_unmatched_data_references() == []


def test_basic_instantiation(glue_titanic_catalog):
    config = yaml.load(
        """
    name: my_glue_catalog_data_connector
    datasource_name: FAKE_Datasource_NAME

    assets:
        db_test.tb_titanic:
            table_name: tb_titanic_with_partitions
            database_name: db_test
            splitter_method: split_on_column_value
            splitter_kwargs:
                column_name: PClass
        asset2:
            table_name: tb_titanic_without_partitions
            database_name: db_test
            sampling_method: _sample_using_random
            sampling_kwargs:
                p: 0.5
                seed: 0
            partitions:
                - PClass
        asset3:
            table_name: tb_titanic_with_partitions
            database_name: db_test
            partitions:
                - PClass
        asset4:
            table_name: tb_titanic_with_partitions
            database_name: db_test
            partitions:
                - PClass
                - SexCode
    """,
    )
    config["execution_engine"] = "execution_engine"

    my_data_connector = ConfiguredAssetAWSGlueDataCatalogDataConnector(**config)

    report = my_data_connector.self_check()
    assert report == {
        "class_name": "ConfiguredAssetAWSGlueDataCatalogDataConnector",
        "data_asset_count": 4,
        "example_data_asset_names": ["asset2", "asset3", "asset4"],
        "data_assets": {
            "asset2": {
                "batch_definition_count": 1,
                "example_data_references": [{}],
            },
            "asset3": {
                "batch_definition_count": 3,
                "example_data_references": [
                    {"PClass": "1st"},
                    {"PClass": "2nd"},
                    {"PClass": "3rd"},
                ],
            },
            "asset4": {
                "batch_definition_count": 6,
                "example_data_references": [
                    {"PClass": "1st", "SexCode": "0"},
                    {"PClass": "1st", "SexCode": "1"},
                    {"PClass": "2nd", "SexCode": "0"},
                ],
            },
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
    }
    # noinspection PyProtectedMember
    assert len(my_data_connector.get_available_data_asset_names()) == 4
    assert my_data_connector.get_unmatched_data_references() == []


def test_invalid_instantiation(glue_titanic_catalog):
    config = yaml.load(
        """
    name: my_glue_catalog_data_connector
    datasource_name: FAKE_Datasource_NAME

    assets:
        asset1:
            table_name: tb_titanic_with_partitions
            database_name: db_test
            partitions:
                PClass
    """,
    )
    config["execution_engine"] = "execution_engine"
    with pytest.raises(DataConnectorError) as excinfo:
        ConfiguredAssetAWSGlueDataCatalogDataConnector(**config)
    assert "partitions" in str(excinfo.value)

    config = yaml.load(
        """
    name: my_glue_catalog_data_connector
    datasource_name: FAKE_Datasource_NAME

    assets:
        asset1:
            table_name: tb_titanic_with_partitions
            partitions:
                - PClass
    """,
    )
    config["execution_engine"] = "execution_engine"

    with pytest.raises(DataConnectorError) as excinfo:
        ConfiguredAssetAWSGlueDataCatalogDataConnector(**config)
    assert "database_name" in str(excinfo.value)

    config = yaml.load(
        """
    name: my_glue_catalog_data_connector
    datasource_name: FAKE_Datasource_NAME

    assets:
        asset1:
            database_name: db_test
            partitions:
                - PClass
    """,
    )
    config["execution_engine"] = "execution_engine"

    with pytest.raises(DataConnectorError) as excinfo:
        ConfiguredAssetAWSGlueDataCatalogDataConnector(**config)
    assert "table_name" in str(excinfo.value)


def test_instantiation_from_a_config(
    empty_data_context_stats_enabled, glue_titanic_catalog
):
    context: DataContext = empty_data_context_stats_enabled
    report_object = context.test_yaml_config(
        """
    module_name: great_expectations.datasource.data_connector
    class_name: ConfiguredAssetAWSGlueDataCatalogDataConnector
    name: my_glue_catalog_data_connector
    datasource_name: FAKE_Datasource_NAME

    assets:
        db_test.tb_titanic:
            table_name: tb_titanic_with_partitions
            database_name: db_test
            splitter_method: split_on_column_value
            splitter_kwargs:
                column_name: PClass
        asset2:
            table_name: tb_titanic_without_partitions
            database_name: db_test
            sampling_method: _sample_using_random
            sampling_kwargs:
                p: 0.5
                seed: 0
        asset3:
            table_name: tb_titanic_with_partitions
            database_name: db_test
            partitions:
                - PClass
        asset4:
            table_name: tb_titanic_with_partitions
            database_name: db_test
            partitions:
                - PClass
                - SexCode
    """,
        runtime_environment={
            "execution_engine": "execution_engine",
        },
        return_mode="report_object",
    )

    assert report_object == {
        "class_name": "ConfiguredAssetAWSGlueDataCatalogDataConnector",
        "data_asset_count": 4,
        "example_data_asset_names": ["asset2", "asset3", "asset4"],
        "data_assets": {
            "asset2": {
                "batch_definition_count": 1,
                "example_data_references": [{}],
            },
            "asset3": {
                "batch_definition_count": 3,
                "example_data_references": [
                    {"PClass": "1st"},
                    {"PClass": "2nd"},
                    {"PClass": "3rd"},
                ],
            },
            "asset4": {
                "batch_definition_count": 6,
                "example_data_references": [
                    {"PClass": "1st", "SexCode": "0"},
                    {"PClass": "1st", "SexCode": "1"},
                    {"PClass": "2nd", "SexCode": "0"},
                ],
            },
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
    }


def test_get_batch_definition_list_from_batch_request(glue_titanic_catalog):
    my_data_connector_yaml = yaml.load(
        f"""
        module_name: great_expectations.datasource.data_connector
        class_name: ConfiguredAssetAWSGlueDataCatalogDataConnector
        datasource_name: FAKE_Datasource_NAME
        name: my_glue_catalog_data_connector
        assets:
            with_partitions:
                database_name: db_test
                table_name: tb_titanic_with_partitions
                partitions:
                    - PClass
                    - SexCode
            without_partitions:
                database_name: db_test
                table_name: tb_titanic_with_partitions
        """,  # noqa: F541
    )
    my_data_connector: ConfiguredAssetAWSGlueDataCatalogDataConnector = (
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
    batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_glue_catalog_data_connector",
                data_asset_name="with_partitions",
                data_connector_query={"batch_filter_parameters": {"PClass": "1st"}},
            )
        )
    )
    assert len(batch_definition_list) == 2
    assert batch_definition_list[0]["batch_identifiers"] == {
        "PClass": "1st",
        "SexCode": "0",
    }
    assert batch_definition_list[1]["batch_identifiers"] == {
        "PClass": "1st",
        "SexCode": "1",
    }

    batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_glue_catalog_data_connector",
                data_asset_name="with_partitions",
                data_connector_query={"batch_filter_parameters": {"SexCode": "0"}},
            )
        )
    )
    assert len(batch_definition_list) == 3
    assert batch_definition_list[0]["batch_identifiers"] == {
        "PClass": "1st",
        "SexCode": "0",
    }
    assert batch_definition_list[1]["batch_identifiers"] == {
        "PClass": "2nd",
        "SexCode": "0",
    }
    assert batch_definition_list[2]["batch_identifiers"] == {
        "PClass": "3rd",
        "SexCode": "0",
    }

    batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_glue_catalog_data_connector",
                data_asset_name="with_partitions",
                data_connector_query={"batch_filter_parameters": {}},
            )
        )
    )
    assert len(batch_definition_list) == 6

    batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_glue_catalog_data_connector",
                data_asset_name="with_partitions",
                data_connector_query={"batch_filter_parameters": {"SexCode": "1st"}},
            )
        )
    )
    assert len(batch_definition_list) == 0

    batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_glue_catalog_data_connector",
                data_asset_name="with_partitions",
            )
        )
    )
    assert len(batch_definition_list) == 6

    batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_glue_catalog_data_connector",
                data_asset_name="without_partitions",
                data_connector_query={"batch_filter_parameters": {"PClass": "1st"}},
            )
        )
    )
    assert len(batch_definition_list) == 0

    batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_glue_catalog_data_connector",
                data_asset_name="without_partitions",
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
    # noinspection PyTypeChecker
    my_data_connector = ConfiguredAssetAWSGlueDataCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine="execution_engine",
    )
    assert len(my_data_connector.assets) == 0


def test_instantiation_with_batch_spec_passthrough(
    test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine,
    glue_titanic_catalog,
):
    execution_engine = (
        test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine
    )
    my_data_connector = ConfiguredAssetAWSGlueDataCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine=execution_engine,
        assets={
            "titanic_asset": {
                "table_name": "tb_titanic_with_partitions",
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


def test_instantiation_with_asset_suffix_and_prefix(glue_titanic_catalog):
    # noinspection PyTypeChecker
    my_data_connector = ConfiguredAssetAWSGlueDataCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine="execution_engine",
        assets={
            "db_test.tb_titanic": {
                "table_name": "tb_titanic_with_partitions",
                "database_name": "db_test",
                "data_asset_name_prefix": "prefix__",
                "data_asset_name_suffix": "__suffix",
            },
        },
    )
    assert "prefix__db_test.tb_titanic__suffix" in my_data_connector.assets


@pytest.mark.parametrize("splitter_method_name_prefix", ["_", ""])
def test_instantiation_with_splitter_sampling_and_prefix(
    splitter_method_name_prefix, glue_titanic_catalog
):
    # noinspection PyTypeChecker
    my_data_connector = ConfiguredAssetAWSGlueDataCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine="execution_engine",
        assets={
            "db_test.tb_titanic": {
                "table_name": "tb_titanic_with_partitions",
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

    data_asset_name = "prefix__db_test.tb_titanic__suffix"
    asset_config = my_data_connector.assets.get(data_asset_name, None)
    assert asset_config and asset_config == {
        "table_name": "tb_titanic_with_partitions",
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
    }


@pytest.mark.parametrize("splitter_method_name_prefix", ["_", ""])
def test_build_batch_spec_with_all_options(
    splitter_method_name_prefix, glue_titanic_catalog
):
    my_data_connector = ConfiguredAssetAWSGlueDataCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine="execution_engine",
        assets={
            "asset_titanic": {
                "table_name": "tb_titanic_with_partitions",
                "database_name": "db_test",
                "splitter_method": f"{splitter_method_name_prefix}split_on_column_value",
                "splitter_kwargs": {
                    "column_name": "PClass",
                    "batch_identifiers": {"PClass": "1st"},
                },
                "sampling_method": f"{splitter_method_name_prefix}sample_using_random",
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
    )

    batch_spec = my_data_connector.build_batch_spec(batch_definition)
    assert batch_spec.database_name == "db_test"
    assert batch_spec.table_name == "tb_titanic_with_partitions"
    assert batch_spec.get("data_asset_name", None) == "prefix__asset_titanic__suffix"
    assert (
        batch_spec.get("splitter_method", None)
        == f"{splitter_method_name_prefix}split_on_column_value"
    )
    assert batch_spec.get("splitter_kwargs", {}) == {
        "column_name": "PClass",
        "batch_identifiers": {"PClass": "1st"},
    }
    assert (
        batch_spec.get("sampling_method", None)
        == f"{splitter_method_name_prefix}sample_using_random"
    )
    assert batch_spec.get("sampling_kwargs", {}) == {"p": 0.5, "seed": 0}
    assert batch_spec.get("data_asset_name_prefix", None) == "prefix__"
    assert batch_spec.get("data_asset_name_suffix", None) == "__suffix"


def test_build_batch_spec_with_minimal_options(glue_titanic_catalog):
    # noinspection PyTypeChecker
    my_data_connector = ConfiguredAssetAWSGlueDataCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine="test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine",
        assets={
            "db_test.tb_titanic": {
                "table_name": "tb_titanic_with_partitions",
                "database_name": "db_test",
            },
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
    assert batch_spec.table_name == "tb_titanic_with_partitions"
    assert batch_spec.get("data_asset_name", None) == "db_test.tb_titanic"


def test_get_unmatched_data_references(glue_titanic_catalog):
    # noinspection PyTypeChecker
    my_data_connector = ConfiguredAssetAWSGlueDataCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine="test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine",
        assets={
            "db_test.tb_titanic": {
                "table_name": "tb_titanic_with_partitions",
                "database_name": "db_test",
            },
        },
    )
    unmatched_references = my_data_connector.get_unmatched_data_references()
    assert len(unmatched_references) == 0


def test_get_available_data_asset_names(glue_titanic_catalog):
    # noinspection PyTypeChecker
    my_data_connector = ConfiguredAssetAWSGlueDataCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine="test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine",
        assets={
            "asset1": {
                "table_name": "tb_titanic_with_partitions",
                "database_name": "db_test",
            }
        },
    )
    data_asset_names = my_data_connector.get_available_data_asset_names()
    assert "asset1" in data_asset_names


def test_get_batch_data_and_metadata_with_sampling_method__random_in_asset_config(
    test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine,
    glue_titanic_catalog,
):
    execution_engine = (
        test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine
    )
    my_data_connector = ConfiguredAssetAWSGlueDataCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine=execution_engine,
        assets={
            "titanic_asset": {
                "table_name": "tb_titanic_with_partitions",
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


def test_get_batch_data_and_metadata_with_sampling_method__random_in_batch_spec_passthrough(
    test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine,
    glue_titanic_catalog,
):
    execution_engine = (
        test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine
    )
    my_data_connector = ConfiguredAssetAWSGlueDataCatalogDataConnector(
        name="my_glue_catalog_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine=execution_engine,
        assets={
            "titanic_asset": {
                "table_name": "tb_titanic_with_partitions",
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


def test_get_batch_data_and_metadata_with_sampling_method__mod(
    in_memory_runtime_context,
    test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine,
    glue_titanic_catalog,
):
    execution_engine = (
        test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine
    )
    in_memory_runtime_context.datasources["FAKE_Datasource_NAME"] = Datasource(
        name="FAKE_Datasource_NAME",
        # Configuration for "execution_engine" here is largely placeholder to comply with "Datasource" constructor.
        execution_engine=execution_engine.config,
        data_connectors={
            "my_glue_catalog_data_connector": {
                "class_name": "ConfiguredAssetAWSGlueDataCatalogDataConnector",
                "assets": {
                    "db_test.tb_titanic": {
                        "table_name": "tb_titanic_with_partitions",
                        "database_name": "db_test",
                    },
                },
            },
        },
    )
    # Updating "execution_engine" to insure peculiarities, incorporated herein, propagate to "ExecutionEngine" itself.
    in_memory_runtime_context.datasources["FAKE_Datasource_NAME"]._execution_engine = execution_engine  # type: ignore[union-attr]

    batch_definition = BatchDefinition(
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
    my_data_connector = in_memory_runtime_context.datasources[
        "FAKE_Datasource_NAME"
    ].data_connectors["my_glue_catalog_data_connector"]
    batch_data, _, __ = my_data_connector.get_batch_data_and_metadata(
        batch_definition=batch_definition
    )
    batch = Batch(data=batch_data, batch_definition=batch_definition)
    validator = Validator(
        execution_engine, batches=[batch], data_context=in_memory_runtime_context
    )
    assert validator.expect_column_values_to_not_be_in_set("Age", [25, 50]).success
    assert len(validator.head(fetch_all=True)) == 38


def test_get_batch_data_and_metadata_with_sampling_method__list(
    in_memory_runtime_context,
    test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine,
    glue_titanic_catalog,
):
    execution_engine = (
        test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine
    )
    in_memory_runtime_context.datasources["FAKE_Datasource_NAME"] = Datasource(
        name="FAKE_Datasource_NAME",
        # Configuration for "execution_engine" here is largely placeholder to comply with "Datasource" constructor.
        execution_engine=execution_engine.config,
        data_connectors={
            "my_glue_catalog_data_connector": {
                "class_name": "ConfiguredAssetAWSGlueDataCatalogDataConnector",
                "assets": {
                    "db_test.tb_titanic": {
                        "table_name": "tb_titanic_with_partitions",
                        "database_name": "db_test",
                    },
                },
            },
        },
    )
    # Updating "execution_engine" to insure peculiarities, incorporated herein, propagate to "ExecutionEngine" itself.
    in_memory_runtime_context.datasources["FAKE_Datasource_NAME"]._execution_engine = execution_engine  # type: ignore[union-attr]

    batch_definition = BatchDefinition(
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
    my_data_connector = in_memory_runtime_context.datasources[
        "FAKE_Datasource_NAME"
    ].data_connectors["my_glue_catalog_data_connector"]
    batch_data, _, __ = my_data_connector.get_batch_data_and_metadata(
        batch_definition=batch_definition
    )
    batch = Batch(data=batch_data, batch_definition=batch_definition)
    validator = Validator(
        execution_engine, batches=[batch], data_context=in_memory_runtime_context
    )
    assert validator.expect_column_values_to_be_in_set("PClass", ["1st"]).success
    assert len(validator.head(fetch_all=True)) == 322


def test_get_batch_data_and_metadata_with_sampling_method__hash(
    in_memory_runtime_context,
    test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine,
    glue_titanic_catalog,
):
    execution_engine = (
        test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine
    )
    in_memory_runtime_context.datasources["FAKE_Datasource_NAME"] = Datasource(
        name="FAKE_Datasource_NAME",
        # Configuration for "execution_engine" here is largely placeholder to comply with "Datasource" constructor.
        execution_engine=execution_engine.config,
        data_connectors={
            "my_glue_catalog_data_connector": {
                "class_name": "ConfiguredAssetAWSGlueDataCatalogDataConnector",
                "assets": {
                    "db_test.tb_titanic": {
                        "table_name": "tb_titanic_with_partitions",
                        "database_name": "db_test",
                    },
                },
            },
        },
    )
    # Updating "execution_engine" to insure peculiarities, incorporated herein, propagate to "ExecutionEngine" itself.
    in_memory_runtime_context.datasources["FAKE_Datasource_NAME"]._execution_engine = execution_engine  # type: ignore[union-attr]

    batch_definition = BatchDefinition(
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
    my_data_connector = in_memory_runtime_context.datasources[
        "FAKE_Datasource_NAME"
    ].data_connectors["my_glue_catalog_data_connector"]
    batch_data, _, __ = my_data_connector.get_batch_data_and_metadata(
        batch_definition=batch_definition
    )
    batch = Batch(data=batch_data, batch_definition=batch_definition)
    validator = Validator(
        execution_engine, batches=[batch], data_context=in_memory_runtime_context
    )
    assert len(validator.head(fetch_all=True)) == 77


@pytest.mark.parametrize("splitter_method_name_prefix", ["_", ""])
def test_get_batch_data_and_metadata_with_splitting_method__whole_table(
    in_memory_runtime_context,
    splitter_method_name_prefix,
    test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine,
    glue_titanic_catalog,
):
    execution_engine = (
        test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine
    )
    in_memory_runtime_context.datasources["FAKE_Datasource_NAME"] = Datasource(
        name="FAKE_Datasource_NAME",
        # Configuration for "execution_engine" here is largely placeholder to comply with "Datasource" constructor.
        execution_engine=execution_engine.config,
        data_connectors={
            "my_glue_catalog_data_connector": {
                "class_name": "ConfiguredAssetAWSGlueDataCatalogDataConnector",
                "assets": {
                    "db_test.tb_titanic": {
                        "table_name": "tb_titanic_with_partitions",
                        "database_name": "db_test",
                    },
                },
            },
        },
    )
    # Updating "execution_engine" to insure peculiarities, incorporated herein, propagate to "ExecutionEngine" itself.
    in_memory_runtime_context.datasources["FAKE_Datasource_NAME"]._execution_engine = execution_engine  # type: ignore[union-attr]

    batch_definition = BatchDefinition(
        datasource_name="FAKE_Datasource_NAME",
        data_connector_name="my_glue_catalog_data_connector",
        data_asset_name="db_test.tb_titanic",
        batch_identifiers=IDDict(),
        batch_spec_passthrough={
            "splitter_method": f"{splitter_method_name_prefix}split_on_whole_table",
        },
    )
    my_data_connector = in_memory_runtime_context.datasources[
        "FAKE_Datasource_NAME"
    ].data_connectors["my_glue_catalog_data_connector"]
    batch_data, _, __ = my_data_connector.get_batch_data_and_metadata(
        batch_definition=batch_definition
    )
    batch = Batch(data=batch_data, batch_definition=batch_definition)
    validator = Validator(
        execution_engine, batches=[batch], data_context=in_memory_runtime_context
    )
    assert len(validator.head(fetch_all=True)) == 1313


@pytest.mark.parametrize("splitter_method_name_prefix", ["_", ""])
def test_get_batch_data_and_metadata_with_splitting_method__column_value(
    in_memory_runtime_context,
    splitter_method_name_prefix,
    test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine,
    glue_titanic_catalog,
):
    execution_engine = (
        test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine
    )
    in_memory_runtime_context.datasources["FAKE_Datasource_NAME"] = Datasource(
        name="FAKE_Datasource_NAME",
        # Configuration for "execution_engine" here is largely placeholder to comply with "Datasource" constructor.
        execution_engine=execution_engine.config,
        data_connectors={
            "my_glue_catalog_data_connector": {
                "class_name": "ConfiguredAssetAWSGlueDataCatalogDataConnector",
                "assets": {
                    "db_test.tb_titanic": {
                        "table_name": "tb_titanic_with_partitions",
                        "database_name": "db_test",
                    },
                },
            },
        },
    )
    # Updating "execution_engine" to insure peculiarities, incorporated herein, propagate to "ExecutionEngine" itself.
    in_memory_runtime_context.datasources["FAKE_Datasource_NAME"]._execution_engine = execution_engine  # type: ignore[union-attr]

    batch_definition = BatchDefinition(
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
    my_data_connector = in_memory_runtime_context.datasources[
        "FAKE_Datasource_NAME"
    ].data_connectors["my_glue_catalog_data_connector"]
    batch_data, _, __ = my_data_connector.get_batch_data_and_metadata(
        batch_definition=batch_definition
    )
    batch = Batch(data=batch_data, batch_definition=batch_definition)
    validator = Validator(
        execution_engine, batches=[batch], data_context=in_memory_runtime_context
    )
    assert validator.expect_column_values_to_be_in_set("PClass", ["1st"]).success
    assert len(validator.head(fetch_all=True)) == 322


@pytest.mark.parametrize("splitter_method_name_prefix", ["_", ""])
def test_get_batch_data_and_metadata_with_splitting_method__divided_integer(
    in_memory_runtime_context,
    splitter_method_name_prefix,
    test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine,
    glue_titanic_catalog,
):
    execution_engine = (
        test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine
    )
    in_memory_runtime_context.datasources["FAKE_Datasource_NAME"] = Datasource(
        name="FAKE_Datasource_NAME",
        # Configuration for "execution_engine" here is largely placeholder to comply with "Datasource" constructor.
        execution_engine=execution_engine.config,
        data_connectors={
            "my_glue_catalog_data_connector": {
                "class_name": "ConfiguredAssetAWSGlueDataCatalogDataConnector",
                "assets": {
                    "db_test.tb_titanic": {
                        "table_name": "tb_titanic_with_partitions",
                        "database_name": "db_test",
                    },
                },
            },
        },
    )
    # Updating "execution_engine" to insure peculiarities, incorporated herein, propagate to "ExecutionEngine" itself.
    in_memory_runtime_context.datasources["FAKE_Datasource_NAME"]._execution_engine = execution_engine  # type: ignore[union-attr]

    batch_definition = BatchDefinition(
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
    my_data_connector = in_memory_runtime_context.datasources[
        "FAKE_Datasource_NAME"
    ].data_connectors["my_glue_catalog_data_connector"]
    batch_data, _, __ = my_data_connector.get_batch_data_and_metadata(
        batch_definition=batch_definition
    )
    batch = Batch(data=batch_data, batch_definition=batch_definition)
    validator = Validator(
        execution_engine, batches=[batch], data_context=in_memory_runtime_context
    )
    assert validator.expect_column_values_to_be_in_set(
        "Age", list(range(20, 40))
    ).success
    assert len(validator.head(fetch_all=True)) == 420


@pytest.mark.parametrize("splitter_method_name_prefix", ["_", ""])
def test_get_batch_data_and_metadata_with_splitting_method__mod_integer(
    in_memory_runtime_context,
    splitter_method_name_prefix,
    test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine,
    glue_titanic_catalog,
):
    execution_engine = (
        test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine
    )
    in_memory_runtime_context.datasources["FAKE_Datasource_NAME"] = Datasource(
        name="FAKE_Datasource_NAME",
        # Configuration for "execution_engine" here is largely placeholder to comply with "Datasource" constructor.
        execution_engine=execution_engine.config,
        data_connectors={
            "my_glue_catalog_data_connector": {
                "class_name": "ConfiguredAssetAWSGlueDataCatalogDataConnector",
                "assets": {
                    "db_test.tb_titanic": {
                        "table_name": "tb_titanic_with_partitions",
                        "database_name": "db_test",
                    },
                },
            },
        },
    )
    # Updating "execution_engine" to insure peculiarities, incorporated herein, propagate to "ExecutionEngine" itself.
    in_memory_runtime_context.datasources["FAKE_Datasource_NAME"]._execution_engine = execution_engine  # type: ignore[union-attr]

    batch_definition = BatchDefinition(
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
    my_data_connector = in_memory_runtime_context.datasources[
        "FAKE_Datasource_NAME"
    ].data_connectors["my_glue_catalog_data_connector"]
    batch_data, _, __ = my_data_connector.get_batch_data_and_metadata(
        batch_definition=batch_definition
    )
    batch = Batch(data=batch_data, batch_definition=batch_definition)
    validator = Validator(
        execution_engine, batches=[batch], data_context=in_memory_runtime_context
    )
    assert validator.expect_column_values_to_be_in_set("Age", [26, 1, 1.5, 51]).success
    assert len(validator.head(fetch_all=True)) == 38


@pytest.mark.parametrize("splitter_method_name_prefix", ["_", ""])
def test_get_batch_data_and_metadata_with_splitting_method__multi_column_values(
    in_memory_runtime_context,
    splitter_method_name_prefix,
    test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine,
    glue_titanic_catalog,
):
    execution_engine = (
        test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine
    )
    in_memory_runtime_context.datasources["FAKE_Datasource_NAME"] = Datasource(
        name="FAKE_Datasource_NAME",
        # Configuration for "execution_engine" here is largely placeholder to comply with "Datasource" constructor.
        execution_engine=execution_engine.config,
        data_connectors={
            "my_glue_catalog_data_connector": {
                "class_name": "ConfiguredAssetAWSGlueDataCatalogDataConnector",
                "assets": {
                    "db_test.tb_titanic": {
                        "table_name": "tb_titanic_with_partitions",
                        "database_name": "db_test",
                    },
                },
            },
        },
    )
    # Updating "execution_engine" to insure peculiarities, incorporated herein, propagate to "ExecutionEngine" itself.
    in_memory_runtime_context.datasources["FAKE_Datasource_NAME"]._execution_engine = execution_engine  # type: ignore[union-attr]

    batch_definition = BatchDefinition(
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
    my_data_connector = in_memory_runtime_context.datasources[
        "FAKE_Datasource_NAME"
    ].data_connectors["my_glue_catalog_data_connector"]
    batch_data, _, __ = my_data_connector.get_batch_data_and_metadata(
        batch_definition=batch_definition
    )
    batch = Batch(data=batch_data, batch_definition=batch_definition)
    validator = Validator(
        execution_engine, batches=[batch], data_context=in_memory_runtime_context
    )
    assert validator.expect_column_values_to_be_in_set("Age", [25]).success
    assert validator.expect_column_values_to_be_in_set("PClass", ["1st"]).success
    assert len(validator.head(fetch_all=True)) == 4


@pytest.mark.parametrize("splitter_method_name_prefix", ["_", ""])
def test_get_batch_data_and_metadata_with_splitting_method__hashed_column(
    in_memory_runtime_context,
    splitter_method_name_prefix,
    test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine,
    glue_titanic_catalog,
):
    execution_engine = (
        test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine
    )
    in_memory_runtime_context.datasources["FAKE_Datasource_NAME"] = Datasource(
        name="FAKE_Datasource_NAME",
        # Configuration for "execution_engine" here is largely placeholder to comply with "Datasource" constructor.
        execution_engine=execution_engine.config,
        data_connectors={
            "my_glue_catalog_data_connector": {
                "class_name": "ConfiguredAssetAWSGlueDataCatalogDataConnector",
                "assets": {
                    "db_test.tb_titanic": {
                        "table_name": "tb_titanic_with_partitions",
                        "database_name": "db_test",
                    },
                },
            },
        },
    )
    # Updating "execution_engine" to insure peculiarities, incorporated herein, propagate to "ExecutionEngine" itself.
    in_memory_runtime_context.datasources["FAKE_Datasource_NAME"]._execution_engine = execution_engine  # type: ignore[union-attr]

    batch_definition = BatchDefinition(
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
    my_data_connector = in_memory_runtime_context.datasources[
        "FAKE_Datasource_NAME"
    ].data_connectors["my_glue_catalog_data_connector"]
    batch_data, _, __ = my_data_connector.get_batch_data_and_metadata(
        batch_definition=batch_definition
    )
    batch = Batch(data=batch_data, batch_definition=batch_definition)
    validator = Validator(
        execution_engine, batches=[batch], data_context=in_memory_runtime_context
    )
    assert len(validator.head(fetch_all=True)) == 77


def test_get_batch_data_and_metadata_with_partitions(
    in_memory_runtime_context,
    test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine,
    glue_titanic_catalog,
):
    execution_engine = (
        test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine
    )
    in_memory_runtime_context.datasources["FAKE_Datasource_NAME"] = Datasource(
        name="FAKE_Datasource_NAME",
        # Configuration for "execution_engine" here is largely placeholder to comply with "Datasource" constructor.
        execution_engine=execution_engine.config,
        data_connectors={
            "my_glue_catalog_data_connector": {
                "class_name": "ConfiguredAssetAWSGlueDataCatalogDataConnector",
                "assets": {
                    "db_test.tb_titanic": {
                        "table_name": "tb_titanic_with_partitions",
                        "database_name": "db_test",
                        "partitions": ["PClass", "SexCode"],
                    },
                },
            },
        },
    )
    # Updating "execution_engine" to insure peculiarities, incorporated herein, propagate to "ExecutionEngine" itself.
    in_memory_runtime_context.datasources["FAKE_Datasource_NAME"]._execution_engine = execution_engine  # type: ignore[union-attr]

    batch_definition = BatchDefinition(
        datasource_name="FAKE_Datasource_NAME",
        data_connector_name="my_glue_catalog_data_connector",
        data_asset_name="db_test.tb_titanic",
        batch_identifiers=IDDict({"PClass": "2nd", "SexCode": "1"}),
    )
    my_data_connector = in_memory_runtime_context.datasources[
        "FAKE_Datasource_NAME"
    ].data_connectors["my_glue_catalog_data_connector"]
    batch_data, _, __ = my_data_connector.get_batch_data_and_metadata(
        batch_definition=batch_definition
    )
    batch = Batch(data=batch_data, batch_definition=batch_definition)
    validator = Validator(
        execution_engine, batches=[batch], data_context=in_memory_runtime_context
    )
    assert validator.expect_column_values_to_be_in_set("PClass", ["2nd"]).success
    assert validator.expect_column_values_to_be_in_set("SexCode", ["1"]).success
