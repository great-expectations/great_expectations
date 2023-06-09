from typing import TYPE_CHECKING

import pytest

from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer
from great_expectations.core.usage_statistics.anonymizers.datasource_anonymizer import (
    DatasourceAnonymizer,
)
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.datasource import Datasource

if TYPE_CHECKING:
    from ruamel.yaml.comments import CommentedMap

yaml = YAMLHandler()


@pytest.fixture
def datasource_anonymizer() -> DatasourceAnonymizer:
    # Standardize the salt so our tests are deterimistic
    salt: str = "00000000-0000-0000-0000-00000000a004"
    aggregate_anonymizer = Anonymizer(salt=salt)
    anonymizer: DatasourceAnonymizer = DatasourceAnonymizer(
        salt=salt, aggregate_anonymizer=aggregate_anonymizer
    )
    return anonymizer


# Purely used for testing inheritance hierarchy herein
class CustomDatasource(Datasource):
    pass


def test_datasource_anonymizer(datasource_anonymizer: DatasourceAnonymizer):
    n1 = datasource_anonymizer._anonymize_datasource_info(
        name="test_datasource",
        config={
            "name": "test_datasource",
            "class_name": "Datasource",
            "module_name": "great_expectations.datasource",
            "execution_engine": {
                "class_name": "PandasExecutionEngine",
                "module_name": "great_expectations.execution_engine",
            },
        },
    )
    assert n1 == {
        "anonymized_execution_engine": {
            "anonymized_name": "6b8f8c12352592a69083f958369c7151",
            "parent_class": "PandasExecutionEngine",
        },
        "anonymized_name": "04bf89e1fb7495b0904bbd5ae478fbe0",
        "parent_class": "Datasource",
    }
    n2 = datasource_anonymizer._anonymize_datasource_info(
        name="test_datasource",
        config={
            "name": "test_datasource",
            "class_name": "CustomDatasource",
            "module_name": "tests.datasource.test_datasource_anonymizer",
            "execution_engine": {
                "class_name": "PandasExecutionEngine",
                "module_name": "great_expectations.execution_engine",
            },
        },
    )
    datasource_anonymizer_2 = DatasourceAnonymizer(aggregate_anonymizer=Anonymizer())
    n3 = datasource_anonymizer_2._anonymize_datasource_info(
        name="test_datasource",
        config={
            "name": "test_datasource",
            "class_name": "CustomDatasource",
            "module_name": "tests.datasource.test_datasource_anonymizer",
            "execution_engine": {
                "class_name": "PandasExecutionEngine",
                "module_name": "great_expectations.execution_engine",
            },
        },
    )
    assert n2["parent_class"] == "Datasource"
    assert n3["parent_class"] == "Datasource"

    assert len(n3["anonymized_class"]) == 32
    assert n2["anonymized_class"] != n3["anonymized_class"]

    # Same anonymizer *does* produce the same result
    n4 = datasource_anonymizer._anonymize_datasource_info(
        name="test_datasource",
        config={
            "name": "test_datasource",
            "class_name": "CustomDatasource",
            "module_name": "tests.datasource.test_datasource_anonymizer",
            "execution_engine": {
                "class_name": "PandasExecutionEngine",
                "module_name": "great_expectations.execution_engine",
            },
        },
    )
    assert n4["anonymized_class"] == n2["anonymized_class"]


def test_anonymize_datasource_info_v3_api_core_ge_class(
    datasource_anonymizer: DatasourceAnonymizer,
):
    name = "test_pandas_datasource"
    yaml_config = """
class_name: Datasource
module_name: great_expectations.datasource

execution_engine:
    class_name: PandasExecutionEngine
    module_name: great_expectations.execution_engine

data_connectors:
    my_filesystem_data_connector:
        class_name: InferredAssetFilesystemDataConnector
        module_name: great_expectations.datasource.data_connector
"""
    config: CommentedMap = yaml.load(yaml_config)
    anonymized_datasource = datasource_anonymizer._anonymize_datasource_info(
        name=name, config=config
    )
    assert anonymized_datasource == {
        "anonymized_data_connectors": [
            {
                "anonymized_name": "42af601aeb8a03d76bf468a462cb62f6",
                "parent_class": "InferredAssetFilesystemDataConnector",
            }
        ],
        "anonymized_execution_engine": {
            "anonymized_name": "6b8f8c12352592a69083f958369c7151",
            "parent_class": "PandasExecutionEngine",
        },
        "anonymized_name": "2642802d79d90ce6d147b0f9f61c3569",
        "parent_class": "Datasource",
    }


def test_anonymize_datasource_info_v3_api_custom_subclass(
    datasource_anonymizer: DatasourceAnonymizer,
):
    name = "test_pandas_datasource"
    yaml_config = """
module_name: tests.data_context.fixtures.plugins.my_custom_v3_api_datasource
class_name: MyCustomV3ApiDatasource

execution_engine:
    class_name: PandasExecutionEngine
    module_name: great_expectations.execution_engine

data_connectors:
    my_filesystem_data_connector:
        class_name: InferredAssetFilesystemDataConnector
        module_name: great_expectations.datasource.data_connector
"""
    config: CommentedMap = yaml.load(yaml_config)
    anonymized_datasource = datasource_anonymizer._anonymize_datasource_info(
        name=name, config=config
    )
    assert anonymized_datasource == {
        "anonymized_name": "2642802d79d90ce6d147b0f9f61c3569",
        "anonymized_class": "ae74d1b58a67f5a944bb9cda16a62472",
        "parent_class": "Datasource",
        "anonymized_execution_engine": {
            "anonymized_name": "6b8f8c12352592a69083f958369c7151",
            "parent_class": "PandasExecutionEngine",
        },
        "anonymized_data_connectors": [
            {
                "anonymized_name": "42af601aeb8a03d76bf468a462cb62f6",
                "parent_class": "InferredAssetFilesystemDataConnector",
            }
        ],
    }


def test_anonymize_simple_sqlalchemy_datasource(
    datasource_anonymizer: DatasourceAnonymizer,
):
    name = "test_simple_sqlalchemy_datasource"
    yaml_config = """
class_name: SimpleSqlalchemyDatasource
connection_string: sqlite:///some_db.db

introspection:
    whole_table_with_limits:
        sampling_method: _sample_using_limit
        sampling_kwargs:
            n: 10
"""
    config: CommentedMap = yaml.load(yaml_config)
    anonymized_datasource = (
        datasource_anonymizer._anonymize_simple_sqlalchemy_datasource(
            name=name, config=config
        )
    )
    assert anonymized_datasource == {
        "anonymized_name": "3be0aacd79b32e22a41949bf607b3e80",
        "parent_class": "SimpleSqlalchemyDatasource",
        "anonymized_execution_engine": {"parent_class": "SqlAlchemyExecutionEngine"},
        "anonymized_data_connectors": [
            {
                "anonymized_name": "d6b508db454c47ea40131b0a11415dd4",
                "parent_class": "InferredAssetSqlDataConnector",
            }
        ],
    }


def test_anonymize_custom_simple_sqlalchemy_datasource(
    datasource_anonymizer: DatasourceAnonymizer,
):
    name = "test_custom_simple_sqlalchemy_datasource"
    yaml_config = """
module_name: tests.data_context.fixtures.plugins.my_custom_simple_sqlalchemy_datasource_class
class_name: MyCustomSimpleSqlalchemyDatasource
connection_string: sqlite:///some_db.db
name: some_name
introspection:
    my_custom_datasource_name:
        data_asset_name_suffix: some_suffix
"""
    config: CommentedMap = yaml.load(yaml_config)
    anonymized_datasource = (
        datasource_anonymizer._anonymize_simple_sqlalchemy_datasource(
            name=name, config=config
        )
    )
    assert anonymized_datasource == {
        "anonymized_name": "d9e0c5f761c6ea5e54000f8c10a1049b",
        "parent_class": "SimpleSqlalchemyDatasource",
        "anonymized_class": "aab66054e62007a9ac5afbcacedaf0d2",
        "anonymized_execution_engine": {"parent_class": "SqlAlchemyExecutionEngine"},
        "anonymized_data_connectors": [
            {
                "anonymized_name": "82b8b59e076789ac1476b2b745ebc268",
                "parent_class": "InferredAssetSqlDataConnector",
            }
        ],
    }


def test_get_parent_class_yes():
    v3_batch_request_api_datasources = [
        "SimpleSqlalchemyDatasource",
        "Datasource",
        "BaseDatasource",
    ]
    parent_classes = v3_batch_request_api_datasources
    configs = [
        {
            "name": "test_datasource",
            "class_name": parent_class,
            "module_name": "great_expectations.datasource",
        }
        for parent_class in parent_classes
    ]
    for idx in range(len(configs)):
        parent_class = DatasourceAnonymizer.get_parent_class(config=configs[idx])
        assert parent_class == parent_classes[idx]


def test_is_custom_parent_class_recognized_yes():
    config = {
        "module_name": "tests.data_context.fixtures.plugins.my_custom_v3_api_datasource",
        "class_name": "MyCustomV3ApiDatasource",
    }
    parent_class = DatasourceAnonymizer.get_parent_class(config=config)
    assert parent_class == "Datasource"


def test_get_parent_class_no():
    parent_classes = ["MyCustomNonDatasourceClass", "MyOtherCustomNonDatasourceClass"]
    configs = [
        {
            "name": "test_datasource",
            "class_name": parent_class,
            "module_name": "great_expectations.datasource",
        }
        for parent_class in parent_classes
    ]
    for idx in range(len(configs)):
        parent_class = DatasourceAnonymizer.get_parent_class(config=configs[idx])
        assert parent_class != parent_classes[idx]
        assert parent_class is None


def test_get_parent_class_v3_api_yes():
    v3_batch_request_api_datasources = [
        "SimpleSqlalchemyDatasource",
        "Datasource",
        "BaseDatasource",
    ]
    parent_classes = v3_batch_request_api_datasources
    configs = [
        {
            "name": "test_datasource",
            "class_name": parent_class,
            "module_name": "great_expectations.datasource",
        }
        for parent_class in parent_classes
    ]
    for idx in range(len(configs)):
        parent_class = DatasourceAnonymizer.get_parent_class_v3_api(config=configs[idx])
        assert parent_class == parent_classes[idx]


def test_is_custom_parent_class_recognized_v3_api_yes():
    config = {
        "module_name": "tests.data_context.fixtures.plugins.my_custom_v3_api_datasource",
        "class_name": "MyCustomV3ApiDatasource",
    }
    parent_class = DatasourceAnonymizer.get_parent_class_v3_api(config=config)
    assert parent_class == "Datasource"


def test_get_parent_class_v3_api_no():
    custom_non_datsource_classes = [
        "MyCustomNonDatasourceClass",
        "MyOtherCustomNonDatasourceClass",
    ]
    parent_classes = custom_non_datsource_classes
    configs = [
        {
            "name": "test_datasource",
            "class_name": parent_class,
            "module_name": "great_expectations.datasource",
        }
        for parent_class in parent_classes
    ]
    for idx in range(len(configs)):
        parent_class = DatasourceAnonymizer.get_parent_class_v3_api(config=configs[idx])
        assert parent_class != parent_classes[idx]
        assert parent_class is None
