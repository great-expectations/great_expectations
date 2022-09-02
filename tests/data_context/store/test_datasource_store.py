import copy
import pathlib
from typing import List, Optional, cast
from unittest.mock import PropertyMock, patch

import pytest

from great_expectations.core.data_context_key import DataContextVariableKey
from great_expectations.core.serializer import (
    AbstractConfigSerializer,
    DictConfigSerializer,
    JsonConfigSerializer,
)
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.data_context.data_context import DataContext
from great_expectations.data_context.data_context_variables import (
    DataContextVariableSchema,
)
from great_expectations.data_context.store.datasource_store import DatasourceStore
from great_expectations.data_context.store.ge_cloud_store_backend import (
    GeCloudRESTResource,
)
from great_expectations.data_context.types.base import (
    DatasourceConfig,
    datasourceConfigSchema,
)
from great_expectations.data_context.types.resource_identifiers import GeCloudIdentifier
from great_expectations.datasource.datasource_serializer import (
    YAMLReadyDictDatasourceConfigSerializer,
)

yaml = YAMLHandler()


@pytest.fixture
def empty_datasource_store(datasource_store_name: str) -> DatasourceStore:
    return DatasourceStore(
        store_name=datasource_store_name,
        serializer=DictConfigSerializer(schema=datasourceConfigSchema),
    )


@pytest.fixture
def datasource_store_with_single_datasource(
    datasource_name: str,
    datasource_config: DatasourceConfig,
    empty_datasource_store: DatasourceStore,
) -> DatasourceStore:
    key = DataContextVariableKey(
        resource_name=datasource_name,
    )
    empty_datasource_store.set(key=key, value=datasource_config)
    return empty_datasource_store


@pytest.mark.unit
def test_datasource_store_with_bad_key_raises_error(
    empty_datasource_store: DatasourceStore, datasource_config: DatasourceConfig
) -> None:
    store: DatasourceStore = empty_datasource_store

    error_msg: str = "key must be an instance of DataContextVariableKey"

    with pytest.raises(TypeError) as e:
        store.set(key="my_bad_key", value=datasource_config)
    assert error_msg in str(e.value)

    with pytest.raises(TypeError) as e:
        store.get(key="my_bad_key")
    assert error_msg in str(e.value)


def _assert_serialized_datasource_configs_are_equal(
    datasource_configs: List[DatasourceConfig],
    serializers: Optional[List[AbstractConfigSerializer]] = None,
) -> None:
    """Assert that the datasource configs are equal using the DictConfigSerializer

    Args:
        datasource_configs: List of datasource configs to check

    Returns:
        None

    Raises:
        AssertionError
    """
    if len(datasource_configs) <= 1:
        raise AssertionError("Must provide at least 2 datasource configs")

    if serializers is None:
        serializers = [DictConfigSerializer(schema=datasourceConfigSchema)] * (
            len(datasource_configs) + 1
        )
    else:
        if len(serializers) <= 1:
            raise AssertionError("Must provide at least 2 datasource serializers")
        if not len(datasource_configs) == len(serializers):
            raise AssertionError(
                "Must provide the same number of serializers as datasource configs"
            )

    for idx, config in enumerate(datasource_configs[:-1]):
        assert serializers[idx].serialize(config) == serializers[idx + 1].serialize(
            datasource_configs[idx + 1]
        )


@pytest.mark.unit
def test__assert_serialized_datasource_configs_are_equal(
    datasource_config: DatasourceConfig, datasource_config_with_names: DatasourceConfig
) -> None:
    """Verify test helper method."""

    # Input errors:
    with pytest.raises(AssertionError):
        _assert_serialized_datasource_configs_are_equal([])

    with pytest.raises(AssertionError):
        _assert_serialized_datasource_configs_are_equal([datasource_config])

    # Happy path
    _assert_serialized_datasource_configs_are_equal(
        [datasource_config, datasource_config]
    )
    _assert_serialized_datasource_configs_are_equal(
        [datasource_config, datasource_config, datasource_config]
    )

    # Unequal configs
    with pytest.raises(AssertionError):
        _assert_serialized_datasource_configs_are_equal(
            [datasource_config, datasource_config_with_names]
        )

    with pytest.raises(AssertionError):
        _assert_serialized_datasource_configs_are_equal(
            [datasource_config, datasource_config, datasource_config_with_names]
        )

    with pytest.raises(AssertionError):
        _assert_serialized_datasource_configs_are_equal(
            [
                datasource_config,
                datasource_config,
                datasource_config_with_names,
                datasource_config,
            ]
        )


@pytest.mark.integration
def test_datasource_store_retrieval(
    empty_datasource_store: DatasourceStore, datasource_config: DatasourceConfig
) -> None:
    store: DatasourceStore = empty_datasource_store

    key = DataContextVariableKey(
        resource_name="my_datasource",
    )
    store.set(key=key, value=datasource_config)
    res: DatasourceConfig = store.get(key=key)

    assert isinstance(res, DatasourceConfig)
    set_config_serializer = DictConfigSerializer(schema=datasourceConfigSchema)
    retrieved_config_serializer = YAMLReadyDictDatasourceConfigSerializer(
        schema=datasourceConfigSchema
    )
    _assert_serialized_datasource_configs_are_equal(
        [datasource_config, res], [set_config_serializer, retrieved_config_serializer]
    )


@pytest.mark.integration
def test_datasource_store_retrieval_cloud_mode(
    datasource_config: DatasourceConfig,
    ge_cloud_base_url: str,
    ge_cloud_access_token: str,
    ge_cloud_organization_id: str,
    shared_called_with_request_kwargs: dict,
) -> None:
    ge_cloud_store_backend_config: dict = {
        "class_name": "GeCloudStoreBackend",
        "ge_cloud_base_url": ge_cloud_base_url,
        "ge_cloud_resource_type": GeCloudRESTResource.DATASOURCE,
        "ge_cloud_credentials": {
            "access_token": ge_cloud_access_token,
            "organization_id": ge_cloud_organization_id,
        },
        "suppress_store_backend_id": True,
    }

    store = DatasourceStore(
        store_name="my_cloud_datasource_store",
        store_backend=ge_cloud_store_backend_config,
        serializer=JsonConfigSerializer(schema=datasourceConfigSchema),
    )

    key = GeCloudIdentifier(
        resource_type=GeCloudRESTResource.DATASOURCE, ge_cloud_id="foobarbaz"
    )

    with patch("requests.put", autospec=True) as mock_put:
        type(mock_put.return_value).status_code = PropertyMock(return_value=200)

        store.set(key=key, value=datasource_config)

        expected_datasource_config = datasourceConfigSchema.dump(datasource_config)

        mock_put.assert_called_with(
            "https://app.test.greatexpectations.io/organizations/bd20fead-2c31-4392-bcd1-f1e87ad5a79c/datasources/foobarbaz",
            json={
                "data": {
                    "type": "datasource",
                    "id": "foobarbaz",
                    "attributes": {
                        "datasource_config": expected_datasource_config,
                        "organization_id": ge_cloud_organization_id,
                    },
                }
            },
            **shared_called_with_request_kwargs,
        )


@pytest.mark.integration
def test_datasource_store_with_inline_store_backend(
    datasource_config: DatasourceConfig, empty_data_context: DataContext
) -> None:
    inline_store_backend_config: dict = {
        "class_name": "InlineStoreBackend",
        "resource_type": DataContextVariableSchema.DATASOURCES,
        "data_context": empty_data_context,
        "suppress_store_backend_id": True,
    }

    store = DatasourceStore(
        store_name="my_datasource_store",
        store_backend=inline_store_backend_config,
        serializer=YAMLReadyDictDatasourceConfigSerializer(
            schema=datasourceConfigSchema
        ),
    )

    key = DataContextVariableKey(
        resource_name="my_datasource",
    )

    store.set(key=key, value=datasource_config)
    res: DatasourceConfig = store.get(key=key)

    assert isinstance(res, DatasourceConfig)
    set_config_serializer = DictConfigSerializer(schema=datasourceConfigSchema)
    retrieved_config_serializer = YAMLReadyDictDatasourceConfigSerializer(
        schema=datasourceConfigSchema
    )
    _assert_serialized_datasource_configs_are_equal(
        [datasource_config, res], [set_config_serializer, retrieved_config_serializer]
    )


@pytest.mark.unit
def test_datasource_store_set_by_name(
    empty_datasource_store: DatasourceStore,
    datasource_config: DatasourceConfig,
    datasource_name: str,
) -> None:
    assert len(empty_datasource_store.list_keys()) == 0

    empty_datasource_store.set_by_name(
        datasource_name=datasource_name, datasource_config=datasource_config
    )

    assert len(empty_datasource_store.list_keys()) == 1


@pytest.mark.integration
def test_datasource_store_retrieve_by_name(
    datasource_name: str,
    datasource_config: DatasourceConfig,
    datasource_store_with_single_datasource: DatasourceStore,
) -> None:
    actual_config: DatasourceConfig = (
        datasource_store_with_single_datasource.retrieve_by_name(
            datasource_name=datasource_name
        )
    )
    set_config_serializer = DictConfigSerializer(schema=datasourceConfigSchema)
    retrieved_config_serializer = YAMLReadyDictDatasourceConfigSerializer(
        schema=datasourceConfigSchema
    )
    _assert_serialized_datasource_configs_are_equal(
        [datasource_config, actual_config],
        [set_config_serializer, retrieved_config_serializer],
    )


@pytest.mark.unit
def test_datasource_store_delete_by_name(
    datasource_name: str,
    datasource_store_with_single_datasource: DatasourceStore,
) -> None:
    assert len(datasource_store_with_single_datasource.list_keys()) == 1

    datasource_store_with_single_datasource.delete_by_name(
        datasource_name=datasource_name
    )

    assert len(datasource_store_with_single_datasource.list_keys()) == 0


@pytest.mark.integration
def test_datasource_store_update_by_name(
    datasource_name: str,
    datasource_config: DatasourceConfig,
    datasource_store_with_single_datasource: DatasourceStore,
) -> None:
    updated_base_directory: str = "foo/bar/baz"

    updated_datasource_config = copy.deepcopy(datasource_config)
    updated_datasource_config.data_connectors["tripdata_monthly_configured"][
        "base_directory"
    ] = updated_base_directory

    datasource_store_with_single_datasource.update_by_name(
        datasource_name=datasource_name, datasource_config=updated_datasource_config
    )

    key = DataContextVariableKey(
        resource_name=datasource_name,
    )
    actual_config = cast(
        DatasourceConfig, datasource_store_with_single_datasource.get(key=key)
    )

    set_config_serializer = DictConfigSerializer(schema=datasourceConfigSchema)
    retrieved_config_serializer = YAMLReadyDictDatasourceConfigSerializer(
        schema=datasourceConfigSchema
    )
    _assert_serialized_datasource_configs_are_equal(
        [updated_datasource_config, actual_config],
        [set_config_serializer, retrieved_config_serializer],
    )


@pytest.mark.unit
def test_datasource_store_update_raises_error_if_datasource_doesnt_exist(
    datasource_name: str,
    empty_datasource_store: DatasourceStore,
) -> None:
    updated_datasource_config = DatasourceConfig()
    with pytest.raises(ValueError) as e:
        empty_datasource_store.update_by_name(
            datasource_name=datasource_name, datasource_config=updated_datasource_config
        )

    assert f"Unable to load datasource `{datasource_name}`" in str(e.value)


@pytest.mark.integration
def test_datasource_store_with_inline_store_backend_config_with_names_does_not_store_datasource_name(
    datasource_config_with_names: DatasourceConfig,
    datasource_config: DatasourceConfig,
    empty_data_context: DataContext,
) -> None:
    inline_store_backend_config: dict = {
        "class_name": "InlineStoreBackend",
        "resource_type": DataContextVariableSchema.DATASOURCES,
        "data_context": empty_data_context,
        "suppress_store_backend_id": True,
    }

    store = DatasourceStore(
        store_name="my_datasource_store",
        store_backend=inline_store_backend_config,
        serializer=YAMLReadyDictDatasourceConfigSerializer(
            schema=datasourceConfigSchema
        ),
    )

    key = DataContextVariableKey(
        resource_name="my_datasource",
    )

    store.set(key=key, value=datasource_config_with_names)
    res: DatasourceConfig = store.get(key=key)

    assert isinstance(res, DatasourceConfig)
    set_config_serializer = DictConfigSerializer(schema=datasourceConfigSchema)
    retrieved_config_serializer = YAMLReadyDictDatasourceConfigSerializer(
        schema=datasourceConfigSchema
    )
    _assert_serialized_datasource_configs_are_equal(
        [datasource_config, res], [set_config_serializer, retrieved_config_serializer]
    )

    with open(
        pathlib.Path(empty_data_context.root_directory) / "great_expectations.yml"
    ) as f:
        context_config_from_disk: dict = yaml.load(f)

    assert "name" not in context_config_from_disk["datasources"]["my_datasource"]


@pytest.mark.integration
def test_datasource_store_with_inline_store_backend_config_with_names_does_not_store_dataconnector_name(
    datasource_config_with_names: DatasourceConfig,
    datasource_config: DatasourceConfig,
    empty_data_context: DataContext,
) -> None:
    inline_store_backend_config: dict = {
        "class_name": "InlineStoreBackend",
        "resource_type": DataContextVariableSchema.DATASOURCES,
        "data_context": empty_data_context,
        "suppress_store_backend_id": True,
    }

    store = DatasourceStore(
        store_name="my_datasource_store",
        store_backend=inline_store_backend_config,
        serializer=YAMLReadyDictDatasourceConfigSerializer(
            schema=datasourceConfigSchema
        ),
    )

    key = DataContextVariableKey(
        resource_name="my_datasource",
    )

    store.set(key=key, value=datasource_config_with_names)
    res: DatasourceConfig = store.get(key=key)

    assert isinstance(res, DatasourceConfig)
    set_config_serializer = DictConfigSerializer(schema=datasourceConfigSchema)
    retrieved_config_serializer = YAMLReadyDictDatasourceConfigSerializer(
        schema=datasourceConfigSchema
    )
    _assert_serialized_datasource_configs_are_equal(
        [datasource_config, res], [set_config_serializer, retrieved_config_serializer]
    )

    with open(
        pathlib.Path(empty_data_context.root_directory) / "great_expectations.yml"
    ) as f:
        context_config_from_disk: dict = yaml.load(f)

    assert (
        "name"
        not in context_config_from_disk["datasources"]["my_datasource"][
            "data_connectors"
        ]["tripdata_monthly_configured"]
    )
