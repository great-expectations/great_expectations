from __future__ import annotations

from typing import Mapping

import pytest

import great_expectations.exceptions as gx_exceptions
from great_expectations import project_manager, set_context
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.serializer import DictConfigSerializer
from great_expectations.data_context.data_context.ephemeral_data_context import (
    EphemeralDataContext,
)
from great_expectations.data_context.store import ExpectationsStore
from great_expectations.data_context.store.checkpoint_store import CheckpointStore
from great_expectations.data_context.store.datasource_store import DatasourceStore
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
    ProgressBarsConfig,
    datasourceConfigSchema,
)
from great_expectations.datasource.fluent.sources import _SourceFactories
from great_expectations.exceptions.exceptions import StoreConfigurationError
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)


class DatasourceStoreSpy(DatasourceStore):
    def __init__(self) -> None:
        self.save_count = 0
        super().__init__(serializer=DictConfigSerializer(schema=datasourceConfigSchema))

    def set(self, key, value, **kwargs):
        ret = super().set(key=key, value=value, **kwargs)
        self.save_count += 1
        return ret


class ExpectationsStoreSpy(ExpectationsStore):
    def __init__(self) -> None:
        self.save_count = 0
        super().__init__()

    def add(self, key, value, **kwargs):
        ret = super().add(key=key, value=value, **kwargs)
        self.save_count += 1
        return ret

    def update(self, key, value, **kwargs):
        ret = super().update(key=key, value=value, **kwargs)
        self.save_count += 1
        return ret

    def add_or_update(self, key, value, **kwargs):
        ret = super().add_or_update(key=key, value=value, **kwargs)
        self.save_count += 1
        return ret


class CheckpointStoreSpy(CheckpointStore):
    STORE_NAME = "checkpoint_store"

    def __init__(self) -> None:
        self.save_count = 0
        super().__init__(store_name=CheckpointStoreSpy.STORE_NAME)

    def add(self, key, value, **kwargs):
        ret = super().add(key=key, value=value, **kwargs)
        self.save_count += 1
        return ret

    def update(self, key, value, **kwargs):
        ret = super().update(key=key, value=value, **kwargs)
        self.save_count += 1
        return ret

    def add_or_update(self, key, value, **kwargs):
        ret = super().add_or_update(key=key, value=value, **kwargs)
        self.save_count += 1
        return ret


class EphemeralDataContextSpy(EphemeralDataContext):
    """
    Simply wraps around EphemeralDataContext but keeps tabs on specific method calls around state management.
    """  # noqa: E501

    def __init__(
        self,
        project_config: DataContextConfig,
    ) -> None:
        # expectation store is required for initializing the base DataContext
        self._expectations_store = ExpectationsStoreSpy()
        self._checkpoint_store = CheckpointStoreSpy()
        super().__init__(project_config)
        self.save_count = 0
        self._datasource_store = DatasourceStoreSpy()

    @property
    def datasource_store(self):
        return self._datasource_store

    @property
    def expectations_store(self):
        return self._expectations_store

    @property
    def checkpoint_store(self):
        return self._checkpoint_store

    def _save_project_config(self):
        """
        No-op our persistence mechanism but increment an internal counter to ensure it was used.
        """
        self.save_count += 1


BLOCK_CONFIG_DATASOURCE_NAME = "my_pandas_datasource"


@pytest.fixture
def in_memory_data_context(
    fluent_datasource_config: dict,
) -> EphemeralDataContextSpy:
    config = DataContextConfig(
        store_backend_defaults=InMemoryStoreBackendDefaults(),
    )
    context = EphemeralDataContextSpy(project_config=config)
    ds_type = _SourceFactories.type_lookup[fluent_datasource_config["type"]]
    fluent_datasources = {
        fluent_datasource_config["name"]: ds_type(**fluent_datasource_config),
    }
    context.datasources.update(fluent_datasources)
    set_context(context)
    return context


@pytest.mark.unit
def test_add_store(in_memory_data_context: EphemeralDataContextSpy):
    context = in_memory_data_context

    num_stores_before = len(context.stores)
    num_store_configs_before = len(context.config.stores)

    context.add_store(
        store_name="my_new_store",
        store_config={
            "module_name": "great_expectations.data_context.store",
            "class_name": "ExpectationsStore",
        },
    )

    num_stores_after = len(context.stores)
    num_store_configs_after = len(context.config.stores)

    assert num_stores_after == num_stores_before + 1
    assert num_store_configs_after == num_store_configs_before + 1
    assert context.save_count == 1


@pytest.mark.unit
def test_delete_store_success(in_memory_data_context: EphemeralDataContextSpy):
    context = in_memory_data_context

    num_stores_before = len(context.stores)
    num_store_configs_before = len(context.config.stores)

    context.delete_store("checkpoint_store")  # We know this to be a default name

    num_stores_after = len(context.stores)
    num_store_configs_after = len(context.config.stores)

    assert num_stores_after == num_stores_before - 1
    assert num_store_configs_after == num_store_configs_before - 1
    assert context.save_count == 1


@pytest.mark.unit
def test_delete_store_failure(in_memory_data_context: EphemeralDataContextSpy):
    context = in_memory_data_context

    num_stores_before = len(context.stores)
    num_store_configs_before = len(context.config.stores)

    with pytest.raises(StoreConfigurationError):
        context.delete_store("my_fake_store_name")

    num_stores_after = len(context.stores)
    num_store_configs_after = len(context.config.stores)

    assert num_stores_after == num_stores_before
    assert num_store_configs_after == num_store_configs_before
    assert context.save_count == 0


@pytest.mark.unit
@pytest.mark.parametrize(
    "config",
    [
        pytest.param(
            DataContextConfig(progress_bars=ProgressBarsConfig(globally=True)),
            id="DataContextConfig",
        ),
        pytest.param({"progress_bars": ProgressBarsConfig(globally=True)}, id="Mapping"),
    ],
)
def test_update_project_config(
    in_memory_data_context: EphemeralDataContextSpy, config: DataContextConfig | Mapping
):
    context = in_memory_data_context

    assert context.progress_bars is None

    context.update_project_config(config)

    assert context.progress_bars["globally"] is True


@pytest.mark.unit
def test_add_expectation_suite_success(
    in_memory_data_context: EphemeralDataContextSpy,
):
    context = in_memory_data_context
    project_manager.set_project(context)
    kwargs = {
        "expectations": [
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={"column": "x", "value_set": [1, 2, 4]},
            ),
        ],
        "expectation_suite_name": "default",
        "meta": {"great_expectations_version": "0.15.44"},
    }
    expected_suite = ExpectationSuite(
        expectations=[
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={"column": "x", "value_set": [1, 2, 4]},
            ),
        ],
        name="default",
        meta={"great_expectations_version": "0.15.44"},
    )

    suite = context.add_expectation_suite(**kwargs)

    assert suite == expected_suite
    assert context.expectations_store.save_count == 1


@pytest.mark.unit
def test_add_expectation_suite_namespace_collision_failure(
    in_memory_data_context: EphemeralDataContextSpy,
):
    context = in_memory_data_context

    suite_name = "default"
    context.add_expectation_suite(expectation_suite_name=suite_name)

    with pytest.raises(gx_exceptions.DataContextError) as e:
        context.add_expectation_suite(expectation_suite_name=suite_name)

    assert f"An ExpectationSuite named {suite_name} already exists" in str(e.value)
    assert context.expectations_store.save_count == 1


@pytest.mark.unit
@pytest.mark.parametrize(
    "use_suite,suite_name",
    [
        pytest.param(
            True,
            "default",
            id="both suite and suite_name",
        ),
        pytest.param(False, None, id="neither suite nor suite_name"),
    ],
)
def test_add_expectation_suite_conflicting_args_failure(
    in_memory_data_context: EphemeralDataContextSpy,
    use_suite: bool,
    suite_name: str | None,
):
    context = in_memory_data_context
    project_manager.set_project(context)
    if use_suite:
        suite = ExpectationSuite(name="default")
    else:
        suite = None

    with pytest.raises(TypeError):
        context.add_expectation_suite(expectation_suite=suite, expectation_suite_name=suite_name)

    assert context.expectations_store.save_count == 0


@pytest.mark.unit
def test_update_expectation_suite_failure(
    in_memory_data_context: EphemeralDataContextSpy,
):
    context = in_memory_data_context

    suite_name = "my_brand_new_suite"
    suite = ExpectationSuite(name=suite_name)

    with pytest.raises(gx_exceptions.ExpectationSuiteError) as e:
        _ = context.update_expectation_suite(suite)

    assert f"Could not find an existing ExpectationSuite named {suite_name}." in str(e.value)


@pytest.mark.unit
@pytest.mark.parametrize(
    "kwargs_index",
    [
        pytest.param(
            0,
            id="individual args",
        ),
        pytest.param(
            1,
            id="existing suite",
        ),
    ],
)
def test_add_or_update_expectation_suite_adds_successfully(
    in_memory_data_context: EphemeralDataContextSpy,
    kwargs_index: int,
):
    project_manager.set_project(in_memory_data_context)

    kwargs_lookup = [
        # can't instantiate Suite in parameters since it requires a data context
        {
            "expectation_suite_name": "default",
            "expectations": [
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_in_set",
                    kwargs={"column": "x", "value_set": [1, 2, 4]},
                ),
            ],
            "meta": {"great_expectations_version": "0.15.44"},
        },
        {
            "expectation_suite": ExpectationSuite(
                expectations=[
                    ExpectationConfiguration(
                        expectation_type="expect_column_values_to_be_in_set",
                        kwargs={"column": "x", "value_set": [1, 2, 4]},
                    ),
                ],
                name="default",
                meta={"great_expectations_version": "0.15.44"},
            ),
        },
    ]
    kwargs = kwargs_lookup[kwargs_index]
    context = in_memory_data_context

    expectation_suite_name = "default"
    expectations = [
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={"column": "x", "value_set": [1, 2, 4]},
        ),
    ]
    meta = {"great_expectations_version": "0.15.44"}

    suite = context.add_or_update_expectation_suite(**kwargs)

    assert suite.name == expectation_suite_name
    assert suite.expectation_configurations == expectations
    assert suite.meta == meta
    assert context.expectations_store.save_count == 1


@pytest.mark.unit
@pytest.mark.parametrize(
    "new_expectations",
    [
        pytest.param(
            [],
            id="empty expectations",
        ),
        pytest.param(
            [
                ExpectationConfiguration(
                    expectation_type="expect_column_to_exist",
                    kwargs={"column": "pickup_datetime"},
                )
            ],
            id="new expectations",
        ),
    ],
)
def test_add_or_update_expectation_suite_updates_successfully(
    in_memory_data_context: EphemeralDataContextSpy,
    new_expectations: list[ExpectationConfiguration],
):
    context = in_memory_data_context
    project_manager.set_project(context)

    suite_name = "default"
    suite = context.add_expectation_suite(
        expectation_suite_name=suite_name,
        expectations=[
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={"column": "x", "value_set": [1, 2, 4]},
            ),
        ],
    )

    assert context.expectations_store.save_count == 1

    suite = context.add_or_update_expectation_suite(
        expectation_suite_name=suite_name, expectations=new_expectations
    )

    assert suite.expectation_configurations == new_expectations
    assert context.expectations_store.save_count == 2


@pytest.mark.unit
@pytest.mark.parametrize(
    "use_suite,suite_name",
    [
        pytest.param(
            True,
            "default",
            id="both suite and suite_name",
        ),
        pytest.param(False, None, id="neither suite nor suite_name"),
    ],
)
def test_add_or_update_expectation_suite_conflicting_args_failure(
    in_memory_data_context: EphemeralDataContextSpy,
    use_suite: ExpectationSuite | None,
    suite_name: str | None,
):
    project_manager.set_project(in_memory_data_context)

    if use_suite:
        suite = ExpectationSuite(name="default")
    else:
        suite = None
    context = in_memory_data_context

    with pytest.raises((TypeError, AssertionError)):
        context.add_or_update_expectation_suite(
            expectation_suite=suite, expectation_suite_name=suite_name
        )

    assert context.expectations_store.save_count == 0
