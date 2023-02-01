from __future__ import annotations

from typing import Mapping

import pytest

import great_expectations.exceptions as gx_exceptions
from great_expectations.checkpoint.checkpoint import Checkpoint
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.data_context.data_context.ephemeral_data_context import (
    EphemeralDataContext,
)
from great_expectations.data_context.store import ExpectationsStore, ProfilerStore
from great_expectations.data_context.store.checkpoint_store import CheckpointStore
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DatasourceConfig,
    InMemoryStoreBackendDefaults,
    ProgressBarsConfig,
)
from great_expectations.exceptions.exceptions import StoreConfigurationError
from great_expectations.rule_based_profiler.rule_based_profiler import RuleBasedProfiler


class ExpectationsStoreSpy(ExpectationsStore):
    def __init__(self) -> None:
        self.save_count = 0
        super().__init__()

    def set(self, key, value, **kwargs):
        self.save_count += 1
        return super().set(key=key, value=value, **kwargs)


class ProfilerStoreSpy(ProfilerStore):

    STORE_NAME = "profiler_store"

    def __init__(self) -> None:
        self.save_count = 0
        super().__init__(ProfilerStoreSpy.STORE_NAME)

    def set(self, key, value, **kwargs):
        self.save_count += 1
        return super().set(key=key, value=value, **kwargs)


class CheckpointStoreSpy(CheckpointStore):

    STORE_NAME = "checkpoint_store"

    def __init__(self) -> None:
        self.save_count = 0
        super().__init__(CheckpointStoreSpy.STORE_NAME)

    def set(self, key, value, **kwargs):
        self.save_count += 1
        return super().set(key=key, value=value, **kwargs)


class EphemeralDataContextSpy(EphemeralDataContext):
    """
    Simply wraps around EphemeralDataContext but keeps tabs on specific method calls around state management.
    """

    def __init__(
        self,
        project_config: DataContextConfig,
    ) -> None:
        super().__init__(project_config)
        self.save_count = 0
        self._expectations_store = ExpectationsStoreSpy()
        self._profiler_store = ProfilerStoreSpy()
        self._checkpoint_store = CheckpointStoreSpy()

    @property
    def expectations_store(self):
        return self._expectations_store

    @property
    def profiler_store(self):
        return self._profiler_store

    @property
    def checkpoint_store(self):
        return self._checkpoint_store

    def _save_project_config(self):
        """
        No-op our persistence mechanism but increment an internal counter to ensure it was used.
        """
        self.save_count += 1


@pytest.fixture
def datasource_name() -> str:
    return "my_pandas_datasource"


@pytest.fixture
def in_memory_data_context(
    datasource_name: str,
    datasource_config: DatasourceConfig,
) -> EphemeralDataContextSpy:
    datasources = {
        datasource_name: datasource_config,
    }
    config = DataContextConfig(
        datasources=datasources, store_backend_defaults=InMemoryStoreBackendDefaults()
    )
    context = EphemeralDataContextSpy(project_config=config)
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
        pytest.param(
            {"progress_bars": ProgressBarsConfig(globally=True)}, id="Mapping"
        ),
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
@pytest.mark.parametrize(
    "kwargs,expected_id",
    [
        pytest.param(
            {},
            None,
            id="no kwargs",
        ),
        pytest.param(
            {
                "id": "d53c2384-f973-4a0c-9c85-af1d67c06f58",
            },
            "d53c2384-f973-4a0c-9c85-af1d67c06f58",
            id="kwargs",
        ),
    ],
)
def test_add_or_update_datasource_updates_successfully(
    in_memory_data_context: EphemeralDataContextSpy,
    datasource_name: str,
    kwargs: dict,
    expected_id: str | None,
):
    context = in_memory_data_context

    num_datasource_before = len(context.datasources)
    num_datasource_configs_before = len(context.config.datasources)

    assert (
        datasource_name in context.datasources
    ), f"Downstream logic in the test relies on {datasource_name} being a datasource; please check your fixtures."

    datasource = context.add_or_update_datasource(name=datasource_name, **kwargs)
    # Let's `id` as an example attr to change so we don't need to assert against the whole config
    assert datasource.config["id"] == expected_id

    num_datasource_after = len(context.datasources)
    num_datasource_configs_after = len(context.config.datasources)

    assert num_datasource_after == num_datasource_before
    assert num_datasource_configs_after == num_datasource_configs_before
    assert context.save_count == 1


@pytest.mark.unit
def test_add_or_update_datasource_adds_successfully(
    in_memory_data_context: EphemeralDataContextSpy,
):
    context = in_memory_data_context

    num_datasource_before = len(context.datasources)
    num_datasource_configs_before = len(context.config.datasources)

    datasource_name = "my_brand_new_datasource"

    assert datasource_name not in context.datasources

    _ = context.add_or_update_datasource(
        name=datasource_name,
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
    )

    num_datasource_after = len(context.datasources)
    num_datasource_configs_after = len(context.config.datasources)

    assert datasource_name in context.datasources
    assert num_datasource_after == num_datasource_before + 1
    assert num_datasource_configs_after == num_datasource_configs_before + 1
    assert context.save_count == 1


@pytest.mark.unit
@pytest.mark.parametrize(
    "kwargs,expected_suite",
    [
        pytest.param(
            {"expectation_suite_name": "my_new_suite"},
            ExpectationSuite(expectation_suite_name="my_new_suite"),
            id="only name",
        ),
        pytest.param(
            {
                "expectations": [
                    ExpectationConfiguration(
                        expectation_type="expect_column_values_to_be_in_set",
                        kwargs={"column": "x", "value_set": [1, 2, 4]},
                    ),
                ],
                "expectation_suite_name": "default",
                "meta": {"great_expectations_version": "0.15.44"},
            },
            ExpectationSuite(
                expectations=[
                    ExpectationConfiguration(
                        expectation_type="expect_column_values_to_be_in_set",
                        kwargs={"column": "x", "value_set": [1, 2, 4]},
                    ),
                ],
                expectation_suite_name="default",
                meta={"great_expectations_version": "0.15.44"},
            ),
            id="misc args",
        ),
    ],
)
def test_add_expectation_suite_success(
    in_memory_data_context: EphemeralDataContextSpy,
    kwargs: dict,
    expected_suite: ExpectationSuite,
):
    context = in_memory_data_context

    suite = context.add_expectation_suite(**kwargs)

    assert suite == expected_suite
    assert context.expectations_store.save_count == 1


@pytest.mark.unit
def test_add_expectation_suite_failure(
    in_memory_data_context: EphemeralDataContextSpy,
):
    context = in_memory_data_context

    suite_name = "default"
    context.add_expectation_suite(expectation_suite_name=suite_name)

    with pytest.raises(gx_exceptions.DataContextError) as e:
        context.add_expectation_suite(expectation_suite_name=suite_name)

    assert f"expectation_suite with name {suite_name} already exists" in str(e.value)
    assert (
        "please delete or update it using `delete_expectation_suite` or `update_expectation_suite`"
        in str(e.value)
    )
    assert context.expectations_store.save_count == 1


@pytest.mark.unit
def test_update_expectation_suite_success(
    in_memory_data_context: EphemeralDataContextSpy,
):
    context = in_memory_data_context

    suite_name = "default"
    suite = context.add_expectation_suite(suite_name)

    assert context.expectations_store.save_count == 1

    suite.expectations = [
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={"column": "x", "value_set": [1, 2, 4]},
        ),
    ]
    context.update_expectation_suite(suite)

    assert context.expectations_store.save_count == 2


@pytest.mark.unit
def test_update_expectation_suite_failure(
    in_memory_data_context: EphemeralDataContextSpy,
):
    context = in_memory_data_context

    suite_name = "my_brand_new_suite"
    suite = ExpectationSuite(expectation_suite_name=suite_name)

    with pytest.raises(gx_exceptions.DataContextError) as e:
        context.update_expectation_suite(suite)

    assert f"expectation_suite with name {suite_name} does not exist." in str(e.value)


@pytest.mark.unit
def test_add_or_update_expectation_suite_adds_successfully(
    in_memory_data_context: EphemeralDataContextSpy,
):
    context = in_memory_data_context

    expectation_suite_name = "default"
    expectations = [
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={"column": "x", "value_set": [1, 2, 4]},
        ),
    ]
    meta = {"great_expectations_version": "0.15.44"}

    suite = context.add_or_update_expectation_suite(
        expectation_suite_name=expectation_suite_name,
        expectations=expectations,
        meta=meta,
    )

    assert suite.expectation_suite_name == expectation_suite_name
    assert suite.expectations == expectations
    assert suite.meta == meta
    assert context.expectations_store.save_count == 1


@pytest.mark.unit
def test_add_or_update_expectation_suite_updates_successfully(
    in_memory_data_context: EphemeralDataContextSpy,
):
    context = in_memory_data_context

    suite_name = "default"
    suite = context.add_expectation_suite(suite_name)

    assert context.expectations_store.save_count == 1

    suite.expectations = [
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={"column": "x", "value_set": [1, 2, 4]},
        ),
    ]
    _ = context.add_or_update_expectation_suite(expectation_suite_name=suite_name)

    assert context.expectations_store.save_count == 2


@pytest.mark.unit
def test_update_profiler_success(
    in_memory_data_context: EphemeralDataContextSpy,
    profiler_rules: dict,
):
    context = in_memory_data_context

    name = "my_rbp"
    config_version = 1.0
    rules = profiler_rules

    profiler = context.add_profiler(
        name=name, config_version=config_version, rules=rules
    )

    assert context.profiler_store.save_count == 1

    profiler.rules = {}
    context.update_profiler(profiler)

    assert context.profiler_store.save_count == 2


@pytest.mark.unit
def test_update_profiler_failure(in_memory_data_context: EphemeralDataContextSpy):
    context = in_memory_data_context

    name = "my_rbp"
    profiler = RuleBasedProfiler(
        name=name,
        config_version=1.0,
        data_context=context,
    )

    with pytest.raises(gx_exceptions.ProfilerNotFoundError) as e:
        context.update_profiler(profiler)

    assert f"Non-existent Profiler configuration named {name}" in str(e.value)


@pytest.mark.unit
def test_add_or_update_profiler_adds_successfully(
    in_memory_data_context: EphemeralDataContextSpy, profiler_rules: dict
):
    context = in_memory_data_context

    name = "my_rbp"
    config_version = 1.0
    rules = profiler_rules

    profiler = context.add_or_update_profiler(
        name=name, config_version=config_version, rules=rules
    )

    config = profiler.config

    assert config.name == name
    assert config.config_version == config_version
    assert len(config.rules) == len(rules) and config.rules.keys() == rules.keys()
    assert context.profiler_store.save_count == 1


@pytest.mark.unit
def test_add_or_update_profiler_updates_successfully(
    in_memory_data_context: EphemeralDataContextSpy,
    profiler_rules: dict,
):
    context = in_memory_data_context

    name = "my_rbp"
    config_version = 1.0
    rules = profiler_rules

    profiler = context.add_profiler(
        name=name, config_version=config_version, rules=rules
    )

    assert context.profiler_store.save_count == 1

    profiler.rules = {}
    _ = context.add_or_update_profiler(name=name)

    assert context.profiler_store.save_count == 2


@pytest.mark.unit
def test_update_checkpoint_success(
    in_memory_data_context: EphemeralDataContextSpy,
    checkpoint_config: dict,
):
    context = in_memory_data_context
    name = "my_checkpoint"
    checkpoint_config["name"] = name

    checkpoint = context.add_checkpoint(**checkpoint_config)

    assert context.checkpoint_store.save_count == 1

    context.update_checkpoint(checkpoint)

    assert context.checkpoint_store.save_count == 2


@pytest.mark.unit
def test_update_checkpoint_failure(in_memory_data_context: EphemeralDataContextSpy):
    context = in_memory_data_context

    name = "my_checkpoint"
    checkpoint = Checkpoint(
        name=name,
        data_context=context,
    )

    with pytest.raises(gx_exceptions.CheckpointNotFoundError) as e:
        context.update_checkpoint(checkpoint)

    assert f"Could not find a Checkpoint named {name}" in str(e.value)
