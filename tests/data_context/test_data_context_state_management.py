from __future__ import annotations

from typing import Mapping
from unittest import mock

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
    CheckpointConfig,
    DataContextConfig,
    DatasourceConfig,
    InMemoryStoreBackendDefaults,
    ProgressBarsConfig,
)
from great_expectations.datasource.new_datasource import Datasource
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
def test_add_datasource_with_existing_datasource(
    in_memory_data_context: EphemeralDataContextSpy,
    datasource_config: DatasourceConfig,
):
    context = in_memory_data_context

    config_dict = datasource_config.to_dict()
    for attr in ("class_name", "module_name"):
        config_dict.pop(attr)
    config_dict["name"] = "my_datasource"

    datasource = Datasource(**config_dict)
    persisted_datasource = context.add_datasource(datasource=datasource)

    expected_config = datasource.config
    actual_config = persisted_datasource.config

    # Name gets injected into data connector as part of serialization hook
    # Removing for purposes of test assertion
    data_connectors = actual_config["data_connectors"]
    data_connector_name = tuple(data_connectors.keys())[0]
    data_connectors[data_connector_name].pop("name")

    assert actual_config == expected_config
    assert context.save_count == 1


@pytest.mark.unit
@pytest.mark.parametrize(
    "datasource,datasource_name",
    [
        pytest.param(
            mock.MagicMock(),
            "my_datasource",
            id="both datasource and name",
        ),
        pytest.param(None, None, id="neither datasource nor name"),
    ],
)
def test_add_datasource_conflicting_args_failure(
    in_memory_data_context: EphemeralDataContextSpy,
    datasource: mock.MagicMock | None,
    datasource_name: str | None,
):
    context = in_memory_data_context

    with pytest.raises(ValueError) as e:
        context.add_datasource(datasource=datasource, name=datasource_name)

    assert (
        "an existing datasource or individual constructor arguments (but not both)"
        in str(e.value)
    )
    assert context.save_count == 0


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
@pytest.mark.parametrize("use_existing_datasource", [True, False])
def test_add_or_update_datasource_adds_successfully(
    in_memory_data_context: EphemeralDataContextSpy,
    datasource_config: DatasourceConfig,
    use_existing_datasource: bool,
):
    context = in_memory_data_context

    num_datasource_before = len(context.datasources)
    num_datasource_configs_before = len(context.config.datasources)

    datasource_name = "my_brand_new_datasource"

    assert datasource_name not in context.datasources

    config_dict = datasource_config.to_dict()
    config_dict["name"] = datasource_name

    if use_existing_datasource:
        for attr in ("class_name", "module_name"):
            config_dict.pop(attr)
        datasource = Datasource(**config_dict)
        _ = context.add_or_update_datasource(datasource=datasource)
    else:
        _ = context.add_or_update_datasource(**config_dict)

    num_datasource_after = len(context.datasources)
    num_datasource_configs_after = len(context.config.datasources)

    assert datasource_name in context.datasources
    assert num_datasource_after == num_datasource_before + 1
    assert num_datasource_configs_after == num_datasource_configs_before + 1
    assert context.save_count == 1


@pytest.mark.unit
@pytest.mark.parametrize(
    "datasource,datasource_name",
    [
        pytest.param(
            mock.MagicMock(),
            "my_datasource",
            id="both datasource and name",
        ),
        pytest.param(None, None, id="neither datasource nor name"),
    ],
)
def test_add_or_update_datasource_conflicting_args_failure(
    in_memory_data_context: EphemeralDataContextSpy,
    datasource: mock.MagicMock | None,
    datasource_name: str | None,
):
    context = in_memory_data_context

    with pytest.raises(ValueError) as e:
        context.add_or_update_datasource(datasource=datasource, name=datasource_name)

    assert (
        "an existing datasource or individual constructor arguments (but not both)"
        in str(e.value)
    )
    assert context.expectations_store.save_count == 0


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
        pytest.param(
            {
                "expectation_suite": ExpectationSuite(
                    expectations=[
                        ExpectationConfiguration(
                            expectation_type="expect_column_values_to_be_in_set",
                            kwargs={"column": "x", "value_set": [1, 2, 4]},
                        ),
                    ],
                    expectation_suite_name="default",
                    meta={"great_expectations_version": "0.15.44"},
                ),
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
            id="existing suite",
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
def test_add_expectation_suite_namespace_collision_failure(
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
@pytest.mark.parametrize(
    "suite,suite_name",
    [
        pytest.param(
            ExpectationSuite(expectation_suite_name="default"),
            "default",
            id="both suite and suite_name",
        ),
        pytest.param(None, None, id="neither suite nor suite_name"),
    ],
)
def test_add_expectation_suite_conflicting_args_failure(
    in_memory_data_context: EphemeralDataContextSpy,
    suite: ExpectationSuite | None,
    suite_name: str | None,
):
    context = in_memory_data_context

    with pytest.raises(ValueError) as e:
        context.add_expectation_suite(
            expectation_suite=suite, expectation_suite_name=suite_name
        )

    assert (
        "an existing expectation_suite or individual constructor arguments (but not both)"
        in str(e.value)
    )
    assert context.expectations_store.save_count == 0


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
    updated_suite = context.update_expectation_suite(suite)

    assert updated_suite.expectations == suite.expectations
    assert context.expectations_store.save_count == 2


@pytest.mark.unit
def test_update_expectation_suite_failure(
    in_memory_data_context: EphemeralDataContextSpy,
):
    context = in_memory_data_context

    suite_name = "my_brand_new_suite"
    suite = ExpectationSuite(expectation_suite_name=suite_name)

    with pytest.raises(gx_exceptions.DataContextError) as e:
        _ = context.update_expectation_suite(suite)

    assert f"expectation_suite with name {suite_name} does not exist." in str(e.value)


@pytest.mark.unit
@pytest.mark.parametrize(
    "kwargs",
    [
        pytest.param(
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
            id="individual args",
        ),
        pytest.param(
            {
                "expectation_suite": ExpectationSuite(
                    expectations=[
                        ExpectationConfiguration(
                            expectation_type="expect_column_values_to_be_in_set",
                            kwargs={"column": "x", "value_set": [1, 2, 4]},
                        ),
                    ],
                    expectation_suite_name="default",
                    meta={"great_expectations_version": "0.15.44"},
                ),
            },
            id="existing suite",
        ),
    ],
)
def test_add_or_update_expectation_suite_adds_successfully(
    in_memory_data_context: EphemeralDataContextSpy,
    kwargs: dict,
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

    suite = context.add_or_update_expectation_suite(**kwargs)

    assert suite.expectation_suite_name == expectation_suite_name
    assert suite.expectations == expectations
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

    assert suite.expectations == new_expectations
    assert context.expectations_store.save_count == 2


@pytest.mark.unit
@pytest.mark.parametrize(
    "suite,suite_name",
    [
        pytest.param(
            ExpectationSuite(expectation_suite_name="default"),
            "default",
            id="both suite and suite_name",
        ),
        pytest.param(None, None, id="neither suite nor suite_name"),
    ],
)
def test_add_or_update_expectation_suite_conflicting_args_failure(
    in_memory_data_context: EphemeralDataContextSpy,
    suite: ExpectationSuite | None,
    suite_name: str | None,
):
    context = in_memory_data_context

    with pytest.raises(ValueError) as e:
        context.add_or_update_expectation_suite(
            expectation_suite=suite, expectation_suite_name=suite_name
        )

    assert (
        "an existing expectation_suite or individual constructor arguments (but not both)"
        in str(e.value)
    )
    assert context.expectations_store.save_count == 0


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

    profiler.rules = []
    updated_profiler = context.update_profiler(profiler)

    assert updated_profiler.rules == profiler.rules
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
        _ = context.update_profiler(profiler)

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
@pytest.mark.parametrize(
    "new_rules",
    [
        pytest.param(
            {},
            id="empty rules",
        ),
        pytest.param(
            {
                "my_new_rule": {
                    "variables": {},
                    "domain_builder": {
                        "class_name": "TableDomainBuilder",
                    },
                    "parameter_builders": [
                        {
                            "class_name": "MetricMultiBatchParameterBuilder",
                            "name": "my_parameter",
                            "metric_name": "my_metric",
                        },
                    ],
                    "expectation_configuration_builders": [
                        {
                            "class_name": "DefaultExpectationConfigurationBuilder",
                            "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                            "column_A": "$domain.domain_kwargs.column_A",
                            "column_B": "$domain.domain_kwargs.column_B",
                        },
                    ],
                },
            },
            id="new rule",
        ),
    ],
)
def test_add_or_update_profiler_updates_successfully(
    in_memory_data_context: EphemeralDataContextSpy,
    profiler_rules: dict,
    new_rules: dict,
):
    context = in_memory_data_context

    name = "my_rbp"
    config_version = 1.0
    rules = profiler_rules

    profiler = context.add_profiler(
        name=name, config_version=config_version, rules=rules
    )

    assert context.profiler_store.save_count == 1

    profiler = context.add_or_update_profiler(name=name, rules=new_rules)

    # Rules get converted to a list within the RBP constructor
    assert sorted(rule.name for rule in profiler.rules) == sorted(new_rules.keys())
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

    action_list = [
        {
            "name": "store_validation_result",
            "action": {
                "class_name": "StoreValidationResultAction",
            },
        },
        {
            "name": "store_evaluation_params",
            "action": {
                "class_name": "StoreEvaluationParametersAction",
            },
        },
        {
            "name": "update_data_docs",
            "action": {
                "class_name": "UpdateDataDocsAction",
            },
        },
    ]
    checkpoint.config.action_list = action_list
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


@pytest.mark.unit
def test_add_or_update_checkpoint_adds_successfully(
    in_memory_data_context: EphemeralDataContextSpy,
    checkpoint_config: dict,
):
    context = in_memory_data_context
    name = "my_checkpoint"
    checkpoint_config["name"] = name

    checkpoint = context.add_or_update_checkpoint(**checkpoint_config)

    actual_config = checkpoint.config

    assert actual_config.name == name
    assert (
        actual_config.expectation_suite_name
        == checkpoint_config["expectation_suite_name"]
    )
    assert actual_config.validations == checkpoint_config["validations"]
    assert context.checkpoint_store.save_count == 1


@pytest.mark.unit
@pytest.mark.parametrize(
    "update_kwargs,expected_config",
    [
        pytest.param(
            {
                "action_list": [],
                "batch_request": {},
                "class_name": "Checkpoint",
                "config_version": 1.0,
                "evaluation_parameters": {},
                "expectation_suite_name": "oss_test_expectation_suite",
                "module_name": "great_expectations.checkpoint",
                "profilers": [],
                "runtime_configuration": {},
                "validations": [
                    {"expectation_suite_name": "taxi.demo_pass"},
                    {
                        "batch_request": {
                            "datasource_name": "oss_test_datasource",
                            "data_connector_name": "oss_test_data_connector",
                            "data_asset_name": "users",
                        }
                    },
                ],
            },
            CheckpointConfig(
                **{
                    "action_list": [],
                    "batch_request": {},
                    "class_name": "Checkpoint",
                    "config_version": 1.0,
                    "evaluation_parameters": {},
                    "expectation_suite_name": "oss_test_expectation_suite",
                    "module_name": "great_expectations.checkpoint",
                    "name": "my_checkpoint",
                    "profilers": [],
                    "runtime_configuration": {},
                    "validations": [
                        {"expectation_suite_name": "taxi.demo_pass"},
                        {
                            "batch_request": {
                                "datasource_name": "oss_test_datasource",
                                "data_connector_name": "oss_test_data_connector",
                                "data_asset_name": "users",
                            }
                        },
                    ],
                }
            ),
            id="no updates",
        ),
        pytest.param(
            {
                "class_name": "Checkpoint",
                "module_name": "great_expectations.checkpoint",
                "config_version": 1.0,
                "validations": [],
                "evaluation_parameters": {
                    "environment": "$GE_ENVIRONMENT",
                    "tolerance": 1.0e-2,
                    "aux_param_0": "$MY_PARAM",
                    "aux_param_1": "1 + $MY_PARAM",
                },
            },
            CheckpointConfig(
                **{
                    "action_list": [],
                    "batch_request": {},
                    "class_name": "Checkpoint",
                    "config_version": 1.0,
                    "evaluation_parameters": {
                        "environment": "$GE_ENVIRONMENT",
                        "tolerance": 1.0e-2,
                        "aux_param_0": "$MY_PARAM",
                        "aux_param_1": "1 + $MY_PARAM",
                    },
                    "expectation_suite_name": None,
                    "module_name": "great_expectations.checkpoint",
                    "name": "my_checkpoint",
                    "profilers": [],
                    "runtime_configuration": {},
                    "validations": [],
                }
            ),
            id="some updates",
        ),
    ],
)
def test_add_or_update_checkpoint_updates_successfully(
    in_memory_data_context: EphemeralDataContextSpy,
    checkpoint_config: dict,
    update_kwargs: dict,
    expected_config: CheckpointConfig,
):
    context = in_memory_data_context
    name = "my_checkpoint"
    checkpoint_config["name"] = name

    checkpoint = context.add_checkpoint(**checkpoint_config)

    assert context.checkpoint_store.save_count == 1

    checkpoint = context.add_or_update_checkpoint(name=name, **update_kwargs)

    assert checkpoint.config.to_dict() == expected_config.to_dict()
    assert context.checkpoint_store.save_count == 2
