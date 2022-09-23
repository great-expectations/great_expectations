"""TODO: Add docstring"""
from dataclasses import dataclass

import pytest

from great_expectations.checkpoint import Checkpoint
from great_expectations.core import (
    ExpectationSuite,
    ExpectationSuiteValidationResult,
    RunIdentifier,
)
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.cloud_migrator import (
    ConfigurationBundle,
    ConfigurationBundleJsonSerializer,
    ConfigurationBundleSchema,
)
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.rule_based_profiler import RuleBasedProfiler
from great_expectations.rule_based_profiler.config.base import (
    ruleBasedProfilerConfigSchema,
)


@dataclass
class DataContextWithSavedItems:
    context: BaseDataContext
    expectation_suite: ExpectationSuite
    checkpoint: Checkpoint
    profiler: RuleBasedProfiler
    validation_result: ExpectationSuiteValidationResult


@pytest.fixture
def in_memory_runtime_context_with_configs_in_stores(
    in_memory_runtime_context: BaseDataContext, profiler_rules: dict
) -> DataContextWithSavedItems:

    context: BaseDataContext = in_memory_runtime_context

    # Add items to the context:

    # Add expectation suite
    expectation_suite: ExpectationSuite = ExpectationSuite(
        expectation_suite_name="my_suite"
    )
    context.save_expectation_suite(expectation_suite)

    # Add checkpoint
    checkpoint: Checkpoint = context.add_checkpoint(
        name="my_checkpoint", class_name="SimpleCheckpoint"
    )

    # Add profiler
    profiler = context.add_profiler(
        "my_profiler", config_version=1.0, rules=profiler_rules
    )

    # Add validation result
    expectation_suite_validation_result = ExpectationSuiteValidationResult(
        success=True,
    )
    context.validations_store.set(
        key=ValidationResultIdentifier(
            expectation_suite_identifier=ExpectationSuiteIdentifier("my_suite"),
            run_id=RunIdentifier(run_name="my_run", run_time=None),
            batch_identifier="my_batch_identifier",
        ),
        value=expectation_suite_validation_result,
    )

    return DataContextWithSavedItems(
        context=context,
        expectation_suite=expectation_suite,
        checkpoint=checkpoint,
        profiler=profiler,
        validation_result=expectation_suite_validation_result,
    )


def dict_equal(dict_1: dict, dict_2: dict) -> bool:
    return convert_to_json_serializable(dict_1) == convert_to_json_serializable(dict_2)


def list_of_dicts_equal(list_1: list, list_2: list) -> bool:

    if len(list_1) != len(list_2):
        return False
    for i in range(len(list_1)):
        if not dict_equal(list_1[i], list_2[i]):
            return False

    return True


@pytest.mark.integration
def test_configuration_bundle_init(
    in_memory_runtime_context_with_configs_in_stores: DataContextWithSavedItems,
):
    """What does this test and why?

    This test is an integration test using a real DataContext to prove out that
    ConfigurationBundle successfully bundles. We can replace this test with
    unit tests.
    """

    context: BaseDataContext = in_memory_runtime_context_with_configs_in_stores.context

    config_bundle = ConfigurationBundle(context)

    assert not config_bundle.is_usage_stats_enabled()

    # Spot check items in variables
    assert (
        config_bundle._data_context_variables.checkpoint_store_name
        == "checkpoint_store"
    )
    assert config_bundle._data_context_variables.progress_bars is None

    assert config_bundle._expectation_suites == [
        in_memory_runtime_context_with_configs_in_stores.expectation_suite
    ]

    assert list_of_dicts_equal(
        config_bundle._checkpoints,
        [in_memory_runtime_context_with_configs_in_stores.checkpoint.config],
    )

    roundtripped_profiler_config = ruleBasedProfilerConfigSchema.load(
        ruleBasedProfilerConfigSchema.dump(
            in_memory_runtime_context_with_configs_in_stores.profiler.config
        )
    )
    assert list_of_dicts_equal(config_bundle._profilers, [roundtripped_profiler_config])

    assert config_bundle._validation_results == [
        in_memory_runtime_context_with_configs_in_stores.validation_result
    ]


@pytest.mark.integration
def test_configuration_bundle_serialization(
    in_memory_runtime_context_with_configs_in_stores: DataContextWithSavedItems,
):
    """What does this test and why?

    This test is an integration test using a real DataContext to prove out the
    ConfigurationBundle serialization. We can replace this test with unit tests.
    """

    context: BaseDataContext = in_memory_runtime_context_with_configs_in_stores.context

    config_bundle = ConfigurationBundle(context)

    serializer = ConfigurationBundleJsonSerializer(schema=ConfigurationBundleSchema())

    serialized_bundle: dict = serializer.serialize(config_bundle)

    expected_serialized_bundle = {
        "checkpoints": [
            {
                "action_list": [
                    {
                        "action": {"class_name": "StoreValidationResultAction"},
                        "name": "store_validation_result",
                    },
                    {
                        "action": {"class_name": "StoreEvaluationParametersAction"},
                        "name": "store_evaluation_params",
                    },
                    {
                        "action": {
                            "class_name": "UpdateDataDocsAction",
                            "site_names": [],
                        },
                        "name": "update_data_docs",
                    },
                ],
                "batch_request": {},
                "class_name": "Checkpoint",
                "config_version": 1.0,
                "evaluation_parameters": {},
                "expectation_suite_ge_cloud_id": None,
                "expectation_suite_name": None,
                "ge_cloud_id": None,
                "module_name": "great_expectations.checkpoint",
                "name": "my_checkpoint",
                "profilers": [],
                "run_name_template": None,
                "runtime_configuration": {},
                "template_name": None,
                "validations": [],
            }
        ],
        "data_context_variables": {
            "checkpoint_store_name": "checkpoint_store",
            "config_variables_file_path": None,
            "config_version": 3.0,
            "data_docs_sites": {},
            "evaluation_parameter_store_name": "evaluation_parameter_store",
            "expectations_store_name": "expectations_store",
            "include_rendered_content": {
                "expectation_suite": False,
                "expectation_validation_result": False,
                "globally": False,
            },
            "notebooks": None,
            "plugins_directory": None,
            "profiler_store_name": "profiler_store",
            "stores": {
                "checkpoint_store": {
                    "class_name": "CheckpointStore",
                    "store_backend": {"class_name": "InMemoryStoreBackend"},
                },
                "evaluation_parameter_store": {
                    "class_name": "EvaluationParameterStore"
                },
                "expectations_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {"class_name": "InMemoryStoreBackend"},
                },
                "profiler_store": {
                    "class_name": "ProfilerStore",
                    "store_backend": {"class_name": "InMemoryStoreBackend"},
                },
                "validations_store": {
                    "class_name": "ValidationsStore",
                    "store_backend": {"class_name": "InMemoryStoreBackend"},
                },
            },
            "validations_store_name": "validations_store",
        },
        "expectation_suites": [
            {
                "data_asset_type": None,
                "expectation_suite_name": "my_suite",
                "expectations": [],
                "ge_cloud_id": None,
                "meta": {"great_expectations_version": "0.15.24+14.g6eff2678d.dirty"},
            }
        ],
        "profilers": [
            {
                "class_name": "RuleBasedProfiler",
                "config_version": 1.0,
                "module_name": "great_expectations.rule_based_profiler",
                "name": "my_profiler",
                "rules": {
                    "rule_1": {
                        "domain_builder": {
                            "class_name": "TableDomainBuilder",
                            "module_name": "great_expectations.rule_based_profiler.domain_builder",
                        },
                        "expectation_configuration_builders": [
                            {
                                "class_name": "DefaultExpectationConfigurationBuilder",
                                "column_A": "$domain.domain_kwargs.column_A",
                                "column_B": "$domain.domain_kwargs.column_B",
                                "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                                "meta": {
                                    "profiler_details": {
                                        "my_parameter_estimator": "$parameter.my_parameter.details",
                                        "note": "Important "
                                        "remarks "
                                        "about "
                                        "estimation "
                                        "algorithm.",
                                    }
                                },
                                "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                                "my_arg": "$parameter.my_parameter.value[0]",
                                "my_other_arg": "$parameter.my_parameter.value[1]",
                            }
                        ],
                        "parameter_builders": [
                            {
                                "class_name": "MetricMultiBatchParameterBuilder",
                                "metric_name": "my_metric",
                                "module_name": "great_expectations.rule_based_profiler.parameter_builder",
                                "name": "my_parameter",
                            }
                        ],
                        "variables": {},
                    }
                },
                "variables": {},
            }
        ],
        "validation_results": [
            {
                "evaluation_parameters": {},
                "meta": {},
                "results": [],
                "statistics": {},
                "success": True,
            }
        ],
    }

    # Remove meta before comparing since it contains the GX version
    serialized_bundle["expectation_suites"][0].pop("meta", None)
    expected_serialized_bundle["expectation_suites"][0].pop("meta", None)

    assert serialized_bundle == expected_serialized_bundle


def test_is_usage_statistics_key_set_if_key_not_present():
    """What does this test and why?

    The ConfigurationBundle should handle a context that has not set the config for
     anonymous_usage_statistics.
    """
    # TODO: Implementation
    pass


@pytest.mark.integration
def test_anonymous_usage_statistics_removed_during_serialization(
    in_memory_runtime_context_with_configs_in_stores: DataContextWithSavedItems,
):
    """What does this test and why?

    When serializing a ConfigurationBundle we need to remove the
    anonymous_usage_statistics key.

    This is currently an integration test using a real Data Context, it can
    be converted to a unit test.
    """

    context: BaseDataContext = in_memory_runtime_context_with_configs_in_stores.context

    assert context.anonymous_usage_statistics is not None

    config_bundle = ConfigurationBundle(context)

    serializer = ConfigurationBundleJsonSerializer(schema=ConfigurationBundleSchema())

    serialized_bundle: dict = serializer.serialize(config_bundle)

    assert (
        serialized_bundle["data_context_variables"].get("anonymous_usage_statistics")
        is None
    )
