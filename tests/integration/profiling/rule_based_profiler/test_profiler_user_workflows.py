import contextlib
import copy
import datetime
from numbers import Number
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    Protocol,
    Tuple,
    Type,
    cast,
)

import numpy as np
import pandas as pd
import pytest
from freezegun import freeze_time
from packaging import version

from great_expectations.core import (
    ExpectationSuite,
)
from great_expectations.core.batch import BatchRequest
from great_expectations.core.domain import (
    INFERRED_SEMANTIC_TYPE_KEY,
    Domain,
    SemanticDomainTypes,
)
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.datasource import DataConnector, Datasource
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.rule_based_profiler import RuleBasedProfilerResult
from great_expectations.rule_based_profiler.config.base import (
    RuleBasedProfilerConfig,
    ruleBasedProfilerConfigSchema,
)
from great_expectations.rule_based_profiler.helpers.util import (
    get_validator_with_expectation_suite,
)
from great_expectations.rule_based_profiler.parameter_container import ParameterNode
from great_expectations.rule_based_profiler.rule_based_profiler import RuleBasedProfiler
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.validator import Validator
from tests.rule_based_profiler.conftest import ATOL, RTOL

if TYPE_CHECKING:
    from ruamel.yaml.comments import CommentedMap

yaml = YAMLHandler()

TIMESTAMP: str = "09/26/2019 13:42:41"


@pytest.fixture
def alice_validator(alice_columnar_table_single_batch_context) -> Validator:
    context = alice_columnar_table_single_batch_context

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    validator: Validator = get_validator_with_expectation_suite(
        data_context=context,
        batch_list=None,
        batch_request=batch_request,
        expectation_suite_name=None,
        expectation_suite=None,
        component_name="profiler",
        persist=False,
    )

    assert len(validator.batches) == 1
    return validator


@pytest.fixture
def bobby_validator(
    bobby_columnar_table_multi_batch_deterministic_data_context,
) -> Validator:
    context = bobby_columnar_table_multi_batch_deterministic_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    validator: Validator = get_validator_with_expectation_suite(
        data_context=context,
        batch_list=None,
        batch_request=batch_request,
        expectation_suite_name=None,
        expectation_suite=None,
        component_name="profiler",
        persist=False,
    )

    assert len(validator.batches) == 3
    return validator


@pytest.fixture
def bobster_validator(
    bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000_data_context,
    set_consistent_seed_within_numeric_metric_range_multi_batch_parameter_builder,
) -> Validator:
    """Utilizes a consistent bootstrap seed in its RBP NumericMetricRangeMultiBatchParameterBuilder."""  # noqa: E501
    context = bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000_data_context

    # Use all batches, loaded by Validator, for estimating Expectation argument values.
    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    validator: Validator = get_validator_with_expectation_suite(
        data_context=context,
        batch_list=None,
        batch_request=batch_request,
        expectation_suite_name=None,
        expectation_suite=None,
        component_name="profiler",
        persist=False,
    )

    assert len(validator.batches) == 36
    return validator


@pytest.fixture
def quentin_validator(
    quentin_columnar_table_multi_batch_data_context,
    set_consistent_seed_within_numeric_metric_range_multi_batch_parameter_builder,
) -> Validator:
    """Utilizes a consistent bootstrap seed in its RBP NumericMetricRangeMultiBatchParameterBuilder."""  # noqa: E501
    context = quentin_columnar_table_multi_batch_data_context

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    validator: Validator = get_validator_with_expectation_suite(
        data_context=context,
        batch_list=None,
        batch_request=batch_request,
        expectation_suite_name=None,
        expectation_suite=None,
        component_name="profiler",
        persist=False,
    )

    assert len(validator.batches) == 36
    return validator


@pytest.mark.slow  # 1.15s
@pytest.mark.big
def test_alice_columnar_table_single_batch_batches_are_accessible(
    alice_columnar_table_single_batch_context,
    alice_columnar_table_single_batch,
):
    """
    What does this test and why?
    Batches created in the multibatch_generic_csv_generator fixture should be available using the
    multibatch_generic_csv_generator_context
    This test most likely duplicates tests elsewhere, but it is more of a test of the configurable fixture.
    """  # noqa: E501

    context = alice_columnar_table_single_batch_context

    datasource_name: str = "alice_columnar_table_single_batch_datasource"
    data_connector_name: str = "alice_columnar_table_single_batch_data_connector"
    data_asset_name: str = "alice_columnar_table_single_batch_data_asset"

    datasource: Datasource = cast(Datasource, context.datasources[datasource_name])
    data_connector: DataConnector = datasource.data_connectors[data_connector_name]

    file_list: List[str] = [alice_columnar_table_single_batch["sample_data_relative_path"]]

    assert (
        data_connector._get_data_reference_list_from_cache_by_data_asset_name(
            data_asset_name=data_asset_name
        )
        == file_list
    )

    batch_request_1: BatchRequest = BatchRequest(
        datasource_name=datasource_name,
        data_connector_name=data_connector_name,
        data_asset_name=data_asset_name,
        data_connector_query={
            "index": -1,
        },
    )
    # Should give most recent batch
    validator_1: Validator = context.get_validator(
        batch_request=batch_request_1,
        create_expectation_suite_with_name="my_expectation_suite_name_1",
    )
    metric_max: int = validator_1.get_metric(
        MetricConfiguration("column.max", metric_domain_kwargs={"column": "event_type"})
    )
    assert metric_max == 73


@freeze_time(TIMESTAMP)
@pytest.mark.slow  # 2.31s
@pytest.mark.big
def test_alice_profiler_user_workflow_single_batch(
    caplog,
    alice_columnar_table_single_batch_context,
    alice_columnar_table_single_batch,
):
    # Load data context
    data_context = alice_columnar_table_single_batch_context

    # Load profiler configs & loop (run tests for each one)
    yaml_config: str = alice_columnar_table_single_batch["profiler_config"]

    # Instantiate Profiler
    profiler_config: CommentedMap = yaml.load(yaml_config)

    # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.  # noqa: E501
    deserialized_config: dict = ruleBasedProfilerConfigSchema.load(profiler_config)
    serialized_config: dict = ruleBasedProfilerConfigSchema.dump(deserialized_config)

    # `class_name`/`module_name` are generally consumed through `instantiate_class_from_config`
    # so we need to manually remove those values if we wish to use the **kwargs instantiation pattern  # noqa: E501
    serialized_config.pop("class_name")
    serialized_config.pop("module_name")

    profiler: RuleBasedProfiler = RuleBasedProfiler(
        **serialized_config,
        data_context=data_context,
    )

    # BatchRequest yielding exactly one batch
    alice_single_batch_data_batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }
    result: RuleBasedProfilerResult = profiler.run(
        batch_request=alice_single_batch_data_batch_request
    )

    expectation_configuration: ExpectationConfiguration
    for expectation_configuration in result.expectation_configurations:
        if "profiler_details" in expectation_configuration.meta:
            expectation_configuration.meta["profiler_details"].pop("estimation_histogram", None)

    assert (
        result.expectation_configurations
        == alice_columnar_table_single_batch[
            "expected_expectation_suite"
        ].expectation_configurations
    )


# noinspection PyUnusedLocal
@pytest.mark.slow  # 1.16s
@pytest.mark.big
def test_bobby_columnar_table_multi_batch_batches_are_accessible(
    monkeypatch,
    bobby_columnar_table_multi_batch_deterministic_data_context,
    bobby_columnar_table_multi_batch,
):
    """
    What does this test and why?
    Batches created in the multibatch_generic_csv_generator fixture should be available using the
    multibatch_generic_csv_generator_context
    This test most likely duplicates tests elsewhere, but it is more of a test of the configurable fixture.
    """  # noqa: E501

    context = bobby_columnar_table_multi_batch_deterministic_data_context

    datasource_name: str = "taxi_pandas"
    data_connector_name: str = "monthly"
    data_asset_name: str = "my_reports"

    datasource: Datasource = cast(Datasource, context.datasources[datasource_name])
    data_connector: DataConnector = datasource.data_connectors[data_connector_name]

    file_list: List[str] = [
        "yellow_tripdata_sample_2019-01.csv",
        "yellow_tripdata_sample_2019-02.csv",
        "yellow_tripdata_sample_2019-03.csv",
    ]

    assert (
        data_connector._get_data_reference_list_from_cache_by_data_asset_name(
            data_asset_name=data_asset_name
        )
        == file_list
    )

    batch_request_latest: BatchRequest = BatchRequest(
        datasource_name=datasource_name,
        data_connector_name=data_connector_name,
        data_asset_name=data_asset_name,
        data_connector_query={
            "index": -1,
        },
    )
    validator_latest: Validator = context.get_validator(
        batch_request=batch_request_latest,
        create_expectation_suite_with_name="my_expectation_suite_name_1",
    )

    metric_configuration_arguments: Dict[str, Any] = {
        "metric_name": "table.row_count",
        "metric_domain_kwargs": {
            "batch_id": validator_latest.active_batch_id,
        },
        "metric_value_kwargs": None,
    }
    metric_value: int = validator_latest.get_metric(
        metric=MetricConfiguration(**metric_configuration_arguments)
    )
    assert metric_value == 9000

    # noinspection PyUnresolvedReferences
    pickup_datetime: datetime.datetime = pd.to_datetime(
        validator_latest.head(n_rows=1)["pickup_datetime"][0]
    ).to_pydatetime()
    month: int = pickup_datetime.month
    assert month == 3


@pytest.mark.skipif(
    version.parse(np.version.version) < version.parse("1.21.0"),
    reason="requires numpy version 1.21.0 or newer",
)
@freeze_time(TIMESTAMP)
@pytest.mark.slow  # 13.08s
@pytest.mark.big
def test_bobby_profiler_user_workflow_multi_batch_row_count_range_rule_and_column_ranges_rule_quantiles_estimator(  # noqa: E501
    caplog,
    bobby_columnar_table_multi_batch_deterministic_data_context,
    bobby_columnar_table_multi_batch,
):
    # Load data context
    data_context = bobby_columnar_table_multi_batch_deterministic_data_context

    # Load profiler configs & loop (run tests for each one)
    yaml_config: str = bobby_columnar_table_multi_batch["profiler_config"]

    # Instantiate Profiler
    profiler_config: dict = yaml.load(yaml_config)

    # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.  # noqa: E501
    deserialized_config: dict = ruleBasedProfilerConfigSchema.load(profiler_config)
    serialized_config: dict = ruleBasedProfilerConfigSchema.dump(deserialized_config)

    # `class_name`/`module_name` are generally consumed through `instantiate_class_from_config`
    # so we need to manually remove those values if we wish to use the **kwargs instantiation pattern  # noqa: E501
    serialized_config.pop("class_name")
    serialized_config.pop("module_name")

    profiler: RuleBasedProfiler = RuleBasedProfiler(
        **serialized_config,
        data_context=data_context,
    )

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    result: RuleBasedProfilerResult = profiler.run(batch_request=batch_request)

    domain: Domain

    fixture_expectation_suite: ExpectationSuite = bobby_columnar_table_multi_batch[
        "test_configuration_quantiles_estimator"
    ]["expected_expectation_suite"]

    expectation_configuration: ExpectationConfiguration
    for expectation_configuration in result.expectation_configurations:
        if "profiler_details" in expectation_configuration.meta:
            expectation_configuration.meta["profiler_details"].pop("estimation_histogram", None)

    assert result.expectation_configurations == fixture_expectation_suite.expectation_configurations

    profiled_fully_qualified_parameter_names_by_domain: Dict[Domain, List[str]] = (
        profiler.get_fully_qualified_parameter_names_by_domain()
    )

    fixture_fully_qualified_parameter_names_by_domain: Dict[Domain, List[str]] = (
        bobby_columnar_table_multi_batch[
            "test_configuration_quantiles_estimator"
        ]["expected_fixture_fully_qualified_parameter_names_by_domain"]
    )

    assert (
        profiled_fully_qualified_parameter_names_by_domain
        == fixture_fully_qualified_parameter_names_by_domain
    )

    domain = Domain(
        domain_type=MetricDomainTypes.TABLE,
        rule_name="row_count_range_rule",
    )

    profiled_fully_qualified_parameter_names_for_domain_id: List[str] = (
        profiler.get_fully_qualified_parameter_names_for_domain_id(domain.id)
    )

    fixture_fully_qualified_parameter_names_for_domain_id: List[str] = (
        bobby_columnar_table_multi_batch[
            "test_configuration_quantiles_estimator"
        ]["expected_fixture_fully_qualified_parameter_names_by_domain"][domain]
    )

    assert (
        profiled_fully_qualified_parameter_names_for_domain_id
        == fixture_fully_qualified_parameter_names_for_domain_id
    )

    profiled_parameter_values_for_fully_qualified_parameter_names_by_domain: Dict[
        Domain, Dict[str, ParameterNode]
    ] = profiler.get_parameter_values_for_fully_qualified_parameter_names_by_domain()

    fixture_profiled_parameter_values_for_fully_qualified_parameter_names_by_domain: Dict[
        Domain, Dict[str, ParameterNode]
    ] = bobby_columnar_table_multi_batch["test_configuration_quantiles_estimator"][
        "expected_parameter_values_for_fully_qualified_parameter_names_by_domain"
    ]

    assert convert_to_json_serializable(
        data=profiled_parameter_values_for_fully_qualified_parameter_names_by_domain
    ) == convert_to_json_serializable(
        data=fixture_profiled_parameter_values_for_fully_qualified_parameter_names_by_domain
    )

    domain = Domain(
        domain_type="column",
        domain_kwargs={"column": "VendorID"},
        details={
            INFERRED_SEMANTIC_TYPE_KEY: {
                "VendorID": SemanticDomainTypes.NUMERIC,
            },
        },
        rule_name="column_ranges_rule",
    )

    profiled_parameter_values_for_fully_qualified_parameter_names_for_domain_id: Dict[
        str, ParameterNode
    ] = profiler.get_parameter_values_for_fully_qualified_parameter_names_for_domain_id(
        domain_id=domain.id
    )

    fixture_profiled_parameter_values_for_fully_qualified_parameter_names_for_domain_id: Dict[
        str, ParameterNode
    ] = bobby_columnar_table_multi_batch["test_configuration_quantiles_estimator"][
        "expected_parameter_values_for_fully_qualified_parameter_names_by_domain"
    ][domain]

    assert convert_to_json_serializable(
        data=profiled_parameter_values_for_fully_qualified_parameter_names_for_domain_id
    ) == convert_to_json_serializable(
        data=fixture_profiled_parameter_values_for_fully_qualified_parameter_names_for_domain_id
    )


class HasStaticDefaultProfiler(Protocol):
    default_profiler_config: RuleBasedProfilerConfig
    # I'd like to force the key "profiler_config" to be present in the following dict.
    # While its absence doesn't break functionality, I do expect it to exist. TypeDicts
    # unfortunately don't help us here since one needs to list all potential keys in a
    # TypeDict since they don't allow extras keys to be present. See the discussion here:
    # https://github.com/python/mypy/issues/4617#issuecomment-367647383
    default_kwarg_values: Dict[str, Any]


# An expectations default profiler config is a static variable. Setting it to a custom value will
# actually overwrite the default. This will cause other tests in this session to fail that depend
# on the default value. This decorator stores the default value and restores it at the end of this test.  # noqa: E501
# We shouldn't be overwriting the default and that is ticketed:
# https://superconductive.atlassian.net/browse/GREAT-1127
@contextlib.contextmanager
def restore_profiler_config(
    expectation: Type[HasStaticDefaultProfiler],
) -> Iterator[None]:
    original_default_profiler_config = copy.deepcopy(expectation.default_profiler_config)
    try:
        yield
    finally:
        expectation.default_profiler_config = original_default_profiler_config
        expectation.default_kwarg_values["profiler_config"] = original_default_profiler_config


@pytest.mark.skipif(
    version.parse(np.version.version) < version.parse("1.21.0"),
    reason="requires numpy version 1.21.0 or newer",
)
@pytest.mark.slow  # 4.83s
@pytest.mark.big
def test_bobster_profiler_user_workflow_multi_batch_row_count_range_rule_bootstrap_estimator(
    caplog,
    bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000_data_context,
    bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000,
    set_consistent_seed_within_numeric_metric_range_multi_batch_parameter_builder,
):
    # Load data context
    data_context = bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000_data_context

    # Load profiler configs & loop (run tests for each one)
    yaml_config: str = bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000[
        "profiler_config"
    ]

    # Instantiate Profiler
    profiler_config: CommentedMap = yaml.load(yaml_config)

    # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.  # noqa: E501
    deserialized_config: dict = ruleBasedProfilerConfigSchema.load(profiler_config)
    serialized_config: dict = ruleBasedProfilerConfigSchema.dump(deserialized_config)

    # `class_name`/`module_name` are generally consumed through `instantiate_class_from_config`
    # so we need to manually remove those values if we wish to use the **kwargs instantiation pattern  # noqa: E501
    serialized_config.pop("class_name")
    serialized_config.pop("module_name")

    profiler: RuleBasedProfiler = RuleBasedProfiler(
        **serialized_config,
        data_context=data_context,
    )

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    result: RuleBasedProfilerResult = profiler.run(batch_request=batch_request)

    expect_table_row_count_to_be_between_expectation_configuration_kwargs: dict = (
        result.expectation_configurations[0].to_json_dict()["kwargs"]
    )
    min_value: int = expect_table_row_count_to_be_between_expectation_configuration_kwargs[
        "min_value"
    ]
    max_value: int = expect_table_row_count_to_be_between_expectation_configuration_kwargs[
        "max_value"
    ]

    assert (
        bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000[
            "test_configuration_bootstrap_estimator"
        ]["expect_table_row_count_to_be_between_min_value_mean_value"]
        < min_value
        < bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000[
            "test_configuration_bootstrap_estimator"
        ]["expect_table_row_count_to_be_between_mean_value"]
    )
    assert (
        bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000[
            "test_configuration_bootstrap_estimator"
        ]["expect_table_row_count_to_be_between_mean_value"]
        < max_value
        < bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000[
            "test_configuration_bootstrap_estimator"
        ]["expect_table_row_count_to_be_between_max_value_mean_value"]
    )


@pytest.mark.skipif(
    version.parse(np.version.version) < version.parse("1.21.0"),
    reason="requires numpy version 1.21.0 or newer",
)
@pytest.mark.slow  # 15.07s
@pytest.mark.big
def test_quentin_profiler_user_workflow_multi_batch_quantiles_value_ranges_rule(
    caplog,
    quentin_columnar_table_multi_batch_data_context,
    quentin_columnar_table_multi_batch,
    set_consistent_seed_within_numeric_metric_range_multi_batch_parameter_builder,
):
    # Load data context
    data_context = quentin_columnar_table_multi_batch_data_context

    # Load profiler configs & loop (run tests for each one)
    yaml_config: str = quentin_columnar_table_multi_batch["profiler_config"]

    # Instantiate Profiler
    profiler_config: CommentedMap = yaml.load(yaml_config)

    # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.  # noqa: E501
    deserialized_config: dict = ruleBasedProfilerConfigSchema.load(profiler_config)
    serialized_config: dict = ruleBasedProfilerConfigSchema.dump(deserialized_config)

    # `class_name`/`module_name` are generally consumed through `instantiate_class_from_config`
    # so we need to manually remove those values if we wish to use the **kwargs instantiation pattern  # noqa: E501
    serialized_config.pop("class_name")
    serialized_config.pop("module_name")

    profiler: RuleBasedProfiler = RuleBasedProfiler(
        **serialized_config,
        data_context=data_context,
    )

    batch_request: dict = {
        "datasource_name": "taxi_pandas",
        "data_connector_name": "monthly",
        "data_asset_name": "my_reports",
    }

    result: RuleBasedProfilerResult = profiler.run(batch_request=batch_request)

    expectation_configuration: ExpectationConfiguration
    expectation_configuration_dict: dict
    column_name: str
    expectation_kwargs: dict
    expect_column_quantile_values_to_be_between_expectation_configurations_kwargs_dict: Dict[
        str, dict
    ] = {
        expectation_configuration_dict["kwargs"]["column"]: expectation_configuration_dict["kwargs"]
        for expectation_configuration_dict in [
            expectation_configuration.to_json_dict()
            for expectation_configuration in result.expectation_configurations
        ]
    }
    expect_column_quantile_values_to_be_between_expectation_configurations_value_ranges_by_column: Dict[  # noqa: E501
        str, List[List[Number]]
    ] = {
        column_name: expectation_kwargs["quantile_ranges"]["value_ranges"]
        for column_name, expectation_kwargs in expect_column_quantile_values_to_be_between_expectation_configurations_kwargs_dict.items()  # noqa: E501
    }

    assert (
        expect_column_quantile_values_to_be_between_expectation_configurations_value_ranges_by_column[
            "tolls_amount"
        ]
        == quentin_columnar_table_multi_batch["test_configuration"][
            "expect_column_quantile_values_to_be_between_quantile_ranges_by_column"
        ]["tolls_amount"]
    )

    value_ranges: List[Tuple[Tuple[float, float]]]
    paired_quantiles: zip
    column_quantiles: List[List[Number]]
    idx: int
    for (
        column_name,
        column_quantiles,
    ) in expect_column_quantile_values_to_be_between_expectation_configurations_value_ranges_by_column.items():  # noqa: E501
        paired_quantiles = zip(
            column_quantiles,
            quentin_columnar_table_multi_batch["test_configuration"][
                "expect_column_quantile_values_to_be_between_quantile_ranges_by_column"
            ][column_name],
        )
        for value_ranges in list(paired_quantiles):
            for idx in range(2):
                np.testing.assert_allclose(
                    actual=value_ranges[0][idx],
                    desired=value_ranges[1][idx],
                    rtol=RTOL,
                    atol=ATOL,
                    err_msg=f"Actual value of {value_ranges[0][idx]} differs from expected value of {value_ranges[1][idx]} by more than {ATOL + RTOL * abs(value_ranges[1][idx])} tolerance.",  # noqa: E501
                )
