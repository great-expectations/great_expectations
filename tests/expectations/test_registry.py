import pytest

import great_expectations.exceptions as gx_exceptions
import great_expectations.expectations as gxe
from great_expectations.compatibility import sqlalchemy
from great_expectations.core.metric_function_types import MetricPartialFunctionTypes
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.execution_engine.sqlite_execution_engine import SqliteExecutionEngine
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.expectations.metrics.column_aggregate_metric_provider import (
    ColumnAggregateMetricProvider,
    column_aggregate_partial,
)
from great_expectations.expectations.registry import (
    _registered_metrics,
    get_expectation_impl,
    get_metric_kwargs,
    get_metric_provider,
    get_sqlalchemy_metric_provider,
)

# module level markers
pytestmark = pytest.mark.unit


def test_registry_basics():
    expectation = get_expectation_impl("expect_column_values_to_be_in_set")
    assert expectation == gxe.ExpectColumnValuesToBeInSet


def test_registry_from_configuration():
    configuration = ExpectationConfiguration(
        type="expect_column_values_to_be_in_set",
        kwargs={"column": "PClass", "value_set": [1, 2, 3]},
    )
    assert configuration._get_expectation_impl() == gxe.ExpectColumnValuesToBeInSet


def test_registry_raises_error_when_invalid_expectation_requested():
    with pytest.raises(gx_exceptions.ExpectationNotFoundError):
        get_expectation_impl("expect_something_in_beta")


def test_get_metric_provider_for_sqlalchemy_engine_subclass():
    # Regression test: a SqliteExecutionEngine (a SqlAlchemyExecutionEngine subclass with no
    # provider registered under its own name) must resolve metrics through the MRO fallback to
    # the provider registered for SqlAlchemyExecutionEngine, without disturbing the default.
    metric_name = "column.standard_deviation.aggregate_fn"

    sa_metric_provider, sa_metric_provider_fn = get_sqlalchemy_metric_provider(
        metric_name,
    )

    # get the metric provider resolved for the sqlite engine subclass
    sqlite_metric_provider, sqlite_metric_provider_fn = get_metric_provider(
        metric_name,
        SqliteExecutionEngine(engine=sqlalchemy.create_engine("sqlite://")),
    )
    assert sa_metric_provider is not None
    assert sqlite_metric_provider is not None
    # Both resolve to the same shared sqlalchemy provider: the sqlite dialect branch lives in
    # the base provider, so it also covers plain engines built over sqlite connections
    # (e.g. context.data_sources.add_sql("sqlite:///...")).
    assert sa_metric_provider == sqlite_metric_provider
    assert sa_metric_provider_fn is not None
    assert sqlite_metric_provider_fn is not None
    assert sa_metric_provider_fn == sqlite_metric_provider_fn


def test_metric_registered_for_engine_subclass_does_not_replace_base_engine_provider():
    # Regression test: a provider declared for a SqlAlchemyExecutionEngine subclass must be
    # registered under that subclass, leaving the provider declared for the base engine in
    # place. When column_aggregate_partial registered every provider under the base engine
    # regardless of the engine it was given, the subclass override silently replaced the
    # default for every SQL backend.
    class _RegistryTestSqlAlchemyEngine(SqlAlchemyExecutionEngine):
        pass

    metric_name = "column.registry_test_engine_subclass_provider"
    aggregate_fn_name = f"{metric_name}.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}"
    try:

        class _RegistryTestMetric(ColumnAggregateMetricProvider):
            metric_name = "column.registry_test_engine_subclass_provider"

            # Only registration is under test; neither provider is ever resolved.
            @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
            def _sqlalchemy(cls, column, **kwargs):
                raise NotImplementedError

            @column_aggregate_partial(engine=_RegistryTestSqlAlchemyEngine)
            def _sqlalchemy_subclass(cls, column, **kwargs):
                raise NotImplementedError

        sqlite_engine = sqlalchemy.create_engine("sqlite://")
        _, base_engine_fn = get_metric_provider(
            aggregate_fn_name, SqlAlchemyExecutionEngine(engine=sqlite_engine)
        )
        _, subclass_engine_fn = get_metric_provider(
            aggregate_fn_name, _RegistryTestSqlAlchemyEngine(engine=sqlite_engine)
        )

        assert base_engine_fn is _RegistryTestMetric._sqlalchemy
        assert subclass_engine_fn is _RegistryTestMetric._sqlalchemy_subclass
    finally:
        _registered_metrics.pop(metric_name, None)
        _registered_metrics.pop(aggregate_fn_name, None)


@pytest.mark.parametrize(
    "expectation_type, kwargs",
    [
        # domain_keys omit batch_id on these; the metric must still learn which Batch.
        pytest.param("expect_table_row_count_to_equal", {"value": 3}, id="table_row_count"),
        pytest.param(
            "expect_column_values_to_be_null", {"column": "x"}, id="column_values_to_be_null"
        ),
        # and the base-class domain_keys, which carry batch_id already
        pytest.param(
            "expect_column_max_to_be_between",
            {"column": "x", "max_value": 1},
            id="column_max_to_be_between",
        ),
    ],
)
def test_get_metric_kwargs_carries_the_configuration_batch_id_to_the_metric_domain(
    expectation_type: str, kwargs: dict
):
    configuration = ExpectationConfiguration(
        type=expectation_type, kwargs={**kwargs, "batch_id": "my_datasource-my_asset"}
    )
    expectation = configuration.to_domain_obj()

    dependencies = expectation.get_validation_dependencies()

    metric_configurations = dependencies.get_metric_configurations()
    assert metric_configurations
    for metric_configuration in metric_configurations:
        assert metric_configuration.metric_domain_kwargs["batch_id"] == "my_datasource-my_asset"


def test_get_metric_kwargs_leaves_batch_id_unset_when_the_configuration_has_none():
    configuration = ExpectationConfiguration(
        type="expect_table_row_count_to_equal", kwargs={"value": 3}
    )

    metric_kwargs = get_metric_kwargs("table.row_count", configuration=configuration)

    assert metric_kwargs["metric_domain_kwargs"]["batch_id"] is None
