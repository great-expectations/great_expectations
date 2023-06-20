from typing import Any, Dict, List, Tuple, Type, Optional, Union, Callable

from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
)
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics import (
    ColumnAggregateMetricProvider,
)
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
)
from great_expectations.execution_engine import (
    ExecutionEngine,
)
from great_expectations.expectations.metrics.metric_provider import (
    MetricProvider,
)
from great_expectations.core.metric_function_types import (
    MetricFunctionTypes,
    MetricPartialFunctionTypes,
)

from great_expectations.expectations.metrics.map_metric_provider.column_condition_partial import (
    column_condition_partial,
)
from great_expectations.expectations.metrics.map_metric_provider.column_pair_condition_partial import (
    column_pair_condition_partial,
)
from great_expectations.expectations.metrics.column_aggregate_metric_provider import (
    column_aggregate_value,
    column_aggregate_partial,
)

from great_expectations.expectations.registry import _registered_metrics


class TestingShimMetricProvider(MetricProvider):

    _registered_metrics = {}

    @classmethod
    def _register_metric(
        cls,
        metric_name: str,
        metric_domain_keys: Tuple[str, ...],
        metric_value_keys: Tuple[str, ...],
        execution_engine: Type[ExecutionEngine],
        metric_class: Type[MetricProvider],
        metric_provider: Optional[Callable],
        metric_fn_type: Optional[
            Union[MetricFunctionTypes, MetricPartialFunctionTypes]
        ] = None,
    ) -> None:
        """Register a metric function with the global registry.
        
        This method is a shim to make _register_metric_functions unit-testable.

        It loosely replicates the functionality of the _register_metric method in great_expectations/expectations/registry.py. Instead of storing metrics in a global registry, it stores it in a registry on the class. This allows us to test the _register_metric_functions method without having to worry about polluting the global registry, namespace collisions, etc.
        """

        execution_engine_name = execution_engine.__name__
        metric_definition = {
            "metric_domain_keys": metric_domain_keys,
            "metric_value_keys": metric_value_keys,
            "default_kwarg_values": metric_class.default_kwarg_values,
            "providers": {execution_engine_name: (metric_class, metric_provider)},
        }
        cls._registered_metrics[metric_name] = metric_definition

# def test__a():
#     """"""

#     assert [metric for metric in _registered_metrics.keys() if metric.startswith("column_values.equal_seven")] == []

#     class CustomColumnValuesEqualSeven(ColumnMapMetricProvider, TestingShimMetricProvider):
#         condition_metric_name = "column_values.equal_seven"

#         @column_condition_partial(engine=PandasExecutionEngine)
#         def _pandas(cls, column, **kwargs):
#             return column == 7

#         @column_condition_partial(engine=SqlAlchemyExecutionEngine)
#         def _sqlalchemy(cls, column, **kwargs):
#             return column.is_(7)

#         @column_condition_partial(engine=SparkDFExecutionEngine)
#         def _spark(cls, column, **kwargs):
#             return column.contains(7)
        
#     CustomColumnValuesEqualSeven()

#     assert [metric for metric in CustomColumnValuesEqualSeven._registered_metrics.keys() if metric.startswith("column_values.equal_seven")] == [
#         'column_values.equal_seven.condition',
#         'column_values.equal_seven.unexpected_count',
#         'column_values.equal_seven.unexpected_index_list',
#         'column_values.equal_seven.unexpected_index_query',
#         'column_values.equal_seven.unexpected_rows',
#         'column_values.equal_seven.unexpected_values',
#         'column_values.equal_seven.unexpected_value_counts',
#         'column_values.equal_seven.unexpected_count.aggregate_fn',
#     ]

#     print(CustomColumnValuesEqualSeven._registered_metrics["column_values.equal_seven.condition"])
#     # {
#     #     'metric_domain_keys': ('batch_id', 'table', 'column', 'row_condition', 'condition_parser'),
#     #     'metric_value_keys': (),
#     #     'default_kwarg_values': {},
#     #     'providers': {
#     #         'PandasExecutionEngine': (
#     #             <class 'tests.expectations.metrics.test__register_metric_functions.test__a.<locals>.CustomColumnValuesEqualSeven'>,
#     #             <function test__a.<locals>.CustomColumnValuesEqualSeven._pandas at 0x7fcf24d5fb80>
#     #         ),
#     #         'SparkDFExecutionEngine': (
#     #             <class 'tests.expectations.metrics.test__register_metric_functions.test__a.<locals>.CustomColumnValuesEqualSeven'>,
#     #             <function test__a.<locals>.CustomColumnValuesEqualSeven._spark at 0x7fcf24d5fee0>
#     #         ),
#     #         'SqlAlchemyExecutionEngine': (
#     #             <class 'tests.expectations.metrics.test__register_metric_functions.test__a.<locals>.CustomColumnValuesEqualSeven'>,
#     #             <function test__a.<locals>.CustomColumnValuesEqualSeven._sqlalchemy at 0x7fcf24d5fd30>
#     #         )
#     #     }
#     # }


#     # assert False


# def test__b(mocker):
#     """Tried to get mocking to work. Gave in 30min. Couldn't."""

#     action = mocker.patch(
#         "great_expectations.expectations.registry.register_metric"
#     )

#     assert [metric for metric in _registered_metrics.keys() if metric.startswith("column_values.equal_seven")] == []

#     class CustomColumnValuesEqualSeven(ColumnMapMetricProvider):
#         condition_metric_name = "column_values.equal_seven"

#         @column_condition_partial(engine=PandasExecutionEngine)
#         def _pandas(cls, column, **kwargs):
#             return column == 7

#         @column_condition_partial(engine=SqlAlchemyExecutionEngine)
#         def _sqlalchemy(cls, column, **kwargs):
#             return column.is_(7)

#         @column_condition_partial(engine=SparkDFExecutionEngine)
#         def _spark(cls, column, **kwargs):
#             return column.contains(7)
        
#     CustomColumnValuesEqualSeven._register_metric_functions()

#     assert [metric_name for metric_name in _registered_metrics.keys() if metric.startswith("column_values.equal_seven")] == [
#         'column_values.equal_seven.condition',
#         'column_values.equal_seven.unexpected_count',
#         'column_values.equal_seven.unexpected_index_list',
#         'column_values.equal_seven.unexpected_index_query',
#         'column_values.equal_seven.unexpected_rows',
#         'column_values.equal_seven.unexpected_values',
#         'column_values.equal_seven.unexpected_value_counts',
#         'column_values.equal_seven.unexpected_count.aggregate_fn',
#     ]

#     print(_registered_metrics["column_values.equal_seven.condition"])

#     action.assert_called_with(
#         metric_name="column_values.equal_seven.condition",
#     )

def test__column_map_metric_provider_with_pandas_column_condition_partial_decorator():
    """Tests that metrics are registered correctly when a ColumnMapMetricProvider is instantiated with a _pandas @column_condition_partial decorator. """

    assert [metric for metric in _registered_metrics.keys() if metric.startswith("column_values.equal_seven")] == []

    metrics_to_be_created = [
        'column_values.equal_seven.condition',
        'column_values.equal_seven.unexpected_count',
        'column_values.equal_seven.unexpected_index_list',
        'column_values.equal_seven.unexpected_index_query',
        'column_values.equal_seven.unexpected_rows',
        'column_values.equal_seven.unexpected_values',
        'column_values.equal_seven.unexpected_value_counts',
    ]

    class CustomColumnValuesEqualSeven(ColumnMapMetricProvider):
        condition_metric_name = "column_values.equal_seven"

        @column_condition_partial(engine=PandasExecutionEngine)
        def _pandas(cls, column, **kwargs):
            return column == 7
        
    CustomColumnValuesEqualSeven()

    assert [metric for metric in _registered_metrics.keys() if metric.startswith("column_values.equal_seven")] == metrics_to_be_created

    for metric in metrics_to_be_created:
        assert set(_registered_metrics[metric].keys()) == {'metric_domain_keys', 'metric_value_keys', 'default_kwarg_values', 'providers'}
        assert set(_registered_metrics[metric]["providers"]) == {'PandasExecutionEngine'}

    # from inspect import signature
    # class_, method_ = _registered_metrics["unexpected_values"]["providers"]['PandasExecutionEngine']
    # print(class_)
    # print(method_)

    # print( signature(method_) )


    # Delete all metrics created in this step
    for metric in metrics_to_be_created:
        del _registered_metrics[metric]



def test__column_map_metric_provider_with_incorrect_column_pair_decorators():
    """Tests that the wrong metrics are registered when a ColumnMapMetricProvider is instantiated with the wrong decorators.
    
    Note: Each of these is essentially a silent failure. This is not good behavior, but it's how things work right now.
    """

    metrics_to_be_created = [
        'column_values.equal_seven.condition',
        'column_values.equal_seven.unexpected_count',
        'column_values.equal_seven.unexpected_index_list',
        'column_values.equal_seven.unexpected_index_query',
        'column_values.equal_seven.unexpected_rows',
        'column_values.equal_seven.unexpected_values',
        'column_values.equal_seven.filtered_row_count',
    ]

    assert [metric for metric in _registered_metrics.keys() if metric.startswith("column_values.equal_seven")] == []


    class CustomColumnValuesEqualSeven(ColumnMapMetricProvider):
        condition_metric_name = "column_values.equal_seven"

        @column_pair_condition_partial(engine=PandasExecutionEngine)
        def _pandas(cls, column_A, column_B, **kwargs):
            return column_A == 7

        @column_pair_condition_partial(engine=SqlAlchemyExecutionEngine)
        def _sqlalchemy(cls, column_A, column_B, _dialect, **kwargs):
            return column_A.is_(7)

        @column_pair_condition_partial(engine=SparkDFExecutionEngine)
        def _spark(cls, column_A, column_B, **kwargs):
            return column_A.contains(7)


    CustomColumnValuesEqualSeven()
    assert [metric for metric in _registered_metrics.keys() if metric.startswith("column_values.equal_seven")] == metrics_to_be_created

    # Delete all metrics created in this step
    for metric in metrics_to_be_created:
        del _registered_metrics[metric]


def test__column_map_metric_provider_with_incorrect_column_aggregate_decorators():
    """Tests that the wrong metrics are registered when a ColumnMapMetricProvider is instantiated with the wrong decorators.
    
    Note: Each of these is essentially a silent failure. This is not good behavior, but it's how things work right now.
    """

    # print("^" * 80)
    # print(_registered_metrics.keys())

    metrics_to_be_created = [
        # 'column_values.equal_seven.condition',
        # 'column_values.equal_seven.unexpected_count',
        # 'column_values.equal_seven.unexpected_index_list',
        # 'column_values.equal_seven.unexpected_index_query',
        # 'column_values.equal_seven.unexpected_rows',
        # 'column_values.equal_seven.unexpected_values',
        # 'column_values.equal_seven.filtered_row_count',
    ]

    assert [metric for metric in _registered_metrics.keys() if metric.startswith("column_values.equal_seven")] == []


    class CustomColumnValuesEqualSeven(ColumnMapMetricProvider):
        condition_metric_name = "column_values.equal_seven"

        @column_aggregate_value(engine=PandasExecutionEngine)
        def _pandas(cls, column, **kwargs):
            return column_A.sum() == 7

        # @column_aggregate_partial(engine=PandasExecutionEngine)
        # def _pandas(cls, column_A, column_B, **kwargs):
        #     return column_A.sum() == 7

        @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
        def _sqlalchemy(cls, column, _dialect, **kwargs):
            raise sa.func.sum(column) == 7
        
        @column_aggregate_partial(engine=SparkDFExecutionEngine)
        def _spark(cls, column, **kwargs):
            raise F.sum(column) == 7

    CustomColumnValuesEqualSeven()
    assert [metric for metric in _registered_metrics.keys() if metric.startswith("column_values.equal_seven")] == []

    # Delete all metrics created in this step
    for metric in metrics_to_be_created:
        del _registered_metrics[metric]




"""
1. Test existence of Metrics
2. Test that Expectations evaluate successfully
3. Test intermediate outputs:
	1. Metric method signatures (?)
    2. Metric inputs/outputs
4. Performance



1. Build "integration" tests that test both the MetricProvider, the Expectation class, the registry, and the execution of Metrics
(Order TBD)
2. Refactor the registry to allow better testing of it as an independent object
3. Add typing Metrics
4. Add explicit testing for metrics
"""


def test__column_map_metric_provider_with_all_three_column_condition_partial_decorators():
    """Tests that metrics are registered correctly when a ColumnMapMetricProvider is instantiated with all three @column_condition_partial decorators."""

    metrics_to_be_created = [
        'column_values.equal_seven.condition',
        'column_values.equal_seven.unexpected_count',
        'column_values.equal_seven.unexpected_index_list',
        'column_values.equal_seven.unexpected_index_query',
        'column_values.equal_seven.unexpected_rows',
        'column_values.equal_seven.unexpected_values',
        'column_values.equal_seven.unexpected_value_counts',
        'column_values.equal_seven.unexpected_count.aggregate_fn',
    ]


    assert [metric for metric in _registered_metrics.keys() if metric.startswith("column_values.equal_seven")] == []

    class CustomColumnValuesEqualSeven(ColumnMapMetricProvider):
        condition_metric_name = "column_values.equal_seven"

        @column_condition_partial(engine=PandasExecutionEngine)
        def _pandas(cls, column, **kwargs):
            return column == 7

        @column_condition_partial(engine=SqlAlchemyExecutionEngine)
        def _sqlalchemy(cls, column, **kwargs):
            return column.is_(7)

        @column_condition_partial(engine=SparkDFExecutionEngine)
        def _spark(cls, column, **kwargs):
            return column.contains(7)
        
    CustomColumnValuesEqualSeven()

    assert [metric for metric in _registered_metrics.keys() if metric.startswith("column_values.equal_seven")] == metrics_to_be_created

    for metric in metrics_to_be_created:
        assert set(_registered_metrics[metric].keys()) == {
            'metric_domain_keys',
            'metric_value_keys',
            'default_kwarg_values',
            'providers',
        }
        if metric != 'column_values.equal_seven.unexpected_count.aggregate_fn':
            assert set(_registered_metrics[metric]["providers"]) == {
                'PandasExecutionEngine',
                'SqlAlchemyExecutionEngine',
                'SparkDFExecutionEngine',
            }
        else:
            assert set(_registered_metrics[metric]["providers"]) == {
                'SqlAlchemyExecutionEngine',
                'SparkDFExecutionEngine',
            }


    from inspect import signature
    class_, method_ = _registered_metrics["column_values.equal_seven.unexpected_values"]["providers"]['PandasExecutionEngine']
    # print(class_)
    print(method_)
    print( signature(method_) )

    class_, method_ = _registered_metrics["column_values.equal_seven.unexpected_values"]["providers"]['SqlAlchemyExecutionEngine']
    # print(class_)
    print(method_)
    print( signature(method_) )

    class_, method_ = _registered_metrics["column_values.equal_seven.unexpected_values"]["providers"]['SparkDFExecutionEngine']
    # print(class_)
    print(method_)
    print( signature(method_) )

    # Delete all metrics created in this step
    for metric in metrics_to_be_created:
        del _registered_metrics[metric]



def test__column_aggregate_metric_provider_with_all_three_decorators():
    """Tests that metrics are registered correctly when a ColumnAggregateMetricProvider is instantiated with the right decorators."""

    metrics_to_be_created = [
        'column.sum_equals_seven',
        'column.sum_equals_seven.aggregate_fn',
    ]

    start_keys = set(_registered_metrics.keys())
    assert [metric for metric in _registered_metrics.keys() if metric.startswith("column.sum_equals_seven")] == []


    class CustomColumnValuesSumToSeven(ColumnAggregateMetricProvider):
        metric_name = "column.sum_equals_seven"

        @column_aggregate_value(engine=PandasExecutionEngine)
        def _pandas(cls, column, **kwargs):
            return column.sum() == 7

        @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
        def _sqlalchemy(cls, column, _dialect, **kwargs):
            raise sa.func.sum(column) == 7
        
        @column_aggregate_partial(engine=SparkDFExecutionEngine)
        def _spark(cls, column, **kwargs):
            raise F.sum(column) == 7

    CustomColumnValuesSumToSeven()
    # print(_registered_metrics.keys())
    # print(start_keys)
    print( start_keys.difference(set(_registered_metrics.keys())) )
    print( set(_registered_metrics.keys()).difference(start_keys) )
    assert [metric for metric in _registered_metrics.keys() if metric.startswith("column.sum_equals_seven")] == metrics_to_be_created

    assert set(_registered_metrics["column.sum_equals_seven"].keys()) == {
        'metric_domain_keys',
        'metric_value_keys',
        'default_kwarg_values',
        'providers',
    }
    assert set(_registered_metrics["column.sum_equals_seven"]["providers"]) == {
        'PandasExecutionEngine',
        'SqlAlchemyExecutionEngine',
        'SparkDFExecutionEngine',
    }

    assert set(_registered_metrics["column.sum_equals_seven.aggregate_fn"].keys()) == {
        'metric_domain_keys',
        'metric_value_keys',
        'default_kwarg_values',
        'providers',
    }
    assert set(_registered_metrics["column.sum_equals_seven.aggregate_fn"]["providers"]) == {
        'SqlAlchemyExecutionEngine',
        'SparkDFExecutionEngine',
    }

    from inspect import signature

    class_, method_ = _registered_metrics["column.sum_equals_seven"]["providers"]['PandasExecutionEngine']
    print("="*80)
    print( class_ )
    print( type(method_) )
    print( method_ )
    print( signature(method_) )

    class_, method_ = _registered_metrics["column.sum_equals_seven"]["providers"]['SqlAlchemyExecutionEngine']
    print("="*80)
    print( class_ )
    print( type(method_) )
    print( method_ )
    print( signature(method_) )

    class_, method_ = _registered_metrics["column.sum_equals_seven"]["providers"]['SparkDFExecutionEngine']
    print("="*80)
    print( class_ )
    print( type(method_) )
    print( method_ )
    print( signature(method_) )


    # Delete all metrics created in this step
    for metric in metrics_to_be_created:
        del _registered_metrics[metric]

    assert False