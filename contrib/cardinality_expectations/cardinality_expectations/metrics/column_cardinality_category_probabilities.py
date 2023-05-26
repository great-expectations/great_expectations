from typing import Dict, Optional
from dataclasses import dataclass

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnAggregateExpectation
from great_expectations.expectations.metrics import (
    ColumnAggregateMetricProvider,
    column_aggregate_partial,
    column_aggregate_value,
)

from cardinality_expectations.cardinality_categories import (
    CardinalityCategoryProbabilities,
    Depth1CardinalityProbabilities,
    Depth2CardinalityProbabilities,
)

class ColumnCardinalityCategoryProbabilities(ColumnAggregateMetricProvider):
    metric_name = "column.cardinality_category_probabilities"

    value_keys = (
        "depth",
    )

    @classmethod
    def estimate_probabilities_with_cardinality_checker_method(
        cls,
        depth: int,
        n_unique: int,
        n_nonmissing: int,
        total_to_unique_ratio: float,
    ) -> CardinalityCategoryProbabilities:
        if depth == 1:
            if total_to_unique_ratio < 10:
                return Depth1CardinalityProbabilities(
                    infinite=1.0,
                    finite=0.0,
                )
            else:
                return Depth1CardinalityProbabilities(
                    infinite=0.0,
                    finite=1.0,
                )
        
        elif depth == 2:
            if n_unique < 7:
                return Depth2CardinalityProbabilities(
                    unique=0.0,
                    duplicated=0.0,
                    a_few=1.0,
                    several=0.0,
                    many=0.0,
                )
            elif n_unique < 20:
                return Depth2CardinalityProbabilities(
                    unique=0.0,
                    duplicated=0.0,
                    a_few=0.0,
                    several=1.0,
                    many=0.0,
                )
                
            elif n_unique == n_nonmissing:
                return Depth2CardinalityProbabilities(
                    unique=1.0,
                    duplicated=0.0,
                    a_few=0.0,
                    several=0.0,
                    many=0.0,
                )
            
            if total_to_unique_ratio < 10:
                return Depth2CardinalityProbabilities(
                    unique=0.0,
                    duplicated=1.0,
                    a_few=0.0,
                    several=0.0,
                    many=0.0,
                )

            else:
                return Depth2CardinalityProbabilities(
                    unique=0.0,
                    duplicated=0.0,
                    a_few=0.0,
                    several=0.0,
                    many=1.0,
                )

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, depth, **kwargs):
        n_unique: int = column.nunique()
        n_nonmissing: int = column.notnull().sum()
        total_to_unique_ratio: float = n_nonmissing / n_unique

        cardinality_category_probabilities = cls.estimate_probabilities_with_cardinality_checker_method(
            depth=depth,
            n_unique=n_unique,
            n_nonmissing=n_nonmissing,
            total_to_unique_ratio=total_to_unique_ratio,
        )

        return cardinality_category_probabilities

    # This method defines the business logic for evaluating your Metric when using a SqlAlchemyExecutionEngine
    # @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     raise NotImplementedError
    #
    # This method defines the business logic for evaluating your Metric when using a SparkDFExecutionEngine
    # @column_aggregate_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column, **kwargs):
    #     raise NotImplementedError