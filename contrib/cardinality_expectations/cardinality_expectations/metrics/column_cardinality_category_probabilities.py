import joblib
from typing import Dict, List, Optional
from dataclasses import dataclass
import os
import numpy as np

from great_expectations.data_context.util import file_relative_path
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
    """MetricProvider Class for column cardinality category probabilities
    
    This class implements the metric provider for the column cardinality category probabilities.
    """

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
            
    @classmethod
    def estimate_probabilities_with_ML_model(
        cls,
        depth: int,
        n_unique: int,
        n_nonmissing: int,
        total_to_unique_ratio: float,
        value_counts: List[int],
    ) -> CardinalityCategoryProbabilities:
        if depth == 1:
            model = joblib.load(
                # os.path.join(
                file_relative_path(
                    __file__,
                    "depth_1_model.joblib"
                ),
                # 
            )

            proba = model.predict_proba([[
                np.log(n_unique),
                np.log(n_nonmissing),
                np.log(total_to_unique_ratio),
                np.log(value_counts[0]),
                np.log(value_counts[1]),
                np.log(value_counts[2]),
                np.log(value_counts[3]),
                np.log(value_counts[4]),
            ]])

            return Depth1CardinalityProbabilities(
                infinite=proba[0][0],
                finite=proba[0][1],
            )

        elif depth == 2:
            model = joblib.load(
                file_relative_path(
                    __file__,
                    "depth_2_model.joblib"
                ),
            )

            proba = model.predict_proba([[
                np.log(total_to_unique_ratio),
                np.log(n_unique),
                np.log(n_nonmissing),
                np.log(value_counts[0]/n_nonmissing) if len(value_counts) > 0 else -100,
                np.log(value_counts[1]/n_nonmissing) if len(value_counts) > 1 else -100,
                np.log(value_counts[2]/n_nonmissing) if len(value_counts) > 2 else -100,
                np.log(value_counts[3]/n_nonmissing) if len(value_counts) > 3 else -100,
                np.log(value_counts[4]/n_nonmissing) if len(value_counts) > 4 else -100,
            ]])

            return Depth2CardinalityProbabilities(
                unique=proba[0][4],
                duplicated=proba[0][1],
                a_few=proba[0][0],
                several=proba[0][3],
                many=proba[0][2],
            )


    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, depth, **kwargs):
        n_unique: int = column.nunique()
        n_nonmissing: int = column.notnull().sum()
        total_to_unique_ratio: float = n_nonmissing / n_unique
        value_counts = column.value_counts().tolist()

        # cardinality_category_probabilities = cls.estimate_probabilities_with_cardinality_checker_method(
        cardinality_category_probabilities = cls.estimate_probabilities_with_ML_model(
            depth=depth,
            n_unique=n_unique,
            n_nonmissing=n_nonmissing,
            total_to_unique_ratio=total_to_unique_ratio,
            value_counts=value_counts,
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