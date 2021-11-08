import json
import logging
from typing import Callable, Dict, Optional

import numpy as np

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.exceptions.exceptions import InvalidExpectationKwargsError
from great_expectations.execution_engine.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import ColumnExpectation
from great_expectations.expectations.registry import get_metric_kwargs
from great_expectations.validator.metric_configuration import MetricConfiguration

logger = logging.getLogger(__name__)

try:
    import sqlalchemy as sa
    from sqlalchemy.dialects import registry
except ImportError:
    logger.debug(
        "Unable to load SqlAlchemy context; install optional sqlalchemy dependency for support."
    )
    sa = None
    registry = None

try:
    import sqlalchemy_redshift.dialect
except ImportError:
    sqlalchemy_redshift = None

try:
    import pybigquery.sqlalchemy_bigquery

    registry.register("bigquery", "pybigquery.sqlalchemy_bigquery", "dialect")

    try:
        getattr(pybigquery.sqlalchemy_bigquery, "INTEGER")
        bigquery_types_tuple = None
    except AttributeError:
        logger.warning(
            "Old pybigquery driver version detected. Consider upgrading to 0.4.14 or later."
        )
        from collections import namedtuple

        BigQueryTypes = namedtuple(
            "BigQueryTypes", sorted(pybigquery.sqlalchemy_bigquery._type_map)
        )
        bigquery_types_tuple = BigQueryTypes(**pybigquery.sqlalchemy_bigquery._type_map)
except ImportError:
    bigquery_types_tuple = None
    pybigquery = None


class ExpectColumnValuesToBeStringIntegersMonotonicallyIncreasing(ColumnExpectation):
    """Expect a column to contain string-typed integers to be monotonically increasing.

    expect_column_values_to_be_string_integers_monotonically_increasing is a :func:`column_map_expectation \
    <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.

    Args:
        column (str): \
            The column name.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.
    See Also:
        :func:`expect_column_values_to_be_decreasing \
        <great_expectations.execution_engine.execution_engine.ExecutionEngine
        .expect_column_values_to_be_decreasing>`
    """

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "production",
        "package": "great_expectations",
        "tags": ["core expectation", "column map expectation"],
        "contributors": ["@great_expectations"],
        "requirements": [],
    }

    map_metric = "column_values.string_integers.monotonically_increasing"

    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }

    def _validate_success_key(
        param: str,
        required: bool,
        configuration: Optional[ExpectationConfiguration],
        validation_rules: Dict[Callable, str],
    ) -> None:
        """"""
        if param not in configuration.kwargs:
            if required:
                raise InvalidExpectationKwargsError(
                    f"Parameter {param} is required but was not found in configuration."
                )
            return

        param_value = configuration.kwargs[param]

        for rule, error_message in validation_rules.items():
            if not rule(param_value):
                raise InvalidExpectationKwargsError(error_message)

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> bool:
        return super().validate_configuration(configuration=configuration)

    def get_validation_dependencies(
        self,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> dict:

        dependencies = super().get_validation_dependencies(
            configuration=configuration,
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )

        metric_kwargs = get_metric_kwargs(
            metric_name="column_values.string_integers.monotonically_increasing.map",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        dependencies["metrics"][
            "column_values.string_integers.monotonically_increasing"
        ] = MetricConfiguration(
            metric_name="column_values.string_integers.monotonically_increasing.map",
            metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
            metric_value_kwargs=metric_kwargs["metric_domain_kwargs"],
        )

        return dependencies

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ) -> Dict:

        SIMI = metrics.get("column_values.string_integers.monotonically_increasing")
        success = all(SIMI[0])

        return ExpectationValidationResult(
            expectation_config={
                "expectation_type": "expect_column_values_to_be_string_integers_monotonically_increasing",
                "kwargs": {
                    "column": "a",
                },
                "meta": {},
                "ge_cloud_id": None,
            },
            meta={},
            result={"observed_value": np.unique(SIMI[0], return_counts=True)},
            success=success,
        )


if __name__ == "__main__":
    self_check_report = (
        ExpectColumnValuesToBeStringIntegersMonotonicallyIncreasing().run_diagnostics()
    )
    print(json.dumps(self_check_report, indent=2))
