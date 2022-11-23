from typing import Optional, Union, Dict, Any, List

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.expectations.expectation import ExpectationValidationResult, QueryExpectation
from great_expectations.expectations.metrics.import_manager import sa, sqlalchemy_engine_Engine, sqlalchemy_engine_Row
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.expectations.metrics.query_metric_provider import QueryMetricProvider
from great_expectations.util import get_sqlalchemy_subquery_type


class QueryMetricCompareTablesCustom(QueryMetricProvider):
    metric_name = "query.metric_compare_tables_custom"
    # values that you want to use in the metric
    value_keys = ("second_table_full_name", "first_table_id_column", "second_table_id_column", "query")

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, execution_engine: SqlAlchemyExecutionEngine, metric_domain_kwargs: dict,
                    metric_value_kwargs: dict, metrics: Dict[str, Any], runtime_configuration: dict) \
            -> List[sqlalchemy_engine_Row]:
        query: Optional[str] = metric_value_kwargs.get("query") or cls.default_kwarg_values.get("query")

        selectable: Union[sa.sql.Selectable, str]
        selectable, _, _ = execution_engine.get_compute_domain(metric_domain_kwargs,
                                                               domain_type=MetricDomainTypes.TABLE)
        # parameters sent by the user
        second_table_full_name: str = metric_value_kwargs.get("second_table_full_name")
        first_table_id_column: str = metric_value_kwargs.get("first_table_id_column")
        second_table_id_column: str = metric_value_kwargs.get("second_table_id_column")

        if isinstance(selectable, sa.Table):
            query = query.format(second_table_full_name=second_table_full_name,
                                 first_table_id_column=first_table_id_column,
                                 second_table_id_column=second_table_id_column, active_batch=selectable)
        elif isinstance(selectable, get_sqlalchemy_subquery_type()):
            # Specifying a runtime query in a RuntimeBatchRequest returns the active bacth as a Subquery; sectioning
            # the active batch off w/ parentheses ensures flow of operations doesn't break
            query = query.format(
                second_table_full_name=second_table_full_name, first_table_id_column=first_table_id_column,
                second_table_id_column=second_table_id_column,
                active_batch=f"({selectable})")

        engine: sqlalchemy_engine_Engine = execution_engine.engine
        result: List[sqlalchemy_engine_Row] = engine.execute(sa.text(query)).fetchall()
        return result


class ExpectAllIdsInFirstTableExistInSecondTableCustom(QueryExpectation):
    """Expect the frequency of occurrences of a specified value in a queried column to be at least <threshold>
    percent of values in that column."""

    metric_dependencies = ("query.template_values",)

    query = """
    select count(1) from (
    SELECT a.{first_table_id_column}
                    FROM {active_batch} a
                    LEFT JOIN {second_table_full_name} b
                    ON (a.{first_table_id_column}=b.{second_table_id_column})
                    WHERE b.{second_table_id_column} IS NULL
                    GROUP BY 1
                    )       
    """

    success_keys = (
        "column",
        "value",
        "query",
    )

    domain_keys = ("second_table_full_name", "first_table_id_column", "second_table_id_column",
                   "batch_id", "row_condition", "condition_parser",)

    default_kwarg_values = {
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
        "column": None,
        "value": "dummy_value",
        "query": query,
    }

    def _validate(
            self, configuration: ExpectationConfiguration,
            metrics: dict, runtime_configuration: dict = None, execution_engine: ExecutionEngine = None) -> \
            Union[ExpectationValidationResult, dict]:
        success = False

        query_result = metrics.get("query.metric_compare_tables_custom")
        num_of_missing_rows = query_result[0][0]

        if num_of_missing_rows == 0:
            success = True

        return {
            "success": success,
            "result": {
                'Rows with IDs in first table missing in second table': num_of_missing_rows}}

    examples = [
        {
            "data": [
                {
                    "dataset_name": "test",
                    "data": {
                        "msid": ["aaa", "aaa", "aaa",
                                 "aaa", "aaa", "aaa",
                                 "aaa", "aaa", "aaa"],
                    },
                },
                {
                    "dataset_name": "test_2",
                    "data": {
                        "msid": ["aaa", "aaa", "aaa",
                                 "aaa", "aaa", "aaa",
                                 "aaa", "aaa", "aaa"],
                    },
                },
            ],
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "second_table_full_name": "test_2",
                        "first_table_id_column": "msid",
                        "second_table_id_column": "msid",
                    },
                    "out": {"success": False},
                    "only_for": ["sqlite", "spark"],
                }
            ],
            "test_backends": [
                {
                    "backend": "sqlalchemy",
                    "dialects": ["sqlite"],
                }]
        },
    ]

    def validate_configuration(
            self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        super().validate_configuration(configuration)


if __name__ == "__main__":
    ExpectAllIdsInFirstTableExistInSecondTableCustom().print_diagnostic_checklist()
