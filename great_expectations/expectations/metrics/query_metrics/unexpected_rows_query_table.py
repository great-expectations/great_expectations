from typing import ClassVar

from great_expectations.expectations.metrics.query_metrics.query_table import QueryTable


class UnexpectedRowsQueryTable(QueryTable):
    metric_name = "unexpected_rows_query.table"
    value_keys = ("unexpected_rows_query",)

    query_param_name: ClassVar[str] = "unexpected_rows_query"
