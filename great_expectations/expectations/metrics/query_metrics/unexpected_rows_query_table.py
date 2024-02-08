from typing import ClassVar

from great_expectations.expectations.metrics.query_metrics.query_table import QueryTable


class UnexpectedRowsQueryTable(QueryTable):
    metric_name = "unexpected_rows_query.table"
    value_keys = ("unexpected_rows_query",)

    # The name of the parameter that will be used to pass the unexpected_rows_query to the query metric provider
    _query_param: ClassVar[str] = "unexpected_rows_query"
