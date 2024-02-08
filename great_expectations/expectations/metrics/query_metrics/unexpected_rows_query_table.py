from great_expectations.compatibility.typing_extensions import override
from great_expectations.expectations.metrics.query_metrics.query_table import QueryTable


class UnexpectedRowsQueryTable(QueryTable):
    metric_name = "unexpected_rows_query.table"
    value_keys = ("unexpected_rows_query",)

    @override
    @classmethod
    def _get_query_param_name(cls) -> str:
        return "unexpected_rows_query"
