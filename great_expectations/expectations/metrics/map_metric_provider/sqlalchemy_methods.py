from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Union,
)

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.metric_function_types import (
    SummarizationMetricNameSuffixes,
)
from great_expectations.core.util import convert_to_json_serializable

if TYPE_CHECKING:
    from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    OperationalError,
)
from great_expectations.expectations.metrics.import_manager import quoted_name, sa
from great_expectations.expectations.metrics.map_metric_provider.map_metric_provider import (
    MapMetricProvider,
)
from great_expectations.expectations.metrics.util import (
    Insert,
    Label,
    Select,
    get_dbms_compatible_column_names,
    get_sqlalchemy_source_table_and_schema,
    sql_statement_with_post_compile_to_string,
    verify_column_names_exist,
)
from great_expectations.util import (
    generate_temporary_table_name,
    get_sqlalchemy_selectable,
)

logger = logging.getLogger(__name__)


def _sqlalchemy_map_condition_unexpected_count_aggregate_fn(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Returns unexpected count for MapExpectations"""
    unexpected_condition, compute_domain_kwargs, accessor_domain_kwargs = metrics.get(
        "unexpected_condition"
    )

    return (
        sa.func.sum(
            sa.case(
                [(unexpected_condition, 1)],
                else_=0,
            )
        ),
        compute_domain_kwargs,
        accessor_domain_kwargs,
    )


def _sqlalchemy_map_condition_unexpected_count_value(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Returns unexpected count for MapExpectations. This is a *value* metric, which is useful for
    when the unexpected_condition is a window function.
    """
    unexpected_condition, compute_domain_kwargs, accessor_domain_kwargs = metrics.get(
        "unexpected_condition"
    )
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    selectable = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    # The integral values are cast to SQL Numeric in order to avoid a bug in AWS Redshift (converted to integer later).
    count_case_statement: List[Label] = sa.case(
        [
            (
                unexpected_condition,
                sa.sql.expression.cast(1, sa.Numeric),
            )
        ],
        else_=sa.sql.expression.cast(0, sa.Numeric),
    ).label("condition")

    count_selectable: Select = sa.select([count_case_statement])
    if not MapMetricProvider.is_sqlalchemy_metric_selectable(map_metric_provider=cls):
        selectable = get_sqlalchemy_selectable(selectable)
        count_selectable = count_selectable.select_from(selectable)

    try:
        if execution_engine.engine.dialect.name.lower() == GXSqlDialect.MSSQL:
            temp_table_name: str = generate_temporary_table_name(
                default_table_name_prefix="#ge_temp_"
            )

            with execution_engine.engine.begin():
                metadata: sa.MetaData = sa.MetaData(execution_engine.engine)
                temp_table_obj: sa.Table = sa.Table(
                    temp_table_name,
                    metadata,
                    sa.Column(
                        "condition", sa.Integer, primary_key=False, nullable=False
                    ),
                )
                temp_table_obj.create(execution_engine.engine, checkfirst=True)

                inner_case_query: Insert = temp_table_obj.insert().from_select(
                    [count_case_statement],
                    count_selectable,
                )
                execution_engine.engine.execute(inner_case_query)

                count_selectable = temp_table_obj

        count_selectable = get_sqlalchemy_selectable(count_selectable)
        unexpected_count_query: Select = (
            sa.select(
                [
                    sa.func.sum(sa.column("condition")).label("unexpected_count"),
                ]
            )
            .select_from(count_selectable)
            .alias("UnexpectedCountSubquery")
        )

        unexpected_count: Union[float, int] = execution_engine.engine.execute(
            sa.select(
                [
                    unexpected_count_query.c[
                        f"{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
                    ],
                ]
            )
        ).scalar()
        # Unexpected count can be None if the table is empty, in which case the count
        # should default to zero.
        try:
            unexpected_count = int(unexpected_count)
        except TypeError:
            unexpected_count = 0

    except OperationalError as oe:
        exception_message: str = f"An SQL execution Exception occurred: {str(oe)}."
        raise gx_exceptions.InvalidMetricAccessorDomainKwargsKeyError(
            message=exception_message
        )

    return convert_to_json_serializable(unexpected_count)


def _sqlalchemy_column_map_condition_values(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """
    Particularly for the purpose of finding unexpected values, returns all the metric values which do not meet an
    expected Expectation condition for ColumnMapExpectation Expectations.
    """
    unexpected_condition, compute_domain_kwargs, accessor_domain_kwargs = metrics.get(
        "unexpected_condition"
    )
    selectable = execution_engine.get_domain_records(
        domain_kwargs=compute_domain_kwargs
    )

    if "column" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column" found in provided metric_domain_kwargs, but it is required for a column map metric
(_sqlalchemy_column_map_condition_values).
"""
        )

    column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]

    column_name = get_dbms_compatible_column_names(
        column_names=column_name,
        batch_columns_list=metrics["table.columns"],
    )

    query = sa.select([sa.column(column_name).label("unexpected_values")]).where(
        unexpected_condition
    )
    if not MapMetricProvider.is_sqlalchemy_metric_selectable(map_metric_provider=cls):
        query = query.select_from(selectable)

    result_format = metric_value_kwargs["result_format"]
    if result_format["result_format"] != "COMPLETE":
        query = query.limit(result_format["partial_unexpected_count"])
    elif (
        result_format["result_format"] == "COMPLETE"
        and execution_engine.engine.dialect.name.lower() == GXSqlDialect.BIGQUERY
    ):
        logger.warning(
            "BigQuery imposes a limit of 10000 parameters on individual queries; "
            "if your data contains more than 10000 columns your results will be truncated."
        )
        query = query.limit(10000)  # BigQuery upper bound on query parameters

    return [
        val.unexpected_values
        for val in execution_engine.engine.execute(query).fetchall()
    ]


def _sqlalchemy_column_pair_map_condition_values(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Return values from the specified domain that match the map-style metric in the metrics dictionary."""
    (
        boolean_mapped_unexpected_values,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics["unexpected_condition"]
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    selectable = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    # noinspection PyPep8Naming
    column_A_name = accessor_domain_kwargs["column_A"]
    # noinspection PyPep8Naming
    column_B_name = accessor_domain_kwargs["column_B"]

    column_names: List[Union[str, quoted_name]] = [
        column_A_name,
        column_B_name,
    ]
    # noinspection PyPep8Naming
    column_A_name, column_B_name = get_dbms_compatible_column_names(
        column_names=column_names,
        batch_columns_list=metrics["table.columns"],
    )

    query = sa.select(
        [
            sa.column(column_A_name).label("unexpected_values_A"),
            sa.column(column_B_name).label("unexpected_values_B"),
        ]
    ).where(boolean_mapped_unexpected_values)
    if not MapMetricProvider.is_sqlalchemy_metric_selectable(map_metric_provider=cls):
        selectable = get_sqlalchemy_selectable(selectable)
        query = query.select_from(selectable)

    result_format = metric_value_kwargs["result_format"]
    if result_format["result_format"] != "COMPLETE":
        query = query.limit(result_format["partial_unexpected_count"])

    unexpected_list = [
        (val.unexpected_values_A, val.unexpected_values_B)
        for val in execution_engine.engine.execute(query).fetchall()
    ]
    return unexpected_list


def _sqlalchemy_column_pair_map_condition_filtered_row_count(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Return record counts from the specified domain that match the map-style metric in the metrics dictionary."""
    _, compute_domain_kwargs, accessor_domain_kwargs = metrics["unexpected_condition"]
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    selectable = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    # noinspection PyPep8Naming
    column_A_name = accessor_domain_kwargs["column_A"]
    # noinspection PyPep8Naming
    column_B_name = accessor_domain_kwargs["column_B"]

    column_names: List[Union[str, quoted_name]] = [column_A_name, column_B_name]
    verify_column_names_exist(
        column_names=column_names, batch_columns_list=metrics["table.columns"]
    )

    return execution_engine.engine.execute(
        sa.select([sa.func.count()]).select_from(selectable)
    ).scalar()


def _sqlalchemy_multicolumn_map_condition_values(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Return values from the specified domain that match the map-style metric in the metrics dictionary."""
    (
        boolean_mapped_unexpected_values,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics["unexpected_condition"]
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    selectable = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    if "column_list" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column_list" found in provided metric_domain_kwargs, but it is required for a multicolumn map metric
(_sqlalchemy_multicolumn_map_condition_values).
"""
        )

    column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs["column_list"]

    column_list = get_dbms_compatible_column_names(
        column_names=column_list,
        batch_columns_list=metrics["table.columns"],
    )

    column_selector = [sa.column(column_name) for column_name in column_list]
    query = sa.select(column_selector).where(boolean_mapped_unexpected_values)
    if not MapMetricProvider.is_sqlalchemy_metric_selectable(map_metric_provider=cls):
        selectable = get_sqlalchemy_selectable(selectable)
        query = query.select_from(selectable)

    result_format = metric_value_kwargs["result_format"]
    if result_format["result_format"] != "COMPLETE":
        query = query.limit(result_format["partial_unexpected_count"])

    return [dict(val) for val in execution_engine.engine.execute(query).fetchall()]


def _sqlalchemy_multicolumn_map_condition_filtered_row_count(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Return record counts from the specified domain that match the map-style metric in the metrics dictionary."""
    _, compute_domain_kwargs, accessor_domain_kwargs = metrics["unexpected_condition"]
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    selectable = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    if "column_list" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column_list" found in provided metric_domain_kwargs, but it is required for a multicolumn map metric
(_sqlalchemy_multicolumn_map_condition_filtered_row_count).
"""
        )

    column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs["column_list"]
    verify_column_names_exist(
        column_names=column_list, batch_columns_list=metrics["table.columns"]
    )

    selectable = get_sqlalchemy_selectable(selectable)

    return execution_engine.engine.execute(
        sa.select([sa.func.count()]).select_from(selectable)
    ).scalar()


def _sqlalchemy_column_map_condition_value_counts(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """
    Returns value counts for all the metric values which do not meet an expected Expectation condition for instances
    of ColumnMapExpectation.
    """
    unexpected_condition, compute_domain_kwargs, accessor_domain_kwargs = metrics.get(
        "unexpected_condition"
    )
    selectable = execution_engine.get_domain_records(
        domain_kwargs=compute_domain_kwargs
    )

    if "column" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column" found in provided metric_domain_kwargs, but it is required for a column map metric
(_sqlalchemy_column_map_condition_value_counts).
"""
        )

    column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]

    column_name = get_dbms_compatible_column_names(
        column_names=column_name,
        batch_columns_list=metrics["table.columns"],
    )

    column: sa.Column = sa.column(column_name)

    query = (
        sa.select([column, sa.func.count(column)])
        .where(unexpected_condition)
        .group_by(column)
    )
    if not MapMetricProvider.is_sqlalchemy_metric_selectable(map_metric_provider=cls):
        query = query.select_from(selectable)

    return execution_engine.engine.execute(query).fetchall()


def _sqlalchemy_map_condition_rows(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """
    Returns all rows of the metric values which do not meet an expected Expectation condition for instances
    of ColumnMapExpectation.
    """
    unexpected_condition, compute_domain_kwargs, accessor_domain_kwargs = metrics.get(
        "unexpected_condition"
    )
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    selectable = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    table_columns = metrics.get("table.columns")
    column_selector = [sa.column(column_name) for column_name in table_columns]
    query = sa.select(column_selector).where(unexpected_condition)
    if not MapMetricProvider.is_sqlalchemy_metric_selectable(map_metric_provider=cls):
        selectable = get_sqlalchemy_selectable(selectable)
        query = query.select_from(selectable)

    result_format = metric_value_kwargs["result_format"]
    if result_format["result_format"] != "COMPLETE":
        query = query.limit(result_format["partial_unexpected_count"])
    try:
        return execution_engine.engine.execute(query).fetchall()
    except OperationalError as oe:
        exception_message: str = f"An SQL execution Exception occurred: {str(oe)}."
        raise gx_exceptions.InvalidMetricAccessorDomainKwargsKeyError(
            message=exception_message
        )


def _sqlalchemy_map_condition_query(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: Dict,
    metric_value_kwargs: Dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> Optional[str]:
    """
    Returns query that will return all rows which do not meet an expected Expectation condition for instances
    of ColumnMapExpectation.

    Requires `unexpected_index_column_names` to be part of `result_format` dict to specify primary_key columns
    to return, along with column the Expectation is run on.
    """
    (
        unexpected_condition,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics.get("unexpected_condition")

    result_format: dict = metric_value_kwargs["result_format"]
    # We will not return map_condition_query if return_unexpected_index_query = False
    return_unexpected_index_query: bool = result_format.get(
        "return_unexpected_index_query"
    )
    if return_unexpected_index_query is False:
        return

    domain_column_name_list: List[str] = list()
    # column map expectations
    if "column" in accessor_domain_kwargs:
        column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]
        domain_column_name_list.append(column_name)
    # multi-column map expectations
    elif "column_list" in accessor_domain_kwargs:
        column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs[
            "column_list"
        ]
        domain_column_name_list = column_list
    # column-map expectations
    elif "column_A" in accessor_domain_kwargs and "column_B" in accessor_domain_kwargs:
        column_list: List[Union[str, quoted_name]] = list()
        column_list.append(accessor_domain_kwargs["column_A"])
        column_list.append(accessor_domain_kwargs["column_B"])
        domain_column_name_list = column_list

    column_selector: List[sa.Column] = []

    all_table_columns: List[str] = metrics.get("table.columns")
    unexpected_index_column_names: List[str] = result_format.get(
        "unexpected_index_column_names"
    )
    if unexpected_index_column_names:
        for column_name in unexpected_index_column_names:
            if column_name not in all_table_columns:
                raise gx_exceptions.InvalidMetricAccessorDomainKwargsKeyError(
                    message=f'Error: The unexpected_index_column: "{column_name}" in does not exist in SQL Table. '
                    f"Please check your configuration and try again."
                )

            column_selector.append(sa.column(column_name))

    for column_name in domain_column_name_list:
        column_selector.append(sa.column(column_name))

    unexpected_condition_query_with_selected_columns: sa.select = sa.select(
        column_selector
    ).where(unexpected_condition)
    source_table_and_schema: sa.Table = get_sqlalchemy_source_table_and_schema(
        execution_engine
    )

    source_table_and_schema_as_selectable: Union[
        sa.Table, sa.Select
    ] = get_sqlalchemy_selectable(source_table_and_schema)
    final_select_statement: sa.select = (
        unexpected_condition_query_with_selected_columns.select_from(
            source_table_and_schema_as_selectable
        )
    )

    query_as_string: str = sql_statement_with_post_compile_to_string(
        engine=execution_engine, select_statement=final_select_statement
    )
    return query_as_string


def _sqlalchemy_map_condition_index(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: Dict,
    metric_value_kwargs: Dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> list[dict[str, Any]] | None:
    """
    Returns indices of the metric values which do not meet an expected Expectation condition for instances
    of ColumnMapExpectation.

    Requires `unexpected_index_column_names` to be part of `result_format` dict to specify primary_key columns
    to return.
    """
    (
        unexpected_condition,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics.get("unexpected_condition")

    result_format = metric_value_kwargs["result_format"]
    if "unexpected_index_column_names" not in result_format:
        return None

    domain_column_name_list: List[str] = list()
    # column map expectations
    if "column" in accessor_domain_kwargs:
        column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]
        domain_column_name_list.append(column_name)
    # multi-column map expectations
    elif "column_list" in accessor_domain_kwargs:
        column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs[
            "column_list"
        ]
        domain_column_name_list = column_list
    # column-map expectations
    elif "column_A" in accessor_domain_kwargs and "column_B" in accessor_domain_kwargs:
        column_list: List[Union[str, quoted_name]] = list()
        column_list.append(accessor_domain_kwargs["column_A"])
        column_list.append(accessor_domain_kwargs["column_B"])
        domain_column_name_list = column_list

    domain_kwargs: dict = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    all_table_columns: List[str] = metrics.get("table.columns")

    unexpected_index_column_names: Optional[List[str]] = result_format.get(
        "unexpected_index_column_names"
    )

    column_selector: List[sa.Column] = []
    for column_name in unexpected_index_column_names:
        if column_name not in all_table_columns:
            raise gx_exceptions.InvalidMetricAccessorDomainKwargsKeyError(
                message=f'Error: The unexpected_index_column: "{column_name}" in does not exist in SQL Table. '
                f"Please check your configuration and try again."
            )
        column_selector.append(sa.column(column_name))

    # the last column we SELECT is the column the Expectation is being run on
    for column_name in domain_column_name_list:
        column_selector.append(sa.column(column_name))

    domain_records_as_selectable: sa.sql.Selectable = (
        execution_engine.get_domain_records(domain_kwargs=domain_kwargs)
    )
    unexpected_condition_query_with_selected_columns: sa.select = sa.select(
        column_selector
    ).where(unexpected_condition)

    if not MapMetricProvider.is_sqlalchemy_metric_selectable(map_metric_provider=cls):
        domain_records_as_selectable: Union[
            sa.Table, sa.Select
        ] = get_sqlalchemy_selectable(domain_records_as_selectable)

    # since SQL tables can be **very** large, truncate query_result values at 20, or at `partial_unexpected_count`
    final_query: sa.select = (
        unexpected_condition_query_with_selected_columns.select_from(
            domain_records_as_selectable
        ).limit(result_format["partial_unexpected_count"])
    )
    query_result: List[tuple] = execution_engine.engine.execute(final_query).fetchall()

    unexpected_index_list: Optional[List[Dict[str, Any]]] = []

    for row in query_result:
        primary_key_dict: Dict[str, Any] = {}
        # add the actual unexpected value
        all_columns = unexpected_index_column_names + domain_column_name_list
        for index in range(len(all_columns)):
            name: str = all_columns[index]
            primary_key_dict[name] = row[index]
        unexpected_index_list.append(primary_key_dict)

    return unexpected_index_list
