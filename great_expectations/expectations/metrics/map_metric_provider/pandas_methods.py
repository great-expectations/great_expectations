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

import numpy as np
import pandas as pd

import great_expectations.exceptions as gx_exceptions

if TYPE_CHECKING:
    from great_expectations.execution_engine import PandasExecutionEngine
    from great_expectations.expectations.metrics.import_manager import quoted_name
from great_expectations.expectations.metrics.util import (
    compute_unexpected_pandas_indices,
    get_dbms_compatible_column_names,
    verify_column_names_exist,
)

logger = logging.getLogger(__name__)


def _pandas_map_condition_unexpected_count(
    cls,
    execution_engine: PandasExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Returns unexpected count for MapExpectations"""
    return np.count_nonzero(metrics["unexpected_condition"][0])


def _pandas_column_map_condition_values(
    cls,
    execution_engine: PandasExecutionEngine,
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
    df = execution_engine.get_domain_records(domain_kwargs=compute_domain_kwargs)

    if "column" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column" found in provided metric_domain_kwargs, but it is required for a column map metric
(_pandas_column_map_condition_values).
"""
        )

    column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]

    column_name = get_dbms_compatible_column_names(
        column_names=column_name,
        batch_columns_list=metrics["table.columns"],
    )

    ###
    # NOTE: 20201111 - JPC - in the map_series / map_condition_series world (pandas), we
    # currently handle filter_column_isnull differently than other map_fn / map_condition
    # cases.
    ###
    filter_column_isnull = kwargs.get(
        "filter_column_isnull", getattr(cls, "filter_column_isnull", False)
    )
    if filter_column_isnull:
        df = df[df[column_name].notnull()]

    domain_values = df[column_name]

    domain_values = domain_values[
        boolean_mapped_unexpected_values == True  # noqa: E712
    ]

    result_format = metric_value_kwargs["result_format"]

    if result_format["result_format"] == "COMPLETE":
        return list(domain_values)

    return list(domain_values[: result_format["partial_unexpected_count"]])


def _pandas_column_pair_map_condition_values(
    cls,
    execution_engine: PandasExecutionEngine,
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
    df = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    if not ("column_A" in domain_kwargs and "column_B" in domain_kwargs):
        raise ValueError(
            """No "column_A" and "column_B" found in provided metric_domain_kwargs, but it is required for a column pair map metric
(_pandas_column_pair_map_condition_values).
"""
        )

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

    domain_values = df[column_names]

    domain_values = domain_values[
        boolean_mapped_unexpected_values == True  # noqa: E712
    ]

    result_format = metric_value_kwargs["result_format"]

    unexpected_list = [
        value_pair
        for value_pair in zip(
            domain_values[column_A_name].values, domain_values[column_B_name].values
        )
    ]
    if result_format["result_format"] == "COMPLETE":
        return unexpected_list

    return unexpected_list[: result_format["partial_unexpected_count"]]


def _pandas_column_pair_map_condition_filtered_row_count(
    cls,
    execution_engine: PandasExecutionEngine,
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
    df = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    if not ("column_A" in domain_kwargs and "column_B" in domain_kwargs):
        raise ValueError(
            """No "column_A" and "column_B" found in provided metric_domain_kwargs, but it is required for a column pair map metric
(_pandas_column_pair_map_condition_filtered_row_count).
"""
        )

    # noinspection PyPep8Naming
    column_A_name = accessor_domain_kwargs["column_A"]
    # noinspection PyPep8Naming
    column_B_name = accessor_domain_kwargs["column_B"]

    column_names: List[Union[str, quoted_name]] = [column_A_name, column_B_name]
    verify_column_names_exist(
        column_names=column_names, batch_columns_list=metrics["table.columns"]
    )

    return df.shape[0]


def _pandas_multicolumn_map_condition_values(
    cls,
    execution_engine: PandasExecutionEngine,
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
    df = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    if "column_list" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column_list" found in provided metric_domain_kwargs, but it is required for a multicolumn map metric
(_pandas_multicolumn_map_condition_values).
"""
        )

    column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs["column_list"]

    column_list = get_dbms_compatible_column_names(
        column_names=column_list,
        batch_columns_list=metrics["table.columns"],
    )

    domain_values = df[column_list]

    domain_values = domain_values[
        boolean_mapped_unexpected_values == True  # noqa: E712
    ]

    result_format = metric_value_kwargs["result_format"]

    if result_format["result_format"] == "COMPLETE":
        return domain_values.to_dict("records")

    return domain_values[: result_format["partial_unexpected_count"]].to_dict("records")


def _pandas_multicolumn_map_condition_filtered_row_count(
    cls,
    execution_engine: PandasExecutionEngine,
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
    df = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    if "column_list" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column_list" found in provided metric_domain_kwargs, but it is required for a multicolumn map metric
(_pandas_multicolumn_map_condition_filtered_row_count).
"""
        )

    column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs["column_list"]
    verify_column_names_exist(
        column_names=column_list, batch_columns_list=metrics["table.columns"]
    )

    return df.shape[0]


# TODO: <Alex>11/15/2022: Please DO_NOT_DELETE this method (even though it is not currently utilized).  Thanks.</Alex>
def _pandas_column_map_series_and_domain_values(
    cls,
    execution_engine: PandasExecutionEngine,
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
    (
        map_series,
        compute_domain_kwargs_2,
        accessor_domain_kwargs_2,
    ) = metrics["metric_partial_fn"]
    assert (
        compute_domain_kwargs == compute_domain_kwargs_2
    ), "map_series and condition must have the same compute domain"
    assert (
        accessor_domain_kwargs == accessor_domain_kwargs_2
    ), "map_series and condition must have the same accessor kwargs"
    df = execution_engine.get_domain_records(domain_kwargs=compute_domain_kwargs)

    if "column" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column" found in provided metric_domain_kwargs, but it is required for a column map metric
(_pandas_column_map_series_and_domain_values).
"""
        )

    column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]

    column_name = get_dbms_compatible_column_names(
        column_names=column_name,
        batch_columns_list=metrics["table.columns"],
    )

    ###
    # NOTE: 20201111 - JPC - in the map_series / map_condition_series world (pandas), we
    # currently handle filter_column_isnull differently than other map_fn / map_condition
    # cases.
    ###
    filter_column_isnull = kwargs.get(
        "filter_column_isnull", getattr(cls, "filter_column_isnull", False)
    )
    if filter_column_isnull:
        df = df[df[column_name].notnull()]

    domain_values = df[column_name]

    domain_values = domain_values[
        boolean_mapped_unexpected_values == True  # noqa: E712
    ]
    map_series = map_series[boolean_mapped_unexpected_values == True]  # noqa: E712

    result_format = metric_value_kwargs["result_format"]

    if result_format["result_format"] == "COMPLETE":
        return (
            list(domain_values),
            list(map_series),
        )

    return (
        list(domain_values[: result_format["partial_unexpected_count"]]),
        list(map_series[: result_format["partial_unexpected_count"]]),
    )


def _pandas_map_condition_index(
    cls,
    execution_engine: PandasExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> Union[List[int], List[Dict[str, Any]]]:
    (
        boolean_mapped_unexpected_values,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics.get("unexpected_condition")
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    domain_records_df: pd.DataFrame = execution_engine.get_domain_records(
        domain_kwargs=domain_kwargs
    )
    domain_column_name_list: List[str] = list()
    # column map expectations
    if "column" in accessor_domain_kwargs:
        column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]

        column_name = get_dbms_compatible_column_names(
            column_names=column_name,
            batch_columns_list=metrics["table.columns"],
        )

        ###
        # NOTE: 20201111 - JPC - in the map_series / map_condition_series world (pandas), we
        # currently handle filter_column_isnull differently than other map_fn / map_condition
        # cases.
        ###
        filter_column_isnull = kwargs.get(
            "filter_column_isnull", getattr(cls, "filter_column_isnull", False)
        )
        if filter_column_isnull:
            domain_records_df = domain_records_df[
                domain_records_df[column_name].notnull()
            ]
        domain_column_name_list.append(column_name)

    # multi-column map expectations
    elif "column_list" in accessor_domain_kwargs:
        column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs[
            "column_list"
        ]
        verify_column_names_exist(
            column_names=column_list, batch_columns_list=metrics["table.columns"]
        )
        domain_column_name_list = column_list

    # column pair expectations
    elif "column_A" in accessor_domain_kwargs and "column_B" in accessor_domain_kwargs:
        column_list: List[Union[str, quoted_name]] = list()
        column_list.append(accessor_domain_kwargs["column_A"])
        column_list.append(accessor_domain_kwargs["column_B"])
        verify_column_names_exist(
            column_names=column_list, batch_columns_list=metrics["table.columns"]
        )
        domain_column_name_list = column_list

    result_format = metric_value_kwargs["result_format"]
    domain_records_df = domain_records_df[boolean_mapped_unexpected_values]

    unexpected_index_list: Union[
        List[int], List[Dict[str, Any]]
    ] = compute_unexpected_pandas_indices(
        domain_records_df=domain_records_df,
        result_format=result_format,
        execution_engine=execution_engine,
        metrics=metrics,
        expectation_domain_column_list=domain_column_name_list,
    )
    if result_format["result_format"] == "COMPLETE":
        return unexpected_index_list
    return unexpected_index_list[: result_format["partial_unexpected_count"]]


def _pandas_map_condition_query(
    cls,
    execution_engine: PandasExecutionEngine,
    metric_domain_kwargs: Dict,
    metric_value_kwargs: Dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> Optional[List[Any]]:
    """
    Returns query that will return all rows which do not meet an expected Expectation condition for instances
    of ColumnMapExpectation. For Pandas, this is currently the full set of unexpected_indices.

    Requires `unexpected_index_column_names` to be part of `result_format` dict to specify primary_key columns
    to return, along with column the Expectation is run on.
    """
    result_format: dict = metric_value_kwargs["result_format"]

    # We will not return map_condition_query if return_unexpected_index_query = False
    return_unexpected_index_query: bool = result_format.get(
        "return_unexpected_index_query"
    )
    if return_unexpected_index_query is False:
        return

    (
        boolean_mapped_unexpected_values,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics.get("unexpected_condition")
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    domain_records_df: pd.DataFrame = execution_engine.get_domain_records(
        domain_kwargs=domain_kwargs
    )
    if "column" in accessor_domain_kwargs:
        column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]

        column_name = get_dbms_compatible_column_names(
            column_names=column_name,
            batch_columns_list=metrics["table.columns"],
        )
        filter_column_isnull = kwargs.get(
            "filter_column_isnull", getattr(cls, "filter_column_isnull", False)
        )
        if filter_column_isnull:
            domain_records_df = domain_records_df[
                domain_records_df[column_name].notnull()
            ]

    elif "column_list" in accessor_domain_kwargs:
        column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs[
            "column_list"
        ]
        verify_column_names_exist(
            column_names=column_list, batch_columns_list=metrics["table.columns"]
        )
    domain_values_df_filtered = domain_records_df[boolean_mapped_unexpected_values]
    return domain_values_df_filtered.index.to_list()


def _pandas_column_map_condition_value_counts(
    cls,
    execution_engine: PandasExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Returns respective value counts for distinct column values"""
    (
        boolean_mapped_unexpected_values,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics.get("unexpected_condition")
    df = execution_engine.get_domain_records(domain_kwargs=compute_domain_kwargs)

    column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]

    if "column" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column" found in provided metric_domain_kwargs, but it is required for a column map metric
(_pandas_column_map_condition_value_counts).
"""
        )

    column_name = get_dbms_compatible_column_names(
        column_names=column_name,
        batch_columns_list=metrics["table.columns"],
    )

    ###
    # NOTE: 20201111 - JPC - in the map_series / map_condition_series world (pandas), we
    # currently handle filter_column_isnull differently than other map_fn / map_condition
    # cases.
    ###
    filter_column_isnull = kwargs.get(
        "filter_column_isnull", getattr(cls, "filter_column_isnull", False)
    )
    if filter_column_isnull:
        df = df[df[column_name].notnull()]

    domain_values = df[column_name]

    result_format = metric_value_kwargs["result_format"]
    value_counts = None
    try:
        value_counts = domain_values[boolean_mapped_unexpected_values].value_counts()
    except ValueError:
        try:
            value_counts = (
                domain_values[boolean_mapped_unexpected_values]
                .apply(tuple)
                .value_counts()
            )
        except ValueError:
            pass

    if not value_counts:
        raise gx_exceptions.MetricComputationError("Unable to compute value counts")

    if result_format["result_format"] == "COMPLETE":
        return value_counts

    return value_counts[result_format["partial_unexpected_count"]]


def _pandas_map_condition_rows(
    cls,
    execution_engine: PandasExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Return values from the specified domain (ignoring the column constraint) that match the map-style metric in the metrics dictionary."""
    (
        boolean_mapped_unexpected_values,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics.get("unexpected_condition")
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    df = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    if "column" in accessor_domain_kwargs:
        column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]

        column_name = get_dbms_compatible_column_names(
            column_names=column_name,
            batch_columns_list=metrics["table.columns"],
        )

        ###
        # NOTE: 20201111 - JPC - in the map_series / map_condition_series world (pandas), we
        # currently handle filter_column_isnull differently than other map_fn / map_condition
        # cases.
        ###
        filter_column_isnull = kwargs.get(
            "filter_column_isnull", getattr(cls, "filter_column_isnull", False)
        )
        if filter_column_isnull:
            df = df[df[column_name].notnull()]

    elif "column_list" in accessor_domain_kwargs:
        column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs[
            "column_list"
        ]
        verify_column_names_exist(
            column_names=column_list, batch_columns_list=metrics["table.columns"]
        )

    result_format = metric_value_kwargs["result_format"]

    df = df[boolean_mapped_unexpected_values]

    if result_format["result_format"] == "COMPLETE":
        return df

    return df.iloc[: result_format["partial_unexpected_count"]]
