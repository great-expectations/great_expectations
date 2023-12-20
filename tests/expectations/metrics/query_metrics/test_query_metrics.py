import pytest

from great_expectations.compatibility.sqlalchemy import (
    sqlalchemy as sa,
)
from great_expectations.expectations.metrics.query_metrics.query_template_values import (
    QueryTemplateValues,
)


@pytest.mark.unit
def test_query_template_get_query_function_with_str():
    """Simple test to ensure that `get_query()` method for QueryTemplateValue can handle strings."""
    query: str = """
        SELECT {column_name}
        FROM {active_batch}
        WHERE {condition}
    """
    selectable = sa.Table("gx_temp_aaa", sa.MetaData(), schema=None)
    template_dict: dict = {"column_name": "aaa", "condition": "is_open"}
    metric_ob: QueryTemplateValues = QueryTemplateValues()
    formatted_query: str = metric_ob.get_query(query, template_dict, selectable)
    assert (
        formatted_query
        == """
        SELECT aaa
        FROM gx_temp_aaa
        WHERE is_open
    """
    )


@pytest.mark.unit
def test_query_template_get_query_function_with_int():
    """Simple test to ensure that the `get_query()` method for QueryTemplateValue can handle integer value"""
    query: str = """
            SELECT {column_to_check}
            FROM {active_batch}
            WHERE {condition}
            GROUP BY {column_to_check}
            """
    selectable = sa.Table("gx_temp_aaa", sa.MetaData(), schema=None)
    template_dict: dict = {"column_to_check": 1, "condition": "is_open"}
    metric_ob: QueryTemplateValues = QueryTemplateValues()
    formatted_query: str = metric_ob.get_query(query, template_dict, selectable)
    assert (
        formatted_query
        == """
            SELECT 1
            FROM gx_temp_aaa
            WHERE is_open
            GROUP BY 1
            """
    )


@pytest.mark.unit
def test_query_template_get_query_function_with_float():
    """Simple test to ensure that the `get_query()` method for QueryTemplateValue can handle float value"""
    query: str = """
            SELECT {column_to_check}
            FROM {active_batch}
            WHERE {condition}
            GROUP BY {column_to_check}
            """
    selectable = sa.Table("gx_temp_aaa", sa.MetaData(), schema=None)
    template_dict: dict = {"column_to_check": 1.0, "condition": "is_open"}
    metric_ob: QueryTemplateValues = QueryTemplateValues()
    formatted_query: str = metric_ob.get_query(query, template_dict, selectable)
    assert (
        formatted_query
        == """
            SELECT 1.0
            FROM gx_temp_aaa
            WHERE is_open
            GROUP BY 1.0
            """
    )
