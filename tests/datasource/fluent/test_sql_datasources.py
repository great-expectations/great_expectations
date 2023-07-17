from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

import pytest

from great_expectations.compatibility import sqlalchemy
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.datasource.fluent import SQLDatasource
from great_expectations.datasource.fluent.sql_datasource import (
    _verify_table_name_exists_and_get_normalized_typed_name_map,
)

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


@pytest.mark.unit
def test_kwargs_are_passed_to_create_engine(mocker: MockerFixture):
    create_engine_spy = mocker.spy(sa, "create_engine")

    ds = SQLDatasource(
        name="my_datasource",
        connection_string="sqlite:///",
        kwargs={"isolation_level": "SERIALIZABLE"},
    )
    print(ds)
    ds.test_connection()

    create_engine_spy.assert_called_once_with(
        "sqlite:///", **{"isolation_level": "SERIALIZABLE"}
    )


@pytest.mark.unit
def test_table_quoted_name_type_does_not_exist():
    """
    DBMS entity names (table, column, etc.) must adhere to correct case insensitivity standards.  All upper case is
    standard for Oracle, DB2, and Snowflake, while all lowercase is standard for SQLAlchemy; hence, proper conversion to
    quoted names must occur.  This test ensures that mechanism for detection of non-existent table_nam" works correctly.
    """
    table_names_in_dbms_schema: list[str | sqlalchemy.quoted_name] = [
        "table_name_0",
        "table_name_1",
        "table_name_2",
        "table_name_3",
    ]

    with pytest.raises(ValueError) as eee:
        _ = _verify_table_name_exists_and_get_normalized_typed_name_map(
            name="nonexistent_table_name",
            typed_names=table_names_in_dbms_schema,
        )
        assert (
            str(eee.value)
            == 'Error: The table "nonexistent_table_name" does not exist.'
        )


@pytest.mark.unit
def test_table_quoted_name_type_all_upper_case_normalizion_is_noop():
    """
    DBMS entity names (table, column, etc.) must adhere to correct case insensitivity standards.  All upper case is
    standard for Oracle, DB2, and Snowflake, while all lowercase is standard for SQLAlchemy; hence, proper conversion to
    quoted names must occur.  This test ensures that all upper case entity usage does not undergo any conversion.
    """
    table_names_in_dbms_schema: list[str | sqlalchemy.quoted_name] = [
        "ACTORS",
        "ARTISTS",
        "ATHLETES",
        "BUSINESS_PEOPLE",
        "HEALTHCARE_WORKERS",
        "ENGINEERS",
        "LAWYERS",
        "MUSICIANS",
        "SCIENTISTS",
        "LITERARY_PROFESSIONALS",
    ]

    name: str
    normalized_table_name_mappings: Sequence[
        tuple[str, str | sqlalchemy.quoted_name] | None
    ] = [
        _verify_table_name_exists_and_get_normalized_typed_name_map(
            name=name,
            typed_names=table_names_in_dbms_schema,
        )
        for name in table_names_in_dbms_schema
    ]

    normalized_table_name_mapping: tuple[str, str | sqlalchemy.quoted_name] | None
    for normalized_table_name_mapping in enumerate(normalized_table_name_mappings):
        assert normalized_table_name_mapping[0] == normalized_table_name_mapping[1]


@pytest.mark.unit
def test_table_quoted_name_type_all_lower_case_normalizion_full():
    """
    DBMS entity names (table, column, etc.) must adhere to correct case insensitivity standards.  All upper case is
    standard for Oracle, DB2, and Snowflake, while all lowercase is standard for SQLAlchemy; hence, proper conversion to
    quoted names must occur.  This test ensures that all lower case entity usage undergo conversion to quoted literals.
    """
    table_names_in_dbms_schema: list[str | sqlalchemy.quoted_name] = [
        "actors",
        "artists",
        "athletes",
        "business_people",
        "healthcare_workers",
        "engineers",
        "lawyers",
        "musicians",
        "scientists",
        "literary_professionals",
    ]

    name: str

    quoted_table_names: list[sqlalchemy.quoted_name] = [
        sqlalchemy.quoted_name(value="actors", quote=True),
        sqlalchemy.quoted_name(value="artists", quote=True),
        sqlalchemy.quoted_name(value="athletes", quote=True),
        sqlalchemy.quoted_name(value="business_people", quote=True),
        sqlalchemy.quoted_name(value="healthcare_workers", quote=True),
        sqlalchemy.quoted_name(value="engineers", quote=True),
        sqlalchemy.quoted_name(value="lawyers", quote=True),
        sqlalchemy.quoted_name(value="musicians", quote=True),
        sqlalchemy.quoted_name(value="scientists", quote=True),
        sqlalchemy.quoted_name(value="literary_professionals", quote=True),
    ]

    normalized_table_name_mappings: Sequence[
        tuple[str, str | sqlalchemy.quoted_name] | None
    ] = [
        _verify_table_name_exists_and_get_normalized_typed_name_map(
            name=name,
            typed_names=quoted_table_names,
        )
        for name in table_names_in_dbms_schema
    ]

    normalized_table_name_mapping: tuple[str, str | sqlalchemy.quoted_name] | None
    for normalized_table_name_mapping in enumerate(normalized_table_name_mappings):
        assert (
            isinstance(normalized_table_name_mapping[1], sqlalchemy.quoted_name)
            and normalized_table_name_mapping[1].quote is True
            and normalized_table_name_mapping[0] == normalized_table_name_mapping[1]
        )


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
