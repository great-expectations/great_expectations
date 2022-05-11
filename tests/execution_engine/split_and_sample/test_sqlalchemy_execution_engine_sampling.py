from unittest import mock

import pytest

from great_expectations.core.id_dict import BatchSpec
from great_expectations.execution_engine.split_and_sample.sqlalchemy_data_sampler import (
    SqlAlchemyDataSampler,
)
from great_expectations.execution_engine.sqlalchemy_dialect import GESqlDialect


@pytest.mark.parametrize(
    "underscore_prefix",
    [
        pytest.param("_", id="underscore prefix"),
        pytest.param("", id="no underscore prefix"),
    ],
)
@pytest.mark.parametrize(
    "sampler_method_name",
    [
        pytest.param(sampler_method_name, id=sampler_method_name)
        for sampler_method_name in [
            "sample_using_limit",
            "sample_using_random",
            "sample_using_mod",
            "sample_using_a_list",
            "sample_using_md5",
        ]
    ],
)
def test_get_sampler_method(underscore_prefix: str, sampler_method_name: str):
    """What does this test and why?

    This test is to ensure that the sampler methods are accessible with and without underscores.
    When new sampling methods are added, the parameter list should be updated.
    """
    data_splitter: SqlAlchemyDataSampler = SqlAlchemyDataSampler()

    sampler_method_name_with_prefix = f"{underscore_prefix}{sampler_method_name}"

    assert data_splitter.get_sampler_method(sampler_method_name_with_prefix) == getattr(
        data_splitter, sampler_method_name
    )


def clean_query_for_comparison(query_string: str) -> str:
    """Remove whitespace and case from query for easy comparison.

    Args:
        query_string: query string to convert.

    Returns:
        String with removed whitespace and converted to lowercase.
    """
    """Remove """
    return query_string.replace("\n", "").replace("\t", "").replace(" ", "").lower()


@pytest.mark.parametrize(
    "dialect",
    [
        pytest.param(dialect, id=dialect)
        for dialect in ["postgresql"]  # GESqlDialect.get_all_dialect_names()
    ],
)
@mock.patch("great_expectations.execution_engine.execution_engine.ExecutionEngine")
def test_sample_using_limit(mock_execution_engine: mock.MagicMock, dialect: str):
    """What does this test and why?

    split_on_limit should build the appropriate query based on input parameters.
    """

    table_name: str = "test_table"
    batch_spec: BatchSpec = BatchSpec(
        table_name=table_name,
        schema_name="test_schema_name",
        sampling_method="sample_using_limit",
        sampling_kwargs={"n": 10},
    )

    # TODO: AJB 20220510 get dialect based on dialect name tested
    from sqlalchemy.dialects import postgresql

    data_sampler: SqlAlchemyDataSampler = SqlAlchemyDataSampler(
        # dialect=sqlalchemy_psycopg2,
        dialect=postgresql.dialect(),
        dialect_name=GESqlDialect(dialect),
    )

    result = data_sampler.sample_using_limit(
        execution_engine=mock_execution_engine, batch_spec=batch_spec, where_clause=None
    )

    print("result:", result)
    if not isinstance(result, str):
        query_str: str = clean_query_for_comparison(
            str(result.compile(compile_kwargs={"literal_binds": True}))
        )
        print("query_str:", query_str)
    else:
        query_str: str = result

    expected: str = clean_query_for_comparison(
        "SELECT * FROM TEST_SCHEMA_NAME.TEST_TABLE WHERE TRUE LIMIT 10"
    )

    assert query_str == expected
