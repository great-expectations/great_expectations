try:
    from unittest import mock
except ImportError:
    from unittest import mock
import pandas as pd
import pytest

from great_expectations.dataset import MetaSqlAlchemyDataset, SqlAlchemyDataset
from great_expectations.util import is_library_loadable
from tests.test_utils import get_dataset


@pytest.fixture
def custom_dataset(sa):
    class CustomSqlAlchemyDataset(SqlAlchemyDataset):
        @MetaSqlAlchemyDataset.column_map_expectation
        def expect_column_values_to_equal_2(self, column):
            return sa.column(column) == 2

        @MetaSqlAlchemyDataset.column_aggregate_expectation
        def expect_column_mode_to_equal_0(self, column):
            mode_query = (
                sa.select(
                    [
                        sa.column(column).label("value"),
                        sa.func.count(sa.column(column)).label("frequency"),
                    ]
                )
                .select_from(self._table)
                .group_by(sa.column(column))
                .order_by(sa.desc(sa.column("frequency")))
            )

            mode = self.engine.execute(mode_query).scalar()
            return {"success": mode == 0, "result": {"observed_value": mode,}}

        @MetaSqlAlchemyDataset.column_aggregate_expectation
        def broken_aggregate_expectation(self, column):
            return {
                "not_a_success_value": True,
            }

        @MetaSqlAlchemyDataset.column_aggregate_expectation
        def another_broken_aggregate_expectation(self, column):
            return {"success": True, "result": {"no_observed_value": True}}

    engine = sa.create_engine("sqlite://")

    data = pd.DataFrame(
        {
            "c1": [2, 2, 2, 2, 0],
            "c2": [4, 4, 5, None, 7],
            "c3": ["cat", "dog", "fish", "tiger", "elephant"],
        }
    )

    data.to_sql(name="test_data", con=engine, index=False)
    custom_dataset = CustomSqlAlchemyDataset("test_data", engine=engine)

    return custom_dataset


def test_custom_sqlalchemydataset(custom_dataset):
    custom_dataset._initialize_expectations()
    custom_dataset.set_default_expectation_argument(
        "result_format", {"result_format": "COMPLETE"}
    )

    result = custom_dataset.expect_column_values_to_equal_2("c1")
    assert result.success is False
    assert result.result["unexpected_list"] == [0]

    result = custom_dataset.expect_column_mode_to_equal_0("c2")
    assert result.success is False
    assert result.result["observed_value"] == 4


def test_broken_decorator_errors(custom_dataset):
    custom_dataset._initialize_expectations()
    custom_dataset.set_default_expectation_argument(
        "result_format", {"result_format": "COMPLETE"}
    )

    with pytest.raises(ValueError) as err:
        custom_dataset.broken_aggregate_expectation("c1")
    assert (
        "Column aggregate expectation failed to return required information: success"
        in str(err.value)
    )

    with pytest.raises(ValueError) as err:
        custom_dataset.another_broken_aggregate_expectation("c1")
    assert (
        "Column aggregate expectation failed to return required information: observed_value"
        in str(err.value)
    )


def test_missing_engine_error(sa):
    with pytest.raises(ValueError) as err:
        SqlAlchemyDataset("test_engine", schema="example")
    assert "Engine or connection_string must be provided." in str(err.value)


def test_only_connection_string(titanic_sqlite_db, sa):
    conn_string = titanic_sqlite_db.url
    SqlAlchemyDataset("titanic", connection_string=conn_string)


def test_sqlalchemydataset_raises_error_on_missing_table_name(sa):
    with pytest.raises(ValueError) as ve:
        SqlAlchemyDataset(table_name=None, engine="foo", connection_string="bar")
    assert str(ve.value) == "No table_name provided."


def test_sqlalchemydataset_builds_guid_for_table_name_on_custom_sql(sa):
    engine = sa.create_engine("sqlite://")
    with mock.patch("uuid.uuid4") as mock_uuid:
        mock_uuid.return_value = "12345678-lots-more-stuff"

        dataset = SqlAlchemyDataset(engine=engine, custom_sql="select 1")
        assert dataset._table.name == "ge_tmp_12345678"


def test_sqlalchemydataset_with_custom_sql(sa):
    engine = sa.create_engine("sqlite://")

    data = pd.DataFrame(
        {
            "name": ["Frank", "Steve", "Jane", "Frank", "Michael"],
            "age": [16, 21, 38, 22, 10],
            "pet": ["fish", "python", "cat", "python", "frog"],
        }
    )

    data.to_sql(name="test_sql_data", con=engine, index=False)

    custom_sql = "SELECT name, pet FROM test_sql_data WHERE age > 12"
    custom_sql_dataset = SqlAlchemyDataset(engine=engine, custom_sql=custom_sql)

    custom_sql_dataset._initialize_expectations()
    custom_sql_dataset.set_default_expectation_argument(
        "result_format", {"result_format": "COMPLETE"}
    )

    result = custom_sql_dataset.expect_column_values_to_be_in_set(
        "pet", ["fish", "cat", "python"]
    )
    assert result.success is True

    result = custom_sql_dataset.expect_column_to_exist("age")
    assert result.success is False


def test_column(sa):
    engine = sa.create_engine("sqlite://")

    data = pd.DataFrame(
        {
            "name": ["Frank", "Steve", "Jane", "Frank", "Michael"],
            "age": [16, 21, 38, 22, 10],
            "pet": ["fish", "python", "cat", "python", "frog"],
        }
    )
    data.to_sql(name="test_sql_data", con=engine, index=False)
    dataset = SqlAlchemyDataset("test_sql_data", engine=engine)
    assert set(dataset.get_table_columns()) == {"name", "age", "pet"}

    obs = dataset.columns
    # Hacks to check instances of types
    for col in obs:
        col["type"] = str(col["type"])

    assert len(obs) == 3
    for column in [
        {
            "name": "name",
            "type": "TEXT",
            "nullable": True,
            "default": None,
            "autoincrement": "auto",
            "primary_key": 0,
        },
        {
            "name": "age",
            "type": "BIGINT",
            "nullable": True,
            "default": None,
            "autoincrement": "auto",
            "primary_key": 0,
        },
        {
            "name": "pet",
            "type": "TEXT",
            "nullable": True,
            "default": None,
            "autoincrement": "auto",
            "primary_key": 0,
        },
    ]:
        assert column in obs


def test_column_fallback(sa):
    engine = sa.create_engine("sqlite://")

    data = pd.DataFrame(
        {
            "name": ["Frank", "Steve", "Jane", "Frank", "Michael"],
            "age": [16, 21, 38, 22, 10],
            "pet": ["fish", "python", "cat", "python", "frog"],
        }
    )

    data.to_sql(name="test_sql_data", con=engine, index=False)
    dataset = SqlAlchemyDataset("test_sql_data", engine=engine)
    assert set(dataset.get_table_columns()) == {"name", "age", "pet"}

    fallback_dataset = SqlAlchemyDataset("test_sql_data", engine=engine)
    # override columns attribute to test fallback
    fallback_dataset.columns = fallback_dataset.column_reflection_fallback()
    assert set(fallback_dataset.get_table_columns()) == {"name", "age", "pet"}

    # check that the results are the same for a few expectations
    assert dataset.expect_column_to_exist(
        "age"
    ) == fallback_dataset.expect_column_to_exist("age")

    assert dataset.expect_column_mean_to_be_between(
        "age", min_value=10
    ) == fallback_dataset.expect_column_mean_to_be_between("age", min_value=10)

    # Test a failing expectation
    assert dataset.expect_table_row_count_to_equal(
        value=3
    ) == fallback_dataset.expect_table_row_count_to_equal(value=3)


@pytest.fixture
def unexpected_count_df(sa):
    return get_dataset("sqlite", {"a": [1, 2, 1, 2, 1, 2, 1, 2, 1, 2]})


def test_sqlalchemy_dataset_view(sqlite_view_engine):
    # This test demonstrates that a view can be used as a SqlAlchemyDataset table for purposes of validation
    dataset = SqlAlchemyDataset("test_view", engine=sqlite_view_engine)
    res = dataset.expect_table_row_count_to_equal(1)
    assert res.success is True

    # A temp view can also be used, though generators will not see it
    dataset = SqlAlchemyDataset("test_temp_view", engine=sqlite_view_engine)
    res = dataset.expect_table_row_count_to_equal(3)
    assert res.success is True


def test_sqlalchemy_dataset_unexpected_count_calculations(sa, unexpected_count_df):
    # The partial_unexpected_count should not affect overall success calculations, but should limit number of returned rows
    res1 = unexpected_count_df.expect_column_values_to_be_in_set(
        "a",
        value_set=[1],
        result_format={"result_format": "BASIC", "partial_unexpected_count": 2},
    )
    res2 = unexpected_count_df.expect_column_values_to_be_in_set(
        "a",
        value_set=[1],
        result_format={"result_format": "BASIC", "partial_unexpected_count": 10},
    )

    assert res1.result["unexpected_count"] == 5
    assert res2.result["unexpected_count"] == 5
    # Note difference here
    assert len(res1.result["partial_unexpected_list"]) == 2
    assert len(res2.result["partial_unexpected_list"]) == 5

    # However, the "COMPLETE" result format ignores the limit.
    res1 = unexpected_count_df.expect_column_values_to_be_in_set(
        "a",
        value_set=[1],
        result_format={"result_format": "COMPLETE", "partial_unexpected_count": 2},
    )
    res2 = unexpected_count_df.expect_column_values_to_be_in_set(
        "a",
        value_set=[1],
        result_format={"result_format": "COMPLETE", "partial_unexpected_count": 10},
    )

    assert res1.result["unexpected_count"] == 5
    assert res2.result["unexpected_count"] == 5


def test_result_format_warning(sa, unexpected_count_df):
    with pytest.warns(
        UserWarning,
        match=r"Setting result format to COMPLETE for a SqlAlchemyDataset can be dangerous",
    ):
        unexpected_count_df.expect_column_values_to_be_in_set(
            "a",
            value_set=[1],
            result_format={"result_format": "COMPLETE", "partial_unexpected_count": 2},
        )


@pytest.mark.skipif(
    is_library_loadable(library_name="sqlalchemy_redshift"),
    reason="sqlalchemy_redshift must not be installed",
)
def test_dataset_attempt_allowing_relative_error_when_redshift_library_not_installed(
    sa,
):
    engine = sa.create_engine("sqlite://")
    dataset = SqlAlchemyDataset(engine=engine, custom_sql="select 1")

    assert isinstance(dataset, SqlAlchemyDataset)
    assert dataset.attempt_allowing_relative_error() is False


def test_expect_compound_columns_to_be_unique(sa):
    engine = sa.create_engine("sqlite://")

    data = pd.DataFrame(
        {
            "col1": [1, 2, 3, 1, 2, 3, None, None],
            "col2": [1, 2, 2, 2, 2, 3, None, None],
            "col3": [1, 1, 2, 2, 3, 2, None, None],
            "col4": [1, None, 2, 2, None, None, None, None],
        }
    )

    data.to_sql(name="test_sql_data", con=engine, index=False)
    dataset = SqlAlchemyDataset("test_sql_data", engine=engine)

    assert not dataset.expect_compound_columns_to_be_unique(["col1", "col2"]).success
    assert not dataset.expect_compound_columns_to_be_unique(["col2", "col3"]).success
    assert not dataset.expect_compound_columns_to_be_unique(["col1", "col3"]).success
    assert dataset.expect_compound_columns_to_be_unique(
        ["col1", "col2", "col3"]
    ).success
    assert not dataset.expect_compound_columns_to_be_unique(
        ["col1", "col2", "col4"], ignore_row_if="any_value_is_missing",
    ).success
    assert dataset.expect_compound_columns_to_be_unique(
        ["col1", "col2", "col4"]
    ).success
