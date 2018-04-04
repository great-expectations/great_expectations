import pytest

from great_expectations.dataset import MetaSqlAlchemyDataset, SqlAlchemyDataset
import sqlalchemy as sa
import pandas as pd


@pytest.fixture
def custom_dataset():
    class CustomSqlAlchemyDataset(SqlAlchemyDataset):

        @MetaSqlAlchemyDataset.column_map_expectation
        def expect_column_values_to_equal_2(self, column):
            return (sa.column(column) == 2)

        @MetaSqlAlchemyDataset.column_aggregate_expectation
        def expect_column_mode_to_equal_0(self, column):
            mode_query = sa.select([
                sa.column(column).label('value'),
                sa.func.count(sa.column(column)).label('frequency')
            ]).select_from(sa.table(self.table_name)).group_by(sa.column(column)).order_by(
                sa.desc(sa.column('frequency')))

            mode = self.engine.execute(mode_query).scalar()
            return {
                "success": mode == 0,
                "result": {
                    "observed_value": mode,
                }
            }

        @MetaSqlAlchemyDataset.column_aggregate_expectation
        def broken_aggregate_expectation(self, column):
            return {
                "not_a_success_value": True,
            }

        @MetaSqlAlchemyDataset.column_aggregate_expectation
        def another_broken_aggregate_expectation(self, column):
            return {
                "success": True,
                "result": {
                    "no_observed_value": True
                }
            }

    engine = sa.create_engine('sqlite://')

    data = pd.DataFrame({
        "c1": [2, 2, 2, 2, 0],
        "c2": [4, 4, 5, None, 7],
        "c3": ["cat", "dog", "fish", "tiger", "elephant"]

    })

    data.to_sql(name='test_data', con=engine, index=False)
    custom_dataset = CustomSqlAlchemyDataset('test_data', engine=engine)

    return custom_dataset


def test_custom_sqlalchemydataset(custom_dataset):
    custom_dataset._initialize_expectations()
    custom_dataset.set_default_expectation_argument("result_format", {"result_format": "COMPLETE"})

    result = custom_dataset.expect_column_values_to_equal_2('c1')
    assert result['success'] == False
    assert result['result']['unexpected_list'] == [0]

    result = custom_dataset.expect_column_mode_to_equal_0('c2')
    assert result['success'] == False
    assert result['result']['observed_value'] == 4


def test_broken_decorator_errors(custom_dataset):
    custom_dataset._initialize_expectations()
    custom_dataset.set_default_expectation_argument("result_format", {"result_format": "COMPLETE"})

    with pytest.raises(ValueError) as err:
        custom_dataset.broken_aggregate_expectation('c1')
        assert "Column aggregate expectation failed to return required information: success" in str(err)

    with pytest.raises(ValueError) as err:
        custom_dataset.another_broken_aggregate_expectation('c1')
        assert "Column aggregate expectation failed to return required information: observed_value" in str(err)


def test_sqlalchemydataset_with_custom_sql():
    engine = sa.create_engine('sqlite://')

    data = pd.DataFrame({
        "name": ["Frank", "Steve", "Jane", "Frank", "Michael"],
        "age": [16, 21, 38, 22, 10],
        "pet": ["fish", "python", "cat", "python", "frog"]
    })

    data.to_sql(name='test_sql_data', con=engine, index=False)

    custom_sql = "SELECT name, pet FROM test_sql_data WHERE age > 12"
    custom_sql_dataset = SqlAlchemyDataset('test_sql_data', engine=engine, custom_sql=custom_sql)

    custom_sql_dataset._initialize_expectations()
    custom_sql_dataset.set_default_expectation_argument("result_format", {"result_format": "COMPLETE"})

    result = custom_sql_dataset.expect_column_values_to_be_in_set("pet", ["fish", "cat", "python"])
    assert result['success'] == True

    result = custom_sql_dataset.expect_column_to_exist("age")
    assert result['success'] == False
