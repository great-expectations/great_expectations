from great_expectations.dataset import MetaSqlAlchemyDataSet, SqlAlchemyDataSet
import sqlalchemy as sa
import pandas as pd

class CustomSqlAlchemyDataSet(SqlAlchemyDataSet):

    @MetaSqlAlchemyDataSet.column_map_expectation
    def expect_column_values_to_equal_2(self, column):
        return (sa.column(column) == 2)

    @MetaSqlAlchemyDataSet.column_aggregate_expectation
    def expect_column_mode_to_equal_0(self, column):
        mode_query = sa.select([
            sa.column(column).label('value'),
            sa.func.count(sa.column(column)).label('frequency')
        ]).select_from(sa.table(self.table_name)).group_by(sa.column(column)).order_by(sa.desc(sa.column('frequency')))

        mode = self.engine.execute(mode_query).scalar()
        return {
            "success": mode == 0,
            "result_obj": {
                "observed_value": mode,
            }
        }


def test_custom_sqlalchemydataset():
    engine = sa.create_engine('sqlite://')

    data = pd.DataFrame({
        "c1": [2, 2, 2, 2, 0],
        "c2": [4, 4, 5, None, 7]
    })

    data.to_sql(name='test_data', con=engine, index=False)

    custom_dataset = CustomSqlAlchemyDataSet(engine, 'test_data')
    custom_dataset.set_default_expectation_argument("result_format", "COMPLETE")

    result = custom_dataset.expect_column_values_to_equal_2('c1')
    assert result['success'] == False
    assert result['result_obj']['unexpected_list'] == [0]

    result = custom_dataset.expect_column_mode_to_equal_0('c2')
    assert result['success'] == False
    assert result['result_obj']['observed_value'] == 4