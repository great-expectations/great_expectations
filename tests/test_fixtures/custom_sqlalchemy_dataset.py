import sqlalchemy as sa

from great_expectations.dataset import Dataset, SqlAlchemyDataset


class CustomSqlAlchemyDataset(SqlAlchemyDataset):
    _data_asset_type = "CustomSqlAlchemyDataset"

    @Dataset.column_aggregate_expectation
    def expect_column_func_value_to_be(self, column, value):
        query = sa.select([sa.func.min(sa.column(column))]).select_from(self._table)
        func_value = self.engine.execute(query).fetchone()[0]

        if value is None:
            success = True
        else:
            success = func_value == value

        return {"success": success, "result": {"observed_value": func_value}}
