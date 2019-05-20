from .base_source import DataSource
from ..dataset.sqlalchemy_dataset import SqlAlchemyDataset
from ..dbt_tools import DBTTools

from sqlalchemy import create_engine, MetaData


class SqlAlchemyDataSource(DataSource):
    """
    A SqlAlchemyDataContext creates a SQLAlchemy engine and provides a list of tables available in the list_datasets
    method. Its get_dataset method returns a new SqlAlchemy dataset with the provided name.

    Warning: this feature is new in v0.4 and may change based on community feedback.
    """

    def __init__(self, *args, **kwargs):
        super(SqlAlchemyDataSource, self).__init__(*args, **kwargs)
        self.meta = MetaData()

    def connect(self, options, *args, **kwargs):
        self.engine = create_engine(options, *args, **kwargs)

    def list_data_assets(self):
        self.meta.reflect(bind=self.engine)
        tables = [str(table) for table in self.meta.sorted_tables]
        return tables

    def get_data_asset(self, data_asset_name, custom_sql=None, schema=None, data_context=None):
        return SqlAlchemyDataset(table_name=data_asset_name, engine=self.engine, custom_sql=custom_sql, schema=schema, data_context=data_context)


class DBTDataSource(SqlAlchemyDataSource):
    def __init__(self, profile, *args, **kwargs):
        super(DBTDataSource, self).__init__(*args, **kwargs)
        self._dbt_tools = DBTTools(profile)
        options = self._dbt_tools.get_sqlalchemy_connection_options()
        self.connect(options)

    def get_data_asset(self, data_asset_name, data_context=None):
        custom_sql = self._dbt_tools.get_model_compiled_sql(data_asset_name)
        return SqlAlchemyDataset(engine=self.engine, custom_sql=custom_sql, data_context=data_context)