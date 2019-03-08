from .base import DataContext
from ..dataset.sqlalchemy_dataset import SqlAlchemyDataset

from sqlalchemy import create_engine, MetaData


class SqlAlchemyDataContext(DataContext):
    """
    A SqlAlchemyDataContext creates a SQLAlchemy engine and provides a list of tables available in the list_datasets
    method. Its get_dataset method returns a new SqlAlchemy dataset with the provided name.

    Warning: this feature is new in v0.4 and may change based on community feedback.
    """

    def __init__(self, *args, **kwargs):
        super(SqlAlchemyDataContext, self).__init__(*args, **kwargs)
        self.meta = MetaData()

    def connect(self, options, *args, **kwargs):
        self.engine = create_engine(options, *args, **kwargs)

    def list_datasets(self):
        self.meta.reflect(bind=self.engine)
        tables = [str(table) for table in self.meta.sorted_tables]
        return tables

    def get_dataset(self, dataset_name, custom_sql=None, schema=None):
        return SqlAlchemyDataset(table_name=dataset_name, engine=self.engine, custom_sql=custom_sql, schema=schema)
