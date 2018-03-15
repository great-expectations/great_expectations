from .base import DataContext
from ..dataset.sqlalchemy_dataset import SqlAlchemyDataSet

from sqlalchemy import create_engine, MetaData


class SqlAlchemyDataContext(DataContext):
    """
    A SqlAlchemyDataContext creates a SQLAlchemy engine and provides a list of tables available in the list_datasets
    method. Its get_dataset method returns a new SqlAlchemy dataset with the provided name.

    Warning: this feature is new in v0.4 and may change based on community feedback.
    """

    def __init__(self, *args, **kwargs):
        super(SqlAlchemyDataContext, self).__init__(*args, **kwargs)

    def connect(self, options):
        self.engine = create_engine(options)

    def list_datasets(self):
        return MetaData.reflect(engine=self.engine).sorted_tables

    def get_dataset(self, dataset_name):
        return SqlAlchemyDataSet(table_name=dataset_name, engine=self.engine)
