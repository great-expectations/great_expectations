from .base_source import DataSource
from ..dataset.sqlalchemy_dataset import SqlAlchemyDataset
from ..dbt_tools import DBTTools

from sqlalchemy import create_engine, MetaData


class DBTDataSource(DataSource):
    """
    A DBTDataSource create a SQLAlchemy connection to the database used by a dbt project
    and allows to create, manage and validate expectations on the models that exist in that dbt project.
    """

    def __init__(self, profile, *args, **kwargs):
        super(DBTDataSource, self).__init__(*args, **kwargs)
        self.meta = MetaData()
        self._dbt_tools = DBTTools(profile)
        options = self._dbt_tools.get_sqlalchemy_connection_options()
        self._connect(options)

    def _connect(self, options, *args, **kwargs):
        self.engine = create_engine(options, *args, **kwargs)

    def get_data_asset(self, data_asset_name, data_context=None):
        """
        Get a data asset object that will allow to create, manage and validate expectations on a dbt model.

        Args:
            data_asset_name (string): \
                Name of an existing dbt model.
                If your model sql file is models/myfolder1/my_model1.sql, pass "myfolder1/my_model1".

        Notes:
            This method will read the compiled SQL for this model from dbt's "compiled" folder - make sure that
            it is up to date after modifying the model's SQL source - recompile or rerun your dbt pipeline
        """
        custom_sql = self._dbt_tools.get_model_compiled_sql(data_asset_name)
        return SqlAlchemyDataset(engine=self.engine, custom_sql=custom_sql, data_context=data_context, data_asset_name=data_asset_name)