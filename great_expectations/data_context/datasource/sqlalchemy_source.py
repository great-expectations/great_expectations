import datetime

import sqlalchemy
from sqlalchemy import create_engine, MetaData

from .datasource import Datasource
from .batch_generator import BatchGenerator
from ...dataset.sqlalchemy_dataset import SqlAlchemyDataset

class QueryGenerator(BatchGenerator):
    """
    """

    def __init__(self, name, type_, datasource):
        super(QueryGenerator, self).__init__(name, type_, datasource)

    def _get_iterator(self, data_asset_name):
        query = self._generator_config["queries"][data_asset_name] 
        return iter([
            {
                "custom_sql": query,
                "timestamp": datetime.datetime.now().timestamp()
            }
        ])

    def add_query(self, name, **kwargs):
        self._generator_config["queries"][name] = {**kwargs}
        self._save_config()


class SqlAlchemyDatasource(Datasource):
    """
    A SqlAlchemyDataContext creates a SQLAlchemy engine and provides a list of tables available in the list_datasets
    method. Its get_dataset method returns a new SqlAlchemy dataset with the provided name.

    Warning: this feature is new in v0.4 and may change based on community feedback.
    """

    def __init__(self, name, type_, data_context, profile_name=None, **kwargs):
        super(SqlAlchemyDatasource, self).__init__(name, type_, data_context)
        if profile_name is not None:
            self._datasource_config.update({
                "profile": profile_name
            })
            credentials = data_context.get_profile_credentials(profile_name)
        else:
            credentials = {}
        
        # Update credentials with anything passed during connection time
        credentials.update({**kwargs})
        self.meta = MetaData()

        if "url" in credentials:
            options = credentials.pop("url")
        else:
            drivername = credentials.pop("drivername")
            options = sqlalchemy.engine.url.URL(drivername, **credentials)

        self._connect(options)

    def _connect(self, options, *args, **kwargs):
        self.engine = create_engine(options, *args, **kwargs)

    def _get_data_asset_generator_class(self, type_):
        if type_ == "queries":
            return QueryGenerator
        else:
            raise ValueError("Unrecognized DataAssetGenerator type %s" % type_)

    def _get_data_asset(self, data_asset_name, batch_kwargs, expectations_config):
        if "custom_sql" not in batch_kwargs:
            batch_kwargs["custom_sql"] = "SELECT * FROM %s;" % data_asset_name

        custom_sql = batch_kwargs["custom_sql"]
        # TODO: resolve table_name and data_assset_name vs custom_sql convention
        return SqlAlchemyDataset(table_name=data_asset_name, engine=self.engine, data_context=self._data_context, data_asset_name=data_asset_name, expectations_config=expectations_config, custom_sql=custom_sql)
