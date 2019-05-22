from .base_source import DataSource
from ..dataset.sqlalchemy_dataset import SqlAlchemyDataset

import os
import yaml
import sqlalchemy
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

        profile_name = kwargs.pop("profile_name", None)
        profiles_filepath = kwargs.pop("profiles_filepath"," ~/.great_expectations/profiles.yml")
        options = self._get_sqlalchemy_connection_options(profile_name, profiles_filepath)
        self._connect(options)

    def _connect(self, options, *args, **kwargs):
        self.engine = create_engine(options, *args, **kwargs)

    def list_data_assets(self):
        self.meta.reflect(bind=self.engine)
        tables = [str(table) for table in self.meta.sorted_tables]
        return tables

    def _get_sqlalchemy_connection_options(self, profile_name, profiles_filepath):
        with open(os.path.expanduser(profiles_filepath), "r") as data:
            profiles_config = yaml.safe_load(data) or {}
            print(profiles_config)

        db_config = profiles_config[profile_name]["sqlaclhemy"]
        options = \
            sqlalchemy.engine.url.URL(
                db_config["type"],
                username=db_config["user"],
                password=db_config["pass"],
                host=db_config["host"],
                port=db_config["port"],
                database=db_config["dbname"],
            )
        return options

    def get_data_asset(self, data_asset_name, custom_sql=None, schema=None, data_context=None):
        return SqlAlchemyDataset(table_name=data_asset_name, engine=self.engine, custom_sql=custom_sql, schema=schema, data_context=data_context, data_asset_name=data_asset_name)

