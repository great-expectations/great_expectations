import os
import yaml
import datetime

from .datasource import Datasource
from .batch_generator import BatchGenerator
from ...dataset.sqlalchemy_dataset import SqlAlchemyDataset

import sqlalchemy
from sqlalchemy import create_engine, MetaData

class DBTModelGenerator(BatchGenerator):
    """This is a helper class that makes using great expectations with dbt easy!"""

    def __init__(self, name, type_, datasource):
        super(DBTModelGenerator, self).__init__(name, type_, datasource)
        self.dbt_target_path = datasource.dbt_target_path

    def _get_iterator(self, data_asset_name):
        """
        Read compiled SQL of a dbt model.

        :param model_name: model name. For model file blah/boo/mymodel.sql, pass the value "blah/boo/mymodel"

        :return: compiled SQL ready to be executed
        """
        try:
            with open(
                os.path.join(self.dbt_target_path, data_asset_name) + ".sql", "r"
            ) as data:
                return iter([{
                    "query": data.read(),
                    "timestamp": datetime.datetime.now().timestamp()
                }])
        except FileNotFoundError as e:
            raise FileNotFoundError(
                "dbt model %s was not found in the compiled directory. Please run `dbt compile` or `dbt run` and try again. Or, check the directory." % data_asset_name
            )


class DBTDatasource(Datasource):
    """
    A DBTDataSource create a SQLAlchemy connection to the database used by a dbt project
    and allows to create, manage and validate expectations on the models that exist in that dbt project.
    """

    def __init__(self, 
            name, 
            type_, 
            data_context, 
            profile,         
            base_directory="../../",
            project_filepath="dbt_project.yml",
            profiles_filepath="~/.dbt/profiles.yml",
            **kwargs
        ):
        super(DBTDatasource, self).__init__(name, type_, data_context)
        self._datasource_config.update({
            "profile": profile,
            "base_directory": base_directory,
            "project_filepath": project_filepath,
            "profiles_filepath": profiles_filepath
        })

        self.meta = MetaData()
        with open(os.path.join(self._datasource_config["base_directory"], self._datasource_config["project_filepath"]), "r") as f:
            self._dbt_project = yaml.safe_load(f) or {}
            
        self.dbt_target_path = os.path.join(
            self._datasource_config["base_directory"],
            self._dbt_project["target-path"],
            "compiled",
            self._dbt_project["name"],
        )

        self._options = self._get_sqlalchemy_connection_options()
        self._connected = False

    def _connect(self, options, *args, **kwargs):
        self.engine = create_engine(options, *args, **kwargs)
        self._connected = True

    def _get_sqlalchemy_connection_options(self):
        with open(os.path.expanduser(self._datasource_config["profiles_filepath"]), "r") as data:
            profiles_config = yaml.safe_load(data) or {}

        target = profiles_config[self._datasource_config["profile"]]["target"]
        db_config = profiles_config[self._datasource_config["profile"]]["outputs"][target]
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

    def get_sqlalchemy_engine(self):
        """
        Create sqlalchemy engine using config and credentials stored in a particular profile/target in dbt profiles.yml file.

        :return: initialized sqlalchemy engine
        """
        with open(os.path.expanduser(self._datasource_config["profiles_filepath"]), "r") as data:
            profiles_config = yaml.safe_load(data) or {}

        target = profiles_config[self._datasource_config["profile"]]["target"]
        db_config = profiles_config[self._datasource_config["profile"]]["outputs"][target]
        engine = sqlalchemy.create_engine(
            sqlalchemy.engine.url.URL(
                db_config["type"],
                username=db_config["user"],
                password=db_config["pass"],
                host=db_config["host"],
                port=db_config["port"],
                database=db_config["dbname"],
            )
        )
        # Ensure the connection is valid
        # TODO error handling might be nice here
        connection = engine.connect()

        return engine

    def _get_data_asset_generator_class(self, type_):
        if type_ == "dbt_models":
            return DBTModelGenerator
        else:
            raise ValueError("Unrecognized DataAssetGenerator type %s" % type_)

    def _get_data_asset(self, data_asset_name, batch_kwargs, expectations_config):
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
        custom_sql = batch_kwargs["custom_sql"]
        return SqlAlchemyDataset(table_name=data_asset_name, engine=self.engine, data_context=self._data_context, data_asset_name=data_asset_name, expectations_config=expectations_config, custom_sql=custom_sql)