import os
import time

from ruamel.yaml import YAML
yaml = YAML(typ='safe')

import sqlalchemy
from sqlalchemy import create_engine, MetaData

from .datasource import Datasource
from .batch_generator import BatchGenerator
from great_expectations.dataset.sqlalchemy_dataset import SqlAlchemyDataset

class DBTModelGenerator(BatchGenerator):
    """This is a helper class that makes using great expectations with dbt easy!"""

    def __init__(self, name="default", datasource=None):
        super(DBTModelGenerator, self).__init__(name, type_="dbt_models", datasource=datasource)
        self.dbt_target_path = datasource.dbt_target_path

    def _get_iterator(self, data_asset_name):
        """
        Read compiled SQL of a dbt model.

        :param model_name: model name. For model file blah/boo/mymodel.sql, pass the value "blah/boo/mymodel"

        :return: compiled SQL ready to be executed
        """
        try:
            with open(os.path.join(self.dbt_target_path, data_asset_name) + ".sql", "r") as data:
                return iter([{
                    "query": data.read(),
                    "timestamp": time.time()
                }])
        except IOError as e:
            if e.errno == errno.NOENT:
                raise IOError(
                    "dbt model %s was not found in the compiled directory. Please run `dbt compile` or `dbt run` and try again. Or, check the directory." % data_asset_name
                )
            else:
                raise

    def list_available_data_asset_names(self):
        return set([path for path in os.walk(self.dbt_target_path) if path.endswith(".sql")])


class DBTDatasource(Datasource):
    """
    A DBTDataSource create a SQLAlchemy connection to the database used by a dbt project
    and allows to create, manage and validate expectations on the models that exist in that dbt project.
    """

    def __init__(self, 
            name="default", 
            data_context=None,
            generators=None,
            profile="default",         
            project_filepath="dbt_project.yml",
            profiles_filepath="~/.dbt/profiles.yml",
            **kwargs
        ):
        if generators is None:
            generators = {
                "default": {"type": "dbt_models"}
            }
        super(DBTDatasource, self).__init__(name, type_="dbt", data_context=data_context, generators=generators)
        self._datasource_config.update({
            "profile": profile,
            "project_filepath": project_filepath,
            "profiles_filepath": profiles_filepath
        })

        self.meta = MetaData()
        with open(os.path.join(self._data_context.get_context_root_directory(), self._datasource_config["project_filepath"]), "r") as f:
            self._dbt_project = yaml.load(f) or {}
            
        self.dbt_target_path = os.path.join(
            self._data_context.get_context_root_directory(),
            self._dbt_project["target-path"],
            "compiled",
            self._dbt_project["name"],
        )

        self._options = self._get_sqlalchemy_connection_options()
        self._connect(self._get_sqlalchemy_connection_options())
        self._build_generators()

    def _connect(self, options, *args, **kwargs):
        self.engine = create_engine(options, *args, **kwargs)

    def _get_sqlalchemy_connection_options(self):
        with open(os.path.expanduser(self._datasource_config["profiles_filepath"]), "r") as data:
            profiles_config = yaml.load(data) or {}

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

    def _get_generator_class(self, type_):
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
        custom_sql = batch_kwargs["query"]
        return SqlAlchemyDataset(table_name=data_asset_name, 
            engine=self.engine, 
            data_context=self._data_context, 
            data_asset_name=data_asset_name,
            expectations_config=expectations_config, 
            custom_sql=custom_sql, 
            batch_kwargs=batch_kwargs)