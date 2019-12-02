import os
import time
import logging
import errno

from ruamel.yaml import YAML

from .sqlalchemy_datasource import SqlAlchemyDatasource
from great_expectations.datasource.generator.batch_generator import BatchGenerator

yaml = YAML()
logger = logging.getLogger(__name__)

try:
    import sqlalchemy
    from sqlalchemy import create_engine, MetaData
except ImportError:
    logger.debug("Unable to import sqlalchemy.")


class DBTModelGenerator(BatchGenerator):
    """This is a helper class that makes using great expectations with dbt easy!"""

    def __init__(self, name="dbt_models", datasource=None):
        super(DBTModelGenerator, self).__init__(name, type_="dbt_models", datasource=datasource)
        self.dbt_target_path = datasource.dbt_target_path

    def _get_iterator(self, data_asset_name, **kwargs):
        """
        Read compiled SQL of a dbt model.

        :param data_asset_name: model name. For model file blah/boo/mymodel.sql, pass the value "blah/boo/mymodel"

        :return: iterator over batch_kwargs with a query parameter equal to the content of the relevant model file
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

    def get_available_data_asset_names(self):
        return set([path for path in os.walk(self.dbt_target_path) if path.endswith(".sql")])


class DBTDatasource(SqlAlchemyDatasource):
    """
    A DBTDataSource creates a SQLAlchemy connection to the database used by a dbt project.

    and allows to create, manage and validate expectations on the models that exist in that dbt project.
    """

    def __init__(self,
                 name="dbt",
                 data_context=None,
                 generators=None,
                 profile="default",
                 project_filepath="dbt_project.yml",
                 profiles_filepath="~/.dbt/profiles.yml",
                 **kwargs
        ):
        if generators is None:
            generators = {
                "dbt_models": {"type": "dbt_models"}
            }
        super(DBTDatasource, self).__init__(name, type_="dbt", data_context=data_context, generators=generators)
        self._datasource_config.update({
            "profile": profile,
            "project_filepath": project_filepath,
            "profiles_filepath": profiles_filepath
        })
        self._datasource_config.update(kwargs)

        with open(os.path.join(self._data_context.root_directory,
                               self._datasource_config["project_filepath"]), "r") as f:
            self._dbt_project = yaml.load(f) or {}
            
        self.dbt_target_path = os.path.join(
            self._data_context.root_directory,
            self._dbt_project["target-path"],
            "compiled",
            self._dbt_project["name"],
        )

        self._options = self._get_sqlalchemy_connection_options()
        self._connect(self._get_sqlalchemy_connection_options(**kwargs))
        self._build_generators()

    def _get_sqlalchemy_connection_options(self, **kwargs):
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

    def build_batch_kwargs(self, *args, **kwargs):
        if len(args) > 0:
            # Allow a model name here
            generator = self.get_generator()
            if isinstance(generator, DBTModelGenerator):
                batch_kwargs = generator.yield_batch_kwargs(args[0])
            else:
                batch_kwargs = {}
        else:
            batch_kwargs = {}
        batch_kwargs.update({
             "timestamp": time.time()
        })
        return batch_kwargs
