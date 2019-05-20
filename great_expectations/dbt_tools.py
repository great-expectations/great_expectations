import os

import sqlalchemy
import yaml


class DBTTools(object):
    """This is a helper class that makes using great expectations with dbt easy!"""

    def __init__(
        self,
        profile,
        # TODO this is geared towards init notebook use
        dbt_base_directory="../../",
        dbt_project_file="dbt_project.yml",
        dbt_profiles_file_path="~/.dbt/profiles.yml",
    ):
        """
        Since the profiles file may contain multiple profiles, the caller must specify the profile to use.

        :param dbt_base_directory: path to base of dbt project (defaults to ../../)
        :param profile: profile name (top level block in profiles.yml)
        :param dbt_profiles_file_path: path to dbt profiles.yml. If not provided, the method will try to load from dbt default location
        """
        self.profile = profile
        self.dbt_profiles_file_path = dbt_profiles_file_path

        with open(os.path.join(dbt_base_directory, dbt_project_file), "r") as f:
            self.dbt_project = yaml.safe_load(f) or {}

        self.dbt_target_path = os.path.join(
            dbt_base_directory,
            self.dbt_project["target-path"],
            "compiled",
            self.dbt_project["name"],
        )

    def get_sqlalchemy_connection_options(self):
        with open(os.path.expanduser(self.dbt_profiles_file_path), "r") as data:
            profiles_config = yaml.safe_load(data) or {}

        target = profiles_config[self.profile]["target"]
        db_config = profiles_config[self.profile]["outputs"][target]
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
        with open(os.path.expanduser(self.dbt_profiles_file_path), "r") as data:
            profiles_config = yaml.safe_load(data) or {}

        target = profiles_config[self.profile]["target"]
        db_config = profiles_config[self.profile]["outputs"][target]
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

    def get_model_compiled_sql(self, model_name):
        """
        Read compiled SQL of a dbt model.

        :param model_name: model name. For model file blah/boo/mymodel.sql, pass the value "blah/boo/mymodel"

        :return: compiled SQL ready to be executed
        """
        try:
            with open(
                os.path.join(self.dbt_target_path, model_name) + ".sql", "r"
            ) as data:
                return data.read()
        except FileNotFoundError as e:
            raise FileNotFoundError(
                f"dbt model {model_name} was not found in the compiled directory. Please run `dbt compile` or `dbt run` and try again. Or, check the directory."
            )
