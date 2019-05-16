import os

import sqlalchemy
import yaml


class DBTTools(object):
    """This is a helper class that makes using great expectations with dbt easy!"""

    def __init__(
        self,
        profile,
        target,
        dbt_project_file="dbt_project.yml",
        dbt_profiles_file_path="~/.dbt/profiles.yml",
    ):
        """
        Since the profiles file may contain multiple profiles and targets, the caller must specify the ones to use.

        :param profile_name: profile name (top level block in profiles.yml)
        :param target_name: target name (inside the profile block)
        :param dbt_profiles_file_path: path to dbt profiles.yml. If not provided, the method will try to load from dbt default location
        """
        self.profile = profile
        self.target = target
        self.dbt_project_file = dbt_project_file
        self.dbt_profiles_file_path = dbt_profiles_file_path

    def get_sqlalchemy_engine(self):
        """
        Create sqlalchemy engine using config and credentials stored in a particular profile/target in dbt profiles.yml file.

        :return: initialized sqlalchemy engine
        """
        with open(os.path.expanduser(self.dbt_profiles_file_path), "r") as data:
            profiles_config = yaml.safe_load(data) or {}

        db_config = profiles_config[self.profile]["outputs"][self.target]
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

    def _get_dbt_target_path(self):
        """
        Get the dbt `target` path from a users's dbt_project.yml file

        :return: path to `target` directory
        """
        with open(os.path.expanduser(self.dbt_project_file), "r") as f:
            dbt_project = yaml.safe_load(f) or {}

        return os.path.join(dbt_project["target-path"], "compiled", dbt_project["name"])

    def get_model_compiled_sql(self, model_name):
        """
        Read compiled SQL of a dbt model.

        :param model_name: model name. For model file blah/boo/mymodel.sql, pass the value "blah/boo/mymodel"

        :return: compiled SQL ready to be executed
        """
        try:
            with open(
                os.path.join(self._get_dbt_target_path(), model_name) + ".sql", "r"
            ) as data:
                return data.read()
        except FileNotFoundError as e:
            raise FileNotFoundError(
                f"dbt model {model_name} was not found. Is it in a sub-directory?"
            )
