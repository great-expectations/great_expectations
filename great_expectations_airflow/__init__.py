from airflow.plugins_manager import AirflowPlugin
from great_expectations_airflow.operators.expectation_operator import ExpectationOperator
from great_expectations_airflow.hooks.s3_csv_hook import ExpectationS3CsvHook
from great_expectations_airflow.hooks.db_hook import ExpectationMySQLHook

class GreatExpectationsPlugin(AirflowPlugin):
    name = 'great_expectations_plugin'

    # A list of class(es) derived from BaseOperator
    operators = [ExpectationOperator]
    # A list of class(es) derived from BaseHook
    hooks = [ExpectationS3CsvHook, ExpectationMySQLHook]
    # A list of class(es) derived from BaseExecutor
    executors = []
    # A list of references to inject into the macros namespace
    macros = []
    # A list of objects created from a class derived
    # from flask_admin.BaseView
    admin_views = []
    # A list of Blueprint object created from flask.Blueprint
    flask_blueprints = []
    # A list of menu links (flask_admin.base.MenuLink)
    menu_links = []