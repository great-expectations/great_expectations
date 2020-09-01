from airflow.plugins_manager import AirflowPlugin

from great_expectations.airflow.operators.checkpoint_operator import CheckpointOperator


class GreatExpectationsPlugin(AirflowPlugin):
    name = 'great_expectations'

    # A list of class(es) derived from BaseOperator
    operators = [CheckpointOperator]
    # A list of class(es) derived from BaseHook
    hooks = []
    # A list of class(es) derived from BaseExecutor
    executors = []
    # A list of references to inject into the macros namespace
    macros = []

    # List of views that use Flask Appbuilder
    appbuilder_views = []
