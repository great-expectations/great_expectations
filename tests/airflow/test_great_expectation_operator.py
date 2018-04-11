import datetime
import unittest

import os
from pprint import pprint

from airflow import DAG
from airflow.models import TaskInstance

from great_expectations_airflow.operators.expectation_operator import ExpectationOperator


class TestGreatExpectationOperator(unittest.TestCase):

    def test_execute(self):
        script_path = os.path.dirname(os.path.realpath(__file__))
        test_set = script_path + '/../test_sets/Titanic.csv'
        expect_json = script_path + '/../test_sets/titanic_expectations.json'

        dag = DAG(dag_id='foo', start_date=datetime.datetime.now())
        task = ExpectationOperator(dag=dag,
                                   task_id='test_airflow_task',
                                   conn_type='local',
                                   dataset=test_set,
                                   expectations_json=expect_json)

        ti = TaskInstance(task=task, execution_date=datetime.datetime.now())
        result = task.execute(ti.get_template_context())

        pprint(result)
        self.assertEqual({}, result)
