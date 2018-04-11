import json
from pprint import pformat

import os
from airflow import AirflowException
from airflow.models import BaseOperator
import great_expectations as ge

from great_expectations_airflow.hooks.s3_csv_hook import ExpectationCsvS3Hook


class ExpectationOperator(BaseOperator):

    def __init__(self,
                 dataset,
                 expectations_json,
                 fail_on_error=True,
                 conn_type="local",
                 conn_id=None,
                 bucket_name=None,
                 dataset_params=None,
                 *args, **kwargs):
        """
        Validate provided dataset using great_expectations.
        :param dataset: Name of the dataset being loaded
        :type str
        :param expectations_json: file pointing to expectation config or json string
        :type str
        :param fail_on_error: True if airflow job should fail when expectations fail
        :type bool
        :param conn_type: Type of conn_id that's being provided. (e.g S3Connection or MySQL). Necessary for deciding
        which hook to use
        :type str
        :param conn_id: Connection that should be used to load dataset
        :type str
        :param bucket_name: Name of bucket where dataset can be found (only when using s3_csv connection)
        :type str
        :param dataset_params: Any arguments that need to be passed to great_expectations validate method
        :type dict
        """
        super().__init__(*args, **kwargs)

        if conn_type != "s3_csv" and conn_type != 'sql' and conn_type != 'local':
            raise AttributeError("Only s3_csv, local and sql connection types are allowed")

        self.conn_type = conn_type
        self.expectations_json = expectations_json
        self.conn_id = conn_id
        self.fail_on_error = fail_on_error

        if dataset_params is None:
            dataset_params = {}

        self.dataset_params = dataset_params
        self.dataset_name = dataset
        self.bucket_name = bucket_name

    def _get_dataframe(self):
        if self.conn_type == 'local':
            return ge.read_csv(self.dataset_name, **self.dataset_params)

        if self.conn_type == 's3_csv':
            hook = ExpectationCsvS3Hook(aws_conn_id=self.conn_id)

            return hook.get_ge_df(self.dataset_name, self.bucket_name, **self.dataset_params)

    def _load_json(self):
        """
        Load exepectation config based on operator parameters. If provided expectations_json is a file the config will
        be loaded from this file. Otherwise we'll try to load the config as a string.
        :return:
        """
        if os.path.isfile(self.expectations_json):
            self.log.info("Loading expectation config from file {file}".format(file=self.expectations_json))
            return json.load(open(self.expectations_json))
        else:
            self.log.info("Loading expectation config from string")
            return json.loads(self.expectations_json)

    def execute(self, context):
        df = self._get_dataframe()
        config = self._load_json()
        self.log.info("Start dataset validation for dataset {set}".format(set=self.dataset_name))
        results = df.validate(expectations_config=config)

        self.log.info(pformat(results))

        for result in results['results']:
            if result['success'] is False:
                if self.fail_on_error is True:
                    raise AirflowException("Validation failed for dataset {name}".format(name=self.dataset_name))

        return results
