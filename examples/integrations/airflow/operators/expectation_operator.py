import json
from pprint import pformat

import os
from airflow import AirflowException
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.dbapi_hook import DbApiHook
from airflow.models import BaseOperator
import great_expectations as ge
from examples.integrations.airflow.hooks.s3_csv_hook import ExpectationS3CsvHook
from examples.integrations.airflow.hooks.db_hook import ExpectationMySQLHook


class ExpectationOperator(BaseOperator):

    results_dest_name = None
    template_fields = ['results_dest_name']

    def __init__(self,
                 dataset,
                 expectations_json,
                 fail_on_error=True,
                 source_conn_id=None,
                 dest_conn_id=None,
                 source_bucket_name=None,
                 results_dest_name=None,
                 results_bucket_name=None,
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
        :param conn_type: Type of conn_id that's being provided. (e.g s3connection or MySQL). Necessary for deciding
        which hook to use
        :type str
        :param source_conn_id: Connection that should be used to load dataset
        :type str
        :param source_bucket_name: Name of bucket where dataset can be found (only when using s3_csv connection)
        :type str
        :param dataset_params: Any arguments that need to be passed to great_expectations validate method
        :type dict
        :param results_dest_name: Name of file where results will be stored
        :type str
        :param results_bucket_name: Name of s3 bucket where results will be stored
        :type str
        """
        super().__init__(*args, **kwargs)

        # Get source hook
        if source_conn_id is not None:
            self._setup_source_conn(source_conn_id, source_bucket_name)

        # Get destination hook and make sure all required parameters are set
        if dest_conn_id is not None:
            self._setup_dest_conn(dest_conn_id, results_bucket_name, results_dest_name)

        self.expectations_json = expectations_json
        self.fail_on_error = fail_on_error

        if dataset_params is None:
            dataset_params = {}

        self.dataset_params = dataset_params
        self.dataset_name = dataset

    def _setup_source_conn(self, source_conn_id, source_bucket_name=None):
        """
        Retrieve connection based on source_conn_id. In case of s3 it also configures the bucket.
        Validates that connection id belongs to supported connection type.
        :param source_conn_id:
        :param source_bucket_name:
        """
        self.source_conn = BaseHook.get_hook(source_conn_id)
        self.source_conn_id = source_conn_id

        # Workaround for getting hook in case of s3 connection
        # This is needed because get_hook silently returns None for s3 connections
        # See https://issues.apache.org/jira/browse/AIRFLOW-2316 for more info
        connection = BaseHook._get_connection_from_env(source_conn_id)
        self.log.info(connection.extra_dejson)
        if connection.conn_type == 's3':
            self.log.info("Setting up s3 connection {0}".format(source_conn_id))
            self.source_conn = S3Hook(aws_conn_id=source_conn_id)
            # End Workaround
            if source_bucket_name is None:
                raise AttributeError("Missing source bucket for s3 connection")
            self.source_bucket_name = source_bucket_name

        if not isinstance(self.source_conn, DbApiHook) and not isinstance(self.source_conn, S3Hook):
            raise AttributeError(
                "Only s3_csv, local and sql connection types are allowed, not {0}".format(type(self.source_conn)))

    def _setup_dest_conn(self, dest_conn_id, results_bucket_name, results_dest_name):
        """
        Setup results connection. Retrieves s3 connection and makes sure we've got location details (bucket, filename)
        :param dest_conn_id:
        :param results_bucket_name:
        :param results_dest_name:
        """
        conn = BaseHook._get_connection_from_env(dest_conn_id)
        if conn.conn_type != 's3':
            raise AttributeError(
                "Only s3 is allowed as a results destination, not {0}".format(conn.conn_type))

        self.dest_conn = S3Hook(aws_conn_id=dest_conn_id)
        self.dest_conn_id = dest_conn_id

        if results_bucket_name is None or results_dest_name is None:
            raise AttributeError("Specify bucket name and key name to store results")

        self.results_bucket_name = results_bucket_name
        self.results_dest_name = results_dest_name

    def _get_dataframe(self):
        """
        Load dataframe based on specified connection
        :return:
        """
        if self.source_conn is None:  # Use local file
            return ge.read_csv(self.dataset_name, **self.dataset_params)

        if isinstance(self.source_conn, S3Hook):
            hook = ExpectationS3CsvHook(aws_conn_id=self.source_conn_id)

            return hook.get_ge_df(self.dataset_name, self.source_bucket_name, **self.dataset_params)

        if isinstance(self.source_conn, DbApiHook):
            hook = ExpectationMySQLHook(mysql_conn_id=self.source_conn_id)

            return hook.get_ge_df(self.dataset_name, **self.dataset_params)

    def _load_json(self):
        """
        Load expectation config based on operator parameters. If provided expectations_json is a file the config will
        be loaded from this file. Otherwise we'll try to load the config as a string.
        :return:
        """
        if os.path.isfile(self.expectations_json):
            self.log.info("Loading expectation config from file {file}".format(file=self.expectations_json))
            return json.load(open(self.expectations_json))
        else:
            self.log.info("Loading expectation config from string")
            return json.loads(self.expectations_json)

    def _store_results(self, results):
        hook = ExpectationS3CsvHook(aws_conn_id=self.dest_conn_id)
        return hook.dump_results(results, self.results_bucket_name, self.results_dest_name)

    def execute(self, context):
        df = self._get_dataframe()
        config = self._load_json()
        self.log.info("Start dataset validation for set {set}".format(set=self.dataset_name))
        results = df.validate(expectations_config=config)

        self.log.info(pformat(results))

        if self.dest_conn is not None:
            self._store_results(results)

        for result in results['results']:
            if result['success'] is False:
                if self.fail_on_error is True:
                    raise AirflowException("Validation failed for dataset {name}".format(name=self.dataset_name))

        return results
