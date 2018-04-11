from tempfile import NamedTemporaryFile

from airflow import AirflowException
from airflow.hooks.S3_hook import S3Hook
import great_expectations as ge

# class ExpectationDatabaseHook(DbApiHook):
#
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#
#     def get_ge_df(self, dataset_name, custom_sql=None):
#         sql_context = ge.get_data_context('SqlAlchemy', self.get_uri())
#         sql_context.get_dataset(dataset_name=dataset_name, custom_sql=custom_sql)


class ExpectationCsvS3Hook(S3Hook):

    def get_ge_df(self, dataset_name, bucket_name=None, **kwargs):
        if not self.check_for_key(dataset_name, bucket_name):
            raise AirflowException("The source key {0} does not exist".format(dataset_name))

        s3_key_object = self.get_key(dataset_name, bucket_name)
        with NamedTemporaryFile("w") as temp_file:
            self.log.info("Temp dumping S3 file {0} contents to local {1} file".format(dataset_name, temp_file.name))
            s3_key_object.download_file(temp_file.name)
            temp_file.flush()

            return ge.read_csv(temp_file.name, **kwargs)