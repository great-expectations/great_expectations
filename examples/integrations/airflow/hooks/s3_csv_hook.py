import json
from tempfile import NamedTemporaryFile

from airflow import AirflowException
from airflow.hooks.S3_hook import S3Hook
import great_expectations as ge


class ExpectationS3CsvHook(S3Hook):

    def get_ge_df(self, dataset_name, bucket_name=None, **kwargs):
        if not self.check_for_key(dataset_name, bucket_name):
            aws_access_key_id, aws_secret_access_key, region_name, s3_endpoint_url = self._get_credentials('eu-west-1')
            self.log.info(aws_access_key_id)
            self.log.info(aws_secret_access_key)
            raise AirflowException("The source key {0} does not exist in bucket {1}".format(dataset_name, bucket_name))

        s3_key_object = self.get_key(dataset_name, bucket_name)
        with NamedTemporaryFile("w") as temp_file:
            self.log.info("Temp dumping S3 file {0} contents to local {1} file".format(dataset_name, temp_file.name))
            s3_key_object.download_file(temp_file.name)
            temp_file.flush()

            return ge.read_csv(temp_file.name, **kwargs)

    def dump_results(self, results, bucket_name, dest_file_name):
        if not self.check_for_bucket(bucket_name):
            raise AirflowException("Bucket {0} doest not exist".format(bucket_name))

        with NamedTemporaryFile("w") as temp_file:
            self.log.info("Temp dumping expectation results to local file {temp_file}".format(temp_file=temp_file))
            json.dump(results, temp_file)
            temp_file.flush()
            self.load_file(temp_file.name, dest_file_name, bucket_name=bucket_name)
            self.log.info("Uploaded {temp} to bucket {bucket} with key {key}".format(temp=temp_file, bucket=bucket_name,
                                                                                     key=dest_file_name))

        return True
