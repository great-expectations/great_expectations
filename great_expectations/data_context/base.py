import os
import json
from great_expectations.version import __version__
from great_expectations.dataset import PandasDataset
from great_expectations import read_csv
from urllib.parse import urlparse

class DataContext(object):
    """A generic DataContext, exposing the base API including constructor with `options` parameter, list_datasets,
    and get_dataset.

    Warning: this feature is new in v0.4 and may change based on community feedback.
    """

    def __init__(self, options=None, *args, **kwargs):
        self.connect(options, *args, **kwargs)

    def connect(self, options):
        # TODO: Revisit this logic to better at making real guesses
        if options is None:
            if os.path.isdir("../notebooks") and os.path.isdir("../../great_expectations"):
                self.directory = ("../data_asset_configurations")
            else:
                self.directory = "./great_expectations/data_asset_configurations"
        else:
            if os.path.isdir(os.path.join(options, "great_expectations")):
                self.directory = options + "/great_expectations/data_asset_configurations"
            else:
                self.directory = os.path.join(options, "great_expectations/data_asset_configurations")
        self.validation_params = {}

    def list_data_asset_configs(self):
        return [os.path.splitext(os.path.basename(file_path))[0] for file_path in os.listdir(self.directory) if file_path.endswith('.json')]

    def get_data_asset_config(self, data_asset_name):
        config_file_path = os.path.join(self.directory, data_asset_name + '.json')
        if os.path.isfile(config_file_path):
            with open(os.path.join(self.directory, data_asset_name + '.json')) as json_file:
                return json.load(json_file)
        else:
            #TODO (Eugene): Would it be better to return None if the file does not exist? Currently this method acts as
            # get_or_create
            return {
                'data_asset_name': data_asset_name,
                'meta': {
                    'great_expectations.__version__': __version__
                },
                'expectations': [],
             }

    def save_data_asset_config(self, data_asset_config):
        data_asset_name = data_asset_config['data_asset_name']
        config_file_path = os.path.join(self.directory, data_asset_name + '.json')
        with open(config_file_path, 'w') as outfile:
            json.dump(data_asset_config, outfile)

    def bind_evaluation_parameters(self, run_id, expectations_config):
        return self.validation_params

    def register_validation_results(self, run_id, validation_results):
        #TODO (Eugene): this is a demo implementation!!!
        for result in validation_results['results']:
            if result['expectation_config']['expectation_type'] == 'expect_column_unique_value_count_to_be_between'\
                and result['expectation_config']['kwargs']['column'] == 'patient_nbr':
                self.validation_params = {
                    "urn:great_expectations:validations:datasets:source_diabetes_data:expectations:expect_column_unique_value_count_to_be_between:columns:patient_nbr:result:observed_value": result['result']['observed_value']
                }

    def _compile(self):
        return NotImplementedError

    def review_validation_result(self, url, failed_only=False):
        url = url.strip()
        if url.startswith("s3://"):
            try:
                import boto3
                s3 = boto3.client('s3')
            except ImportError:
                raise ImportError("boto3 is required for retrieving a dataset from s3")
        
            parsed_url = urlparse(url)
            bucket = parsed_url.netloc
            key = parsed_url.path[1:]
            
            s3_response_object = s3.get_object(Bucket=bucket, Key=key)
            object_content = s3_response_object['Body'].read()
            
            results_dict = json.loads(object_content)

            if failed_only:
                failed_results_list = [result for result in results_dict["results"] if not result["success"]]
                results_dict["results"] = failed_results_list
                return results_dict
            else:
                return results_dict
        else:
            raise ValueError("Only s3 urls are supported.")

    def get_failed_dataset(self, validation_result, **kwargs):
        try:
            reference_url = validation_result["meta"]["dataset_reference"]
        except KeyError:
            raise ValueError("Validation result must have a dataset_reference in the meta object to fetch")
        
        if reference_url.startswith("s3://"):
            try:
                import boto3
                s3 = boto3.client('s3')
            except ImportError:
                raise ImportError("boto3 is required for retrieving a dataset from s3")
        
            parsed_url = urlparse(reference_url)
            bucket = parsed_url.netloc
            key = parsed_url.path[1:]
            
            s3_response_object = s3.get_object(Bucket=bucket, Key=key)
            if key.endswith(".csv"):
                # Materialize as dataset
                # TODO: check the associated config for the correct data_asset_type to use
                return read_csv(s3_response_object['Body'], **kwargs)
            else:
                return s3_response_object['Body']

        else:
            raise ValueError("Only s3 urls are supported.")