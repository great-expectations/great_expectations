import os
import json
from great_expectations.version import __version__

class DataContext(object):
    """A generic DataContext, exposing the base API including constructor with `options` parameter, list_datasets,
    and get_dataset.

    Warning: this feature is new in v0.4 and may change based on community feedback.
    """

    def __init__(self, options, *args, **kwargs):
        self.connect(options, *args, **kwargs)

    def connect(self, options):
        self.directory = options
        self.validation_params = {}

    def list_dataset_configs(self):
        return [os.path.splitext(os.path.basename(file_path))[0] for file_path in os.listdir(self.directory) if file_path.endswith('.json')]

    def get_dataset_config(self, data_asset_name):
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

    def save_dataset_config(self, dataset_config):
        data_asset_name = dataset_config['data_asset_name']
        config_file_path = os.path.join(self.directory, data_asset_name + '.json')
        with open(config_file_path, 'w') as outfile:
            json.dump(dataset_config, outfile)

    def bind_evaluation_parameters(self, run_id, expectations_config):
        return self.validation_params

    def register_validation_results(self, run_id, validation_results):
        #TODO (Eugene): this is a demo implementation!!!
        for result in validation_results['results']:
            if result['expectation_config']['expectation_type'] == 'expect_column_unique_value_count_to_be_between'\
                and result['expectation_config']['kwargs']['column'] == 'patient_nbr':
                self.validation_params = {
                    "urn:great_expectations:validations:datasets:diabetes_data:expectations:expect_column_unique_value_count_to_be_between:columns:patient_nbr:result:observed_value": result['result']['observed_value']
                }

    def _compile(self):
        return NotImplementedError
