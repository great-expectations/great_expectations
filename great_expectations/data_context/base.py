import os
import json
import logging
import yaml
import sys
import copy
from glob import glob

from great_expectations.exceptions import ExpectationsConfigNotFoundError
from great_expectations.version import __version__
from great_expectations.dataset import PandasDataset
from great_expectations import read_csv
from IPython.display import display
import ipywidgets as widgets
from urllib.parse import urlparse

from .datasource.sqlalchemy_source import SqlAlchemyDatasource
from .datasource.dbt_source import DBTDatasource
from .datasource.pandas_source import PandasCSVDatasource
from .datasource.spark_source import SparkDFDatasource

from .expectation_explorer import ExpectationExplorer

logger = logging.getLogger(__name__)
debug_view = widgets.Output(layout={'border': '3 px solid pink'})


class DataContext(object):
    #TODO: update class documentation
    """A generic DataContext, exposing the base API including constructor with `options` parameter, list_datasets,
    and get_dataset.

    Warning: this feature is new in v0.4 and may change based on community feedback.
    """

    def __init__(self, options=None, expectation_explorer=False, *args, **kwargs):
        self._expectation_explorer = expectation_explorer
        if expectation_explorer:
            self._expectation_explorer_manager = ExpectationExplorer()
        self.connect(options, *args, **kwargs)
        self._datasources = {}

    def connect(self, context_root_dir):
        # determine the "context root directory" - this is the parent of "great_expectations" dir

        if context_root_dir is None:
            raise Exception("the guessing logic not implemented yet!")
        # TODO: Revisit this logic to better at making real guesses
        # if os.path.isdir("../notebooks") and os.path.isdir("../../great_expectations"):
            #     self.context_root_directory = "../data_asset_configurations"
            # else:
            #     self.context_root_directory = "./great_expectations/data_asset_configurations"
        else:
            if os.path.isdir(os.path.join(context_root_dir, "great_expectations")):
                self.context_root_directory = context_root_dir
            else:
                self.context_root_directory = context_root_dir

        self.context_root_directory = os.path.abspath(self.context_root_directory)

        self.directory = os.path.join(self.context_root_directory, "great_expectations/expectations")
        self.plugin_store_directory = os.path.join(self.context_root_directory, "great_expectations/plugins/store")
        sys.path.append(self.plugin_store_directory)
        
        self._project_config = self._load_project_config()

        self._load_evaluation_parameter_store()
        self._compiled = False

    def _load_project_config(self):
        # TODO: What if the project config file does not exist?
        # TODO: Should we merge the project config file with the global config file?
        with open(os.path.join(self.context_root_directory, "great_expectations.yml"), "r") as data:
            return yaml.safe_load(data) or {}

    def _save_project_config(self):
        with open(os.path.join(self.context_root_directory, "great_expectations.yml"), "w") as data:
            yaml.safe_dump(self._project_config, data)

    def _get_all_profile_credentials(self):
        try:
            with open(os.path.join(self.context_root_directory, "uncommitted/credentials/profiles.yml"), "r") as profiles_file:
                return yaml.safe_load(profiles_file) or {}
        except FileNotFoundError:
            logger.warning("No profile credential store found.")
            return {}

    def get_profile_credentials(self, profile_name):
        profiles = self._get_all_profile_credentials()
        if profile_name in profiles:
            return profiles[profile_name]
        else:
            return {}

    def add_profile_credentials(self, profile_name, **kwargs):
        profiles = self._get_all_profile_credentials()
        profiles[profile_name] = {**kwargs}
        profiles_filepath = os.path.join(self.context_root_directory, "uncommitted/credentials/profiles.yml")
        os.makedirs(os.path.dirname(profiles_filepath), exist_ok=True)
        with open(profiles_filepath, "w") as profiles_file:
            yaml.safe_dump(profiles, profiles_file)

    def get_datasource_config(self, datasource_name):
        """We allow a datasource to be defined in any combination of the following two ways:
        
        1. It may be fully specified in the datasources section of the great_expectations.yml file
        2. It may be stored in a file by convention located in `datasources/<datasource_name>/config.yml`
        3. It may be listed in the great_expectations.yml file with a config_file key that provides a relative path to a different yml config file
        
        Any key duplicated across configs will be updated by the last key read (in the order above)
        """
        datasource_config = {}
        defined_config_path = None
        default_config_path = os.path.join(self.context_root_directory, "datasources", datasource_name, "config.yml")
        if datasource_name in self._project_config["datasources"]:
            base_datasource_config = copy.deepcopy(self._project_config["datasources"][datasource_name])
            if "config_file" in base_datasource_config:
                defined_config_path = os.path.join(self.context_root_directory, base_datasource_config.pop("config_file"))
            datasource_config.update(base_datasource_config)
        
        try:
            with open(default_config_path, "r") as config_file:
                default_path_datasource_config = yaml.safe_load(config_file) or {}
            datasource_config.update(default_path_datasource_config)
        except FileNotFoundError:
            logger.debug("No config file found in default location for datasource %s" % datasource_name)
        
        if defined_config_path is not None:
            try:
                with open(defined_config_path, "r") as config_file:
                    defined_path_datasource_config = yaml.safe_load(config_file) or {}
                datasource_config.update(defined_path_datasource_config)
            except FileNotFoundError:
                logger.warning("No config file found in user-defined location for datasource %s" % datasource_name)
        
        return datasource_config

    def list_data_assets(self, datasource_name="default"):
        datasource = self.get_datasource(datasource_name)
        return datasource.list_data_assets()

    def get_data_asset(self, data_asset_name, datasource_name="default", batch_kwargs=None):
        datasource = self.get_datasource(datasource_name)
        data_asset = datasource.get_data_asset(data_asset_name, batch_kwargs)
        # data_asset._initialize_expectations(self.get_data_asset_config(data_asset_name))
        return data_asset

    def add_datasource(self, name, type_, **kwargs):
        datasource_class = self._get_datasource_class(type_)
        datasource = datasource_class(name, type_, data_context=self, **kwargs)
        self._datasources[name] = datasource
        if not "datasources" in self._project_config:
            self._project_config["datasources"] = {}
        self._project_config["datasources"][name] = datasource.get_config()
        self._save_project_config()

        return datasource

    def get_config(self):
        self._save_project_config()
        return self._project_config

    def _get_datasource_class(self, datasource_type):
        if datasource_type == "pandas":
            return PandasCSVDatasource
        elif datasource_type == "dbt":
            return DBTDatasource
        elif datasource_type == "sqlalchemy":
            return SqlAlchemyDatasource
        elif datasource_type == "spark":
            return SparkDFDatasource
        else:
            try:
                # Update to do dynamic loading based on plugin types
                return PandasCSVDatasource
            except ImportError:
                raise
    
    def get_datasource(self, datasource_name):
        if datasource_name in self._datasources:
            return self._datasources[datasource_name]
        try:
            datasource_config = copy.deepcopy(self._project_config["datasources"][datasource_name])
            type_ = datasource_config.pop("type")
            datasource_class= self._get_datasource_class(type_)
            return datasource_class(datasource_name, type_, self, **datasource_config)
        except KeyError:
            raise ValueError(f"Unable to load datasource %s -- no configuration found or invalid configuration." % datasource_name)


    def _load_evaluation_parameter_store(self):

        # This is a trivial class that implements in-memory key value store.
        # We use it when user does not specify a custom class in the config file
        class MemoryEvaluationParameterStore(object):
            def __init__(self):
                self.dict = {}
            def get(self, run_id, name):
                if run_id in self.dict:
                    return self.dict[run_id][name] 
                else:
                    return {}
            def set(self, run_id, name, value):
                if not run_id in self.dict:
                    self.dict[run_id] = {}
                self.dict[run_id][name] = value
            def get_run_parameters(self, run_id):
                if run_id in self.dict:
                    return self.dict[run_id]
                else:
                    return {}

        # If user wishes to provide their own implementation for this key value store (e.g.,
        # Redis-based), they should specify the following in the project config file:
        #
        # evaluation_parameter_store:
        #   type: demostore
        #   config:  - this is optional - this is how we can pass kwargs to the object's c-tor
        #     param1: boo
        #     param2: bah
        #
        # Module called "demostore" must be found in great_expectations/plugins/store.
        # Class "GreatExpectationsEvaluationParameterStore" must be defined in that module.
        # The class must implement the following methods:
        # 1. def __init__(self, **kwargs)
        #
        # 2. def get(self, name)
        #
        # 3. def set(self, name, value)
        #
        # We will load the module dynamically
        try:
            config_block = self._project_config.get("evaluation_parameter_store")
            if not config_block or not config_block.get("type"):
                self._evaluation_parameter_store = MemoryEvaluationParameterStore()
            else:
                module_name = config_block.get("type")
                class_name = "GreatExpectationsEvaluationParameterStore"

                loaded_module = __import__(module_name, fromlist=[module_name])
                loaded_class = getattr(loaded_module, class_name)
                if config_block.get("config"):
                    self._evaluation_parameter_store = loaded_class(**config_block.get("config"))
                else:
                    self._evaluation_parameter_store = loaded_class()
        except Exception as err:
            logger.exception("Failed to load evaluation_parameter_store class")
            raise

    def list_expectations_configs(self):
        root_path = self.directory
        result = [os.path.splitext(os.path.relpath(y, root_path))[0] for x in os.walk(root_path) for y in glob(os.path.join(x[0], '*.json'))]
        return result

    def _find_data_asset_config(self, data_asset_name, batch_kwargs):
        configs = self.list_expectations_configs()
        if data_asset_name in configs:
            return self.get_data_asset_config(data_asset_name)
        else:
            last_found_config = None
            options = 0
            for config in configs:
                if data_asset_name in config:
                    options += 1
                    last_found_config = config
            if options == 1:
                return last_found_config

        raise ExpectationsConfigNotFoundError(data_asset_name)
                

    def get_expectations_config(self, data_asset_name, batch_kwargs):
        return self.get_data_asset_config(data_asset_name)

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
        os.makedirs(os.path.split(config_file_path)[0], exist_ok=True)
        with open(config_file_path, 'w') as outfile:
            json.dump(data_asset_config, outfile)
        self._compiled = False

    def bind_evaluation_parameters(self, run_id, expectations_config):
        return self._evaluation_parameter_store.get_run_parameters(run_id)

    def register_validation_results(self, run_id, validation_results):
        if not self._compiled:
            self._compile()

        if "meta" not in validation_results or "data_asset_name" not in validation_results["meta"]:
            logger.warning("No data_asset_name found in validation results; evaluation parameters cannot be registered.")
            return
        elif validation_results["meta"]["data_asset_name"] not in self._compiled_parameters["data_assets"]:
            # This is fine; short-circuit since we do not need to register any results from this dataset.
            return
        else:
            data_asset_name = validation_results["meta"]["data_asset_name"]
        
        for result in validation_results['results']:
            # Unoptimized: loop over all results and check if each is needed
            expectation_type = result['expectation_config']['expectation_type']
            if expectation_type in self._compiled_parameters["data_assets"][data_asset_name]:
                # First, bind column-style parameters
                if (("column" in result['expectation_config']['kwargs']) and 
                    ("columns" in self._compiled_parameters["data_assets"][data_asset_name][expectation_type]) and 
                    (result['expectation_config']['kwargs']["column"] in self._compiled_parameters["data_assets"][data_asset_name][expectation_type]["columns"])):

                    column = result['expectation_config']['kwargs']["column"]
                    # Now that we have a small search space, invert logic, and look for the parameters in our result
                    for type_key, desired_parameters in self._compiled_parameters["data_assets"][data_asset_name][expectation_type]["columns"][column].items():
                        # value here is the set of desired parameters under the type_key
                        for desired_param in desired_parameters:
                            desired_key = desired_param.split(":")[-1]
                            if type_key == "result" and desired_key in result['result']:
                                self.store_validation_param(run_id, desired_param, result["result"][desired_key])
                            elif type_key == "details" and desired_key in result["result"]["details"]:
                                self.store_validation_param(run_id, desired_param, result["result"]["details"])
                            else:
                                logger.warning("Unrecognized key for parameter %s" % desired_param)
                
                # Next, bind parameters that do not have column parameter
                for type_key, desired_parameters in self._compiled_parameters["data_assets"][data_asset_name][expectation_type].items():
                    if type_key == "columns":
                        continue
                    for desired_param in desired_parameters:
                        desired_key = desired_param.split(":")[-1]
                        if type_key == "result" and desired_key in result['result']:
                            self.store_validation_param(run_id, desired_param, result["result"][desired_key])
                        elif type_key == "details" and desired_key in result["result"]["details"]:
                            self.store_validation_param(run_id, desired_param, result["result"]["details"])
                        else:
                            logger.warning("Unrecognized key for parameter %s" % desired_param)

    def store_validation_param(self, run_id, key, value):
        self._evaluation_parameter_store.set(run_id, key, value)

    def get_validation_param(self, run_id, key):
        return self._evaluation_parameter_store.get(run_id, key)

    def _compile(self):
        """Compiles all current expectation configurations in this context to be ready for reseult registration.
        
        Compilation only respects parameters with a URN structure beginning with urn:great_expectations:validations
        It splits parameters by the : (colon) character; valid URNs must have one of the following structures to be
        automatically recognized.

        "urn" : "great_expectations" : "validations" : data_asset_name : "expectations" : expectation_name : "columns" : column_name : "result": result_key
         [0]            [1]                 [2]              [3]              [4]              [5]              [6]          [7]         [8]        [9]
        
        "urn" : "great_expectations" : "validations" : data_asset_name : "expectations" : expectation_name : "columns" : column_name : "details": details_key
         [0]            [1]                 [2]              [3]              [4]              [5]              [6]          [7]         [8]        [9]

        "urn" : "great_expectations" : "validations" : data_asset_name : "expectations" : expectation_name : "result": result_key
         [0]            [1]                 [2]              [3]              [4]              [5]            [6]          [7]  

        "urn" : "great_expectations" : "validations" : data_asset_name : "expectations" : expectation_name : "details": details_key
         [0]            [1]                 [2]              [3]              [4]              [5]             [6]          [7]  

         Parameters are compiled to the following structure:
         {
             "raw": <set of all parameters requested>
             "data_assets": {
                 data_asset_name: {
                    expectation_name: {
                        "details": <set of details parameter values requested>
                        "result": <set of result parameter values requested>
                        column_name: {
                            "details": <set of details parameter values requested>
                            "result": <set of result parameter values requested>
                        }
                    }
                 }
             }
         }

        """

        # Full recompilation every time
        self._compiled_parameters = {
            "raw": set(),
            "data_assets": {}
        }

        known_assets = self.list_expectations_configs()
        config_paths = [y for x in os.walk(self.directory) for y in glob(os.path.join(x[0], '*.json'))]

        for config_file in config_paths:
            config = json.load(open(config_file, 'r'))
            for expectation in config["expectations"]:
                for _, value in expectation["kwargs"].items():
                    if isinstance(value, dict) and '$PARAMETER' in value:
                        # Compile only respects parameters in urn structure beginning with urn:great_expectations:validations
                        if value["$PARAMETER"].startswith("urn:great_expectations:validations:"):
                            column_expectation = False
                            parameter = value["$PARAMETER"]
                            self._compiled_parameters["raw"].add(parameter)
                            param_parts = parameter.split(":")
                            try:
                                data_asset = param_parts[3]
                                expectation_name = param_parts[5]
                                if param_parts[6] == "columns":
                                    column_expectation = True
                                    column_name = param_parts[7]
                                    param_key = param_parts[8]
                                else:
                                    param_key = param_parts[6]
                            except IndexError:
                                logger.warning("Invalid parameter urn (not enough parts): %s" % parameter)

                            if data_asset not in known_assets:
                                logger.warning("Adding parameter %s for unknown data asset config" % parameter)

                            if data_asset not in self._compiled_parameters["data_assets"]:
                                self._compiled_parameters["data_assets"][data_asset] = {}

                            if expectation_name not in self._compiled_parameters["data_assets"][data_asset]:
                                self._compiled_parameters["data_assets"][data_asset][expectation_name] = {}

                            if column_expectation:
                                if "columns" not in self._compiled_parameters["data_assets"][data_asset][expectation_name]:
                                    self._compiled_parameters["data_assets"][data_asset][expectation_name]["columns"] = {}
                                if column_name not in self._compiled_parameters["data_assets"][data_asset][expectation_name]["columns"]:
                                    self._compiled_parameters["data_assets"][data_asset][expectation_name]["columns"][column_name] = {}
                                if param_key not in self._compiled_parameters["data_assets"][data_asset][expectation_name]["columns"][column_name]:
                                    self._compiled_parameters["data_assets"][data_asset][expectation_name]["columns"][column_name][param_key] = set()
                                self._compiled_parameters["data_assets"][data_asset][expectation_name]["columns"][column_name][param_key].add(parameter)   
                            
                            elif param_key in ["result", "details"]:
                                if param_key not in self._compiled_parameters["data_assets"][data_asset][expectation_name]:
                                    self._compiled_parameters["data_assets"][data_asset][expectation_name][param_key] = set()
                                self._compiled_parameters["data_assets"][data_asset][expectation_name][param_key].add(parameter)  
                            
                            else:
                                logger.warning("Invalid parameter urn (unrecognized structure): %s" % parameter)

        self._compiled = True

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

    def update_return_obj(self, data_asset, return_obj):
        if self._expectation_explorer:
            return self._expectation_explorer_manager.create_expectation_widget(data_asset, return_obj)
        else:
            return return_obj
