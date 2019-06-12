import os
import json
import logging
from ruamel.yaml import YAML
import sys
import copy
import errno
from glob import glob
from six import string_types
import datetime

from ..version import __version__
from ..util import safe_mmkdir, read_csv

import ipywidgets as widgets
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

from great_expectations.util import get_slack_callback
from great_expectations.data_asset import DataAsset
from great_expectations.dataset import PandasDataset
from great_expectations.datasource.sqlalchemy_source import SqlAlchemyDatasource
from great_expectations.datasource.dbt_source import DBTDatasource
from great_expectations.datasource import PandasDatasource
from great_expectations.datasource import SparkDFDatasource
from great_expectations.profile.pseudo_pandas_profiling import PseudoPandasProfiler

from .expectation_explorer import ExpectationExplorer

logger = logging.getLogger(__name__)
debug_view = widgets.Output(layout={'border': '3 px solid pink'})
yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False


class DataContext(object):
    """A DataContext represents a Great Expectations project. It captures essential information such as
    expectations configurations.

    The DataContext is configured via a yml file that should be stored in a file called great_expectations/great_expectations.yml
    under the context_root_dir passed during initialization.

    """

    def __init__(self, context_root_dir=None, expectation_explorer=False):
        self._expectation_explorer = expectation_explorer
        self._datasources = {}
        if expectation_explorer:
            self._expectation_explorer_manager = ExpectationExplorer()
        # determine the "context root directory" - this is the parent of "great_expectations" dir

        if context_root_dir is None:
            if (os.path.isdir("../notebooks") and os.path.isdir("../../great_expectations")
                    and os.path.isfile("../../great_expectations/great_expectations.yml")):
                self.context_root_directory = "../../"
            elif os.path.isdir("./great_expectations") and os.path.isfile("./great_expectations/great_expectations.yml"):
                self.context_root_directory = "./"
            else:
                raise("Unable to locate context root directory. Please provide a directory name.")

        self.context_root_directory = os.path.abspath(context_root_dir)

        self.expectations_directory = os.path.join(self.context_root_directory, "great_expectations/expectations")
        self.plugin_store_directory = os.path.join(self.context_root_directory, "great_expectations/plugins/store")
        sys.path.append(self.plugin_store_directory)
        
        self._project_config = self._load_project_config()
        if "datasources" not in self._project_config:
            self._project_config["datasources"] = {}
        for datasource in self._project_config["datasources"].keys():
            # TODO: if one of these loads fails, be okay with that
            self.get_datasource(datasource)

        self._load_evaluation_parameter_store()
        self._compiled = False

    def get_context_root_directory(self):
        return self.context_root_directory

    def _load_project_config(self):
        try:
            with open(os.path.join(self.context_root_directory, "great_expectations/great_expectations.yml"), "r") as data:
                return yaml.load(data)
        except IOError as e:
            if e.errno != errno.ENOENT:
                raise
            with open(os.path.join(self.context_root_directory, "great_expectations/great_expectations.yml"), "w") as template:
                template.write(PROJECT_TEMPLATE)

            with open(os.path.join(self.context_root_directory, "great_expectations/great_expectations.yml"),
                      "r") as template:
                base_config = yaml.load(template)

            return base_config

    def _save_project_config(self):
        with open(os.path.join(self.context_root_directory, "great_expectations/great_expectations.yml"), "w") as data:
            yaml.dump(self._project_config, data)

    def _get_all_profile_credentials(self):
        try:
            with open(os.path.join(self.context_root_directory, "great_expectations/uncommitted/credentials/profiles.yml"), "r") as profiles_file:
                return yaml.load(profiles_file) or {}
        except IOError as e:
            if e.errno != errno.ENOENT:
                raise
            logger.debug("Generating empty profile store.")
            base_profile_store = yaml.load("{}")
            base_profile_store.yaml_set_start_comment(PROFILE_COMMENT)
            return base_profile_store

    def get_profile_credentials(self, profile_name):
        profiles = self._get_all_profile_credentials()
        if profile_name in profiles:
            return profiles[profile_name]
        else:
            return {}

    def add_profile_credentials(self, profile_name, **kwargs):
        profiles = self._get_all_profile_credentials()
        profiles[profile_name] = dict(**kwargs)
        profiles_filepath = os.path.join(self.context_root_directory, "great_expectations/uncommitted/credentials/profiles.yml")
        safe_mmkdir(os.path.dirname(profiles_filepath), exist_ok=True)
        if not os.path.isfile(profiles_filepath):
            logger.info("Creating new profiles store at {profiles_filepath}".format(profiles_filepath=profiles_filepath))
        with open(profiles_filepath, "w") as profiles_file:
            yaml.dump(profiles, profiles_file)

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
                default_path_datasource_config = yaml.load(config_file) or {}
            datasource_config.update(default_path_datasource_config)
        except IOError as e:
            if e.errno != errno.ENOENT:
                raise
            logger.debug("No config file found in default location for datasource %s" % datasource_name)
        
        if defined_config_path is not None:
            try:
                with open(defined_config_path, "r") as config_file:
                    defined_path_datasource_config = yaml.load(config_file) or {}
                datasource_config.update(defined_path_datasource_config)
            except IOError as e:
                if e.errno != errno.ENOENT:
                    raise
                logger.warning("No config file found in user-defined location for datasource %s" % datasource_name)
        
        return datasource_config

    def list_available_data_asset_names(self, datasource_names=None, generator_names=None):
        data_asset_names = []
        if datasource_names is None:
            datasource_names = [datasource["name"] for datasource in self.list_datasources()]
        elif isinstance(datasource_names, string_types):
            datasource_names = [datasource_names]
        elif not isinstance(datasource_names, list):
            raise ValueError("Datasource names must be a datasource name, list of datasource anmes or None (to list all datasources)")
        
        if generator_names is not None:
            if isinstance(generator_names, string_types):
                generator_names = [generator_names]
            if len(generator_names) != len(datasource_names):
                raise ValueError("If providing generators, you must specify one generator for each datasource.")

        for idx, datasource_name in enumerate(datasource_names):
            datasource = self.get_datasource(datasource_name)
            data_asset_names.append(
                {
                    "datasource": datasource_name,
                    "generators": datasource.list_available_data_asset_names(generator_names[idx] if generator_names is not None else None) 
                }
            )
        return data_asset_names

    def get_batch(self, datasource_name, data_asset_name, batch_kwargs=None, **kwargs):
        data_asset_name = self._normalize_data_asset_name(data_asset_name)
        # datasource_name = find(data_asset_name.split("/")[0]
        datasource = self.get_datasource(datasource_name)
        if not datasource:
            raise Exception("Can't find datasource {0:s} in the config - please check your great_expectations.yml")

        data_asset = datasource.get_data_asset(data_asset_name, batch_kwargs, **kwargs)
        return data_asset

    def add_datasource(self, name, type_, **kwargs):
        datasource_class = self._get_datasource_class(type_)
        datasource = datasource_class(name=name, data_context=self, **kwargs)
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
            return PandasDatasource
        elif datasource_type == "dbt":
            return DBTDatasource
        elif datasource_type == "sqlalchemy":
            return SqlAlchemyDatasource
        elif datasource_type == "spark":
            return SparkDFDatasource
        else:
            try:
                # Update to do dynamic loading based on plugin types
                return PandasDatasource
            except ImportError:
                raise
 
    def get_datasource(self, datasource_name="default"):
        if datasource_name in self._datasources:
            return self._datasources[datasource_name]
        elif datasource_name in self._project_config["datasources"]:
            datasource_config = copy.deepcopy(self._project_config["datasources"][datasource_name])
        # elif len(self._project_config["datasources"]) == 1:
        #     datasource_name = list(self._project_config["datasources"])[0]
        #     datasource_config = copy.deepcopy(self._project_config["datasources"][datasource_name])
        else:
            raise ValueError("Unable to load datasource %s -- no configuration found or invalid configuration." % datasource_name)
        type_ = datasource_config.pop("type")
        datasource_class= self._get_datasource_class(type_)
        datasource = datasource_class(name=datasource_name, data_context=self, **datasource_config)
        self._datasources[datasource_name] = datasource
        return datasource
            
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
        root_path = self.expectations_directory
        result = [os.path.splitext(os.path.relpath(y, root_path))[0] for x in os.walk(root_path) for y in glob(os.path.join(x[0], '*.json'))]
        return result

    def list_datasources(self):
        return [{"name": key, "type": value["type"]} for key, value in self._project_config["datasources"].items()]

    def _normalize_data_asset_name(self, data_asset_name, batch_kwargs=None):
        configs = self.list_expectations_configs()
        if data_asset_name in configs:
            return data_asset_name
        else:
            last_found_config = None
            options = 0
            for config in configs:
                config_components = config.split("/")
                if data_asset_name in config:
                    options += 1
                    last_found_config = config
            if options == 1:
                return last_found_config

        # We allow "new" configs to be considered normalized out of the box
        return data_asset_name
        # raise ExpectationsConfigNotFoundError(data_asset_name)

    def get_expectations(self, data_asset_name, batch_kwargs=None):
        config_file_path = os.path.join(self.expectations_directory, data_asset_name + '.json')
        if os.path.isfile(config_file_path):
            with open(os.path.join(self.expectations_directory, data_asset_name + '.json')) as json_file:
                return json.load(json_file)
        else:
            # TODO: Should this return None? Currently this method acts as get_or_create
            return {
                'data_asset_name': data_asset_name,
                'meta': {
                    'great_expectations.__version__': __version__
                },
                'expectations': [],
             } 

    def save_expectations(self, expectations, data_asset_name=None):
        if data_asset_name is None:
            data_asset_name = expectations['data_asset_name']
        config_file_path = os.path.join(self.expectations_directory, data_asset_name + '.json')
        safe_mmkdir(os.path.split(config_file_path)[0], exist_ok=True)
        with open(config_file_path, 'w') as outfile:
            json.dump(expectations, outfile)
        self._compiled = False

    def bind_evaluation_parameters(self, run_id, expectations):
        # TOOO: only return parameters requested by the given expectations
        return self._evaluation_parameter_store.get_run_parameters(run_id)

    def register_validation_results(self, run_id, validation_results, data_asset=None):
        """Process results of a validation run, including registering evaluation parameters that are now available
        and storing results and snapshots if so configured."""

        # TODO: harmonize with data_asset_name logic below
        try:
            data_asset_name = validation_results["meta"]["data_asset_name"]
        except KeyError:
            logger.warning("No data_asset_name found in validation results; using '_untitled'")
            data_asset_name = "_untitled"

        if "result_store" in self._project_config:
            result_store = self._project_config["result_store"]
            if isinstance(result_store, dict) and "filesystem" in result_store:
                validation_filepath = os.path.join(self.context_root_directory, "great_expectations", result_store["filesystem"]["base_directory"],
                                       run_id, data_asset_name + ".json")
                logger.info("Storing validation result: %s" % validation_filepath)
                safe_mmkdir(os.path.join(self.context_root_directory, "great_expectations", result_store["filesystem"]["base_directory"], run_id))
                with open(validation_filepath, "w") as outfile:
                    json.dump(validation_results, outfile)
            if isinstance(result_store, dict) and "s3" in result_store:
                bucket = result_store["s3"]["bucket"]
                key_prefix = result_store["s3"]["key_prefix"]
                key = os.path.join(key_prefix, "validations/{run_id}/{data_asset_name}.json".format(run_id=run_id,
                                                                                     data_asset_name=data_asset_name))
                validation_results["meta"]["result_reference"] = "s3://{bucket}/{key}".format(bucket=bucket, key=key)
                try:
                    import boto3
                    s3 = boto3.resource('s3')
                    result_s3 = s3.Object(bucket, key)
                    result_s3.put(Body=json.dumps(validation_results).encode('utf-8'))
                except ImportError:
                    logger.error("Error importing boto3 for AWS support.")
                except Exception:
                    raise

        if "result_callback" in self._project_config:
            result_callback = self._project_config["result_callback"]
            if isinstance(result_callback, dict) and "slack" in result_callback:
                get_slack_callback(result_callback["slack"])(validation_results)
            else:
                logger.warning("Unrecognized result_callback configuration.")

        if "data_asset_snapshot_store" in self._project_config and validation_results["success"] is False:
            data_asset_snapshot_store = self._project_config["data_asset_snapshot_store"]
            if isinstance(data_asset, PandasDataset):
                if isinstance(data_asset_snapshot_store, dict) and "filesystem" in data_asset_snapshot_store:
                    logger.info("Storing dataset to file")
                    safe_mmkdir(os.path.join(self.context_root_directory, "great_expectations", data_asset_snapshot_store["filesystem"]["base_directory"], run_id))
                    data_asset.to_csv(os.path.join(self.context_root_directory, "great_expectations", data_asset_snapshot_store["filesystem"]["base_directory"],
                                                   run_id,
                                                   data_asset_name + ".csv.gz"), compression="gzip")

                if isinstance(data_asset_snapshot_store, dict) and "s3" in data_asset_snapshot_store:
                    bucket = data_asset_snapshot_store["s3"]["bucket"]
                    key_prefix = data_asset_snapshot_store["s3"]["key_prefix"]
                    key = key_prefix + "snapshots/{run_id}/{data_asset_name}".format(run_id=run_id,
                                                                                     data_asset_name=data_asset_name) + ".csv.gz"
                    validation_results["meta"]["data_asset_snapshot"] = "s3://{bucket}/{key}".format(bucket=bucket, key=key)

                    try:
                        import boto3
                        s3 = boto3.resource('s3')
                        result_s3 = s3.Object(bucket, key)
                        result_s3.put(Body=data_asset.to_csv(compression="gzip").encode('utf-8'))
                    except ImportError:
                        logger.error("Error importing boto3 for AWS support.")
                    except Exception:
                        raise
            else:
                logger.warning(
                    "Unable to save data_asset of type: %s. Only PandasDataset is supported." % type(data_asset))

        if not self._compiled:
            self._compile()

        if "meta" not in validation_results or "data_asset_name" not in validation_results["meta"]:
            logger.warning("No data_asset_name found in validation results; evaluation parameters cannot be registered.")
            return validation_results
        elif validation_results["meta"]["data_asset_name"] not in self._compiled_parameters["data_assets"]:
            # This is fine; short-circuit since we do not need to register any results from this dataset.
            return validation_results
        
        for result in validation_results['results']:
            # Unoptimized: loop over all results and check if each is needed
            expectation_type = result['expectation_config']['expectation_type']
            if expectation_type in self._compiled_parameters["data_assets"][data_asset_name]:
                # First, bind column-style parameters
                if (("column" in result['expectation_config']['kwargs']) and 
                    ("columns" in self._compiled_parameters["data_assets"][data_asset_name][expectation_type]) and 
                    (result['expectation_config']['kwargs']["column"] in
                     self._compiled_parameters["data_assets"][data_asset_name][expectation_type]["columns"])):

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

        return validation_results

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
        config_paths = [y for x in os.walk(self.expectations_directory) for y in glob(os.path.join(x[0], '*.json'))]

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

    def profile_datasource(self, datasource_name, profiler_name="PseudoPandasProfiling", max_data_assets=10):
        # FIXME: I've include
        # logger.info("Profiling %s with %s" % (datasource_name, profiler_name))
        print("Profiling %s with %s" % (datasource_name, profiler_name))
        datasource = self.get_datasource(datasource_name)
        data_asset_names = datasource.list_available_data_asset_names()

        #!!! Abe 2019/06/11: This seems brittle. I don't understand why this object is packaged this way.
        data_asset_name_list = list(data_asset_names[0]["available_data_asset_names"])
        # logger.info("Found %d named data assets" % (len(data_asset_name_list)))
        print("Found %d named data assets" % (len(data_asset_name_list)))
        
        if max_data_assets == None or max_data_assets >= len(data_asset_name_list):
            # logger.info("Profiling all %d." % (len(data_asset_name_list)))
            print("Profiling all %d." % (len(data_asset_name_list)))
        else:
            # logger.info("Profiling the first %d, alphabetically." % (max_data_assets))
            print("Profiling the first %d, alphabetically." % (max_data_assets))
            data_asset_name_list.sort()
            data_asset_name_list = data_asset_name_list[:max_data_assets]

        for name in data_asset_name_list:
            try:
                start_time = datetime.datetime.now()

                #FIXME: There needs to be an affordance here to limit to 100 rows, or downsample, etc.
                batch = self.get_batch(datasource_name=datasource_name, data_asset_name=name)

                # expectations_config, evr_config = PseudoPandasProfiler.profile(batch)
                expectations_config = PseudoPandasProfiler.profile(batch)
                self.save_expectations(expectations_config, name)
                
                duration = (datetime.datetime.now() - start_time).total_seconds()

                print("\tProfiled %d rows from %s (%.3f sec)" % (batch.shape[0], name, duration))

            #!!! FIXME: THIS IS WAAAAY TO GENERAL. As soon as BatchKwargsError is fully implemented, we'll want to switch to that.
            except:
                #!!! FIXME: This error message could be a lot more helpful than it is
                print("\tWARNING: Unable to load %s. Skipping profiling." % (name))



PROJECT_HELP_COMMENT = """# Welcome to great expectations. 
# This project configuration file allows you to define datasources, 
# generators, integrations, and other configuration artifacts that
# make it easier to use Great Expectations.

# For more help configuring great expectations, 
# see the documentation at: https://greatexpectations.io/config_file.html

"""

PROJECT_OPTIONAL_CONFIG_COMMENT = """

# Configure additional data context options here.

# Uncomment the lines below to enable s3 as a result store. If a result store is enabled,
# validation results will be saved in the store according to run id.

# For S3, ensure that appropriate credentials or assume_role permissions are set where
# validation happens.


result_store:
  filesystem:
    base_directory: uncommitted/validations/
#   s3:
#     bucket: <your bucket>
#     key_prefix: <your key prefix>
#   

# Uncomment the lines below to enable a result callback.

# result_callback:
#   slack: https://slack.com/replace_with_your_webhook
    
    
# Uncomment the lines below to save snapshots of data assets that fail validation.

# data_asset_snapshot_store:
#   filesystem:
#     base_directory: uncommitted/snapshots/
#   s3:
#     bucket:
#     key_prefix:

"""

PROJECT_TEMPLATE = PROJECT_HELP_COMMENT + "datasources: {}\n" + PROJECT_OPTIONAL_CONFIG_COMMENT


PROFILE_COMMENT = """This file stores profiles with database access credentials. 
Do not commit this file to version control. 

A profile can optionally have a single parameter called 
"url" which will be passed to sqlalchemy's create_engine.

Otherwise, all credential options specified here for a 
given profile will be passed to sqlalchemy's create URL function.

"""