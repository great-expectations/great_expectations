# -*- coding: utf-8 -*-

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
import shutil
from collections import OrderedDict

from .util import NormalizedDataAssetName, get_slack_callback, safe_mmkdir

from great_expectations.exceptions import DataContextError, ConfigNotFoundError, ProfilerError

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

from great_expectations.data_asset.util import get_empty_expectation_suite
from great_expectations.dataset import Dataset, PandasDataset
from great_expectations.datasource import (
    PandasDatasource,
    SqlAlchemyDatasource,
    SparkDFDatasource,
    DBTDatasource
)
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
from great_expectations.render.renderer import DescriptivePageRenderer, PrescriptivePageRenderer
from great_expectations.render.view import (
    DefaultJinjaPageView,
    DefaultJinjaIndexPageView,
)


from .expectation_explorer import ExpectationExplorer

logger = logging.getLogger(__name__)
yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False

ALLOWED_DELIMITERS = ['.', '/']


class DataContext(object):
    """A DataContext represents a Great Expectations project. It organizes storage and access for
    expectation suites, datasources, notification settings, and data fixtures.

    The DataContext is configured via a yml file stored in a directory called great_expectations; the configuration file
    as well as managed expectation suites should be stored in version control.

    Use the `create` classmethod to create a new empty config, or instantiate the DataContext
    by passing the path to an existing data context root directory.

    DataContexts use data sources you're already familiar with. Generators help introspect data stores and data execution
    frameworks (such as airflow, Nifi, dbt, or dagster) to describe and produce batches of data ready for analysis. This
    enables fetching, validation, profiling, and documentation of  your data in a way that is meaningful within your
    existing infrastructure and work environment.

    DataContexts use a datasource-based namespace, where each accessible type of data has a three-part
    normalized *data_asset_name*, consisting of *datasource/generator/generator_asset*.

    - The datasource actually connects to a source of materialized data and returns Great Expectations DataAssets \
      connected to a compute environment and ready for validation.

    - The Generator knows how to introspect datasources and produce identifying "batch_kwargs" that define \
      particular slices of data.

    - The generator_asset is a specific name -- often a table name or other name familiar to users -- that \
      generators can slice into batches.

    An expectation suite is a collection of expectations ready to be applied to a batch of data. Since
    in many projects it is useful to have different expectations evaluate in different contexts--profiling
    vs. testing; warning vs. error; high vs. low compute; ML model or dashboard--suites provide a namespace
    option for selecting which expectations a DataContext returns.

    In many simple projects, the datasource or generator name may be omitted and the DataContext will infer
    the correct name when there is no ambiguity.

    Similarly, if no expectation suite name is provided, the DataContext will assume the name "default".
    """

    @classmethod
    def create(cls, project_root_dir=None):
        """Build a new great_expectations directory and DataContext object in the provided project_root_dir.

        `create` will not create a new "great_expectations" directory in the provided folder, provided one does not
        already exist. Then, it will initialize a new DataContext in that folder and write the resulting config.

        Args:
            project_root_dir: path to the root directory in which to create a new great_expectations directory

        Returns:
            DataContext
        """
        if not os.path.isdir(project_root_dir):
            raise DataContextError("project_root_dir must be a directory in which to initialize a new DataContext")
        else:
            try:
                os.mkdir(os.path.join(project_root_dir, "great_expectations"))
            except (FileExistsError, OSError):
                raise DataContextError(
                    "Cannot create a DataContext object when a great_expectations directory "
                    "already exists at the provided root directory.")

            with open(os.path.join(project_root_dir, "great_expectations/great_expectations.yml"), "w") as template:
                template.write(PROJECT_TEMPLATE)

        return cls(os.path.join(project_root_dir, "great_expectations"))
            
    def __init__(self, context_root_dir=None, expectation_explorer=False, data_asset_name_delimiter='/'):
        """DataContext constructor

        Args:
            context_root_dir: location to look for the ``great_expectations.yml`` file. If None, searches for the file \
            based on conventions for project subdirectories.
            expectation_explorer: If True, load the expectation explorer manager, which will modify GE return objects \
            to include ipython notebook widgets.
            data_asset_name_delimiter: the delimiter character to use when parsing data_asset_name parameters. \
            Defaults to '/'

        Returns:
            None
        """
        self._expectation_explorer = expectation_explorer
        self._datasources = {}
        if expectation_explorer:
            self._expectation_explorer_manager = ExpectationExplorer()

        # determine the "context root directory" - this is the parent of "great_expectations" dir
        if context_root_dir is None:
            if os.path.isdir("../notebooks") and os.path.isfile("../great_expectations.yml"):
                context_root_dir = "../"
            elif os.path.isdir("./great_expectations") and \
                    os.path.isfile("./great_expectations/great_expectations.yml"):
                context_root_dir = "./great_expectations"
            elif os.path.isdir("./") and os.path.isfile("./great_expectations.yml"):
                context_root_dir = "./"
            else:
                raise(
                    "Unable to locate context root directory. Please provide a directory name."
                )

        self._context_root_directory = os.path.abspath(context_root_dir)

        # TODO: these paths should be configurable
        self.expectations_directory = os.path.join(self.root_directory, "expectations")
        self.fixtures_validations_directory = os.path.join(self.root_directory, "fixtures/validations")
        self.data_doc_directory = os.path.join(self.root_directory, "uncommitted/documentation")
        self.plugin_store_directory = os.path.join(self.root_directory, "plugins/store")
        sys.path.append(self.plugin_store_directory)
        
        self._project_config = self._load_project_config()

        if "datasources" not in self._project_config:
            self._project_config["datasources"] = {}
        for datasource in self._project_config["datasources"].keys():
            self.get_datasource(datasource)

        self._load_evaluation_parameter_store()
        self._compiled = False
        if data_asset_name_delimiter not in ALLOWED_DELIMITERS:
            raise DataContextError("Invalid delimiter: delimiter must be '.' or '/'")
        self._data_asset_name_delimiter = data_asset_name_delimiter

    @property
    def root_directory(self):
        """The root directory for configuration objects in the data context; the location in which
        ``great_expectations.yml`` is located."""
        return self._context_root_directory

    def _load_project_config(self):
        """Loads the project configuration file."""
        try:
            with open(os.path.join(self.root_directory, "great_expectations.yml"), "r") as data:
                return yaml.load(data)
        except IOError:
            raise ConfigNotFoundError(self.root_directory)

    @property
    def data_asset_name_delimiter(self):
        """Configurable delimiter character used to parse data asset name strings into \
        ``NormalizedDataAssetName`` objects."""
        return self._data_asset_name_delimiter
    
    @data_asset_name_delimiter.setter
    def data_asset_name_delimiter(self, new_delimiter):
        """data_asset_name_delimiter property setter method"""
        if new_delimiter not in ALLOWED_DELIMITERS:
            raise DataContextError("Invalid delimiter: delimiter must be '.' or '/'")
        else:
            self._data_asset_name_delimiter = new_delimiter

    def get_validation_location(self, data_asset_name, expectation_suite_name, run_id):
        """Get the local path where a validation result is stored, given full asset name and run id

        Args:
            data_asset_name: name of data asset for which to get validation location
            expectation_suite_name: name of expectation suite for which to get validation location
            run_id: run_id of validation to get. If no run_id is specified, fetch the latest run_id according to \
            alphanumeric sort (by default, the latest run_id if using ISO 8601 formatted timestamps for run_id

        Returns:
            path (str): path to the validation location for the specified data_asset, expectation_suite and run_id
        """
        result = {}

        if "result_store" not in self._project_config:
            logger.warning("Unable to get validation results: no result store configured.")
            return {}

        data_asset_name = self._normalize_data_asset_name(data_asset_name)
        result_store = self._project_config["result_store"]
        if "filesystem" in result_store and isinstance(result_store["filesystem"], dict):
            if "base_directory" not in result_store["filesystem"]:
                raise DataContextError(
                    "Invalid result_store configuration: 'base_directory' is required for a filesystem store.")

            base_directory = result_store["filesystem"]["base_directory"]
            if not os.path.isabs(base_directory):
                base_directory = os.path.join(self.root_directory, base_directory)

            if run_id is None:  # Get most recent run_id
                runs = [name for name in os.listdir(base_directory) if
                        os.path.isdir(os.path.join(base_directory, name))]
                run_id = sorted(runs)[-1]

            validation_path = os.path.join(
                base_directory,
                run_id,
                self._get_normalized_data_asset_name_filepath(
                    data_asset_name, 
                    expectation_suite_name,
                    base_path=""
                )
            )

            result['filepath'] = validation_path

        elif "s3" in result_store and isinstance(result_store["s3"], dict):
            # FIXME: this code is untested
            if "bucket" not in result_store["s3"] or "key_prefix" not in result_store["s3"]:
                raise DataContextError(
                    "Invalid result_store configuration: 'bucket' and 'key_prefix' are required for an s3 store.")

            try:
                import boto3
                s3 = boto3.client('s3')
            except ImportError:
                raise ImportError("boto3 is required for retrieving a dataset from s3")

            bucket = result_store["s3"]["bucket"]
            key_prefix = result_store["s3"]["key_prefix"]

            if run_id is None:  # Get most recent run_id
                all_objects = s3.list_objects(Bucket=bucket)
                # Remove the key_prefix and first slash from the name
                validations = [
                    name[len(key_prefix) + 1:] 
                    for name in all_objects 
                    if name.startswith(key_prefix) and len(name) > len(key_prefix) + 1
                ]
                # run id is the first section after the word "validations"
                runs = [validation.split('/')[1] for validation in validations]
                run_id = sorted(runs)[-1]

            key = os.path.join(
                key_prefix,
                "validations",
                run_id,
                self._get_normalized_data_asset_name_filepath(
                    data_asset_name,
                    expectation_suite_name,
                    base_path=""
                )
            )

            result['bucket'] = bucket
            result['key'] = key

        else:
            raise DataContextError("Invalid result_store configuration: only 'filesystem' and 's3' are supported.")

        return result

    def get_validation_doc_filepath(self, data_asset_name, expectation_suite_name):
        """Get the local path where a the rendered html doc for a validation result is stored, given full asset name.

        Args:
            data_asset_name: name of data asset for which to get documentation filepath
            expectation_suite_name: name of expectation suite for which to get validation location

        Returns:
            path (str): Path to the location

        """
        # TODO: this path should be configurable or parameterized to support descriptive and prescriptive docs
        validation_filepath = self._get_normalized_data_asset_name_filepath(
            data_asset_name,
            expectation_suite_name,
            base_path=self.data_doc_directory,
            file_extension=".html"
        )

        return validation_filepath

    def move_validation_to_fixtures(self, data_asset_name, expectation_suite_name, run_id):
        """
        Move validation results from uncommitted to fixtures/validations to make available for the data doc renderer

        Args:
            data_asset_name: name of data asset for which to get documentation filepath
            expectation_suite_name: name of expectation suite for which to get validation location
            run_id: run_id of validation to get. If no run_id is specified, fetch the latest run_id according to \
            alphanumeric sort (by default, the latest run_id if using ISO 8601 formatted timestamps for run_id


        Returns:
            None
        """
        source_filepath = self.get_validation_location(data_asset_name, expectation_suite_name, run_id)['filepath']

        destination_filepath = self._get_normalized_data_asset_name_filepath(
            data_asset_name,
            expectation_suite_name,
            base_path=self.fixtures_validations_directory,
            file_extension=".json"
        )

        safe_mmkdir(os.path.dirname(destination_filepath))
        shutil.move(source_filepath, destination_filepath)

    #####
    #
    # Internal helper methods
    #
    #####

    def _get_normalized_data_asset_name_filepath(self, data_asset_name,
                                                 expectation_suite_name,
                                                 base_path=None,
                                                 file_extension=".json"):
        """Get the path where the project-normalized data_asset_name expectations are stored. This method is used
        internally for constructing all absolute and relative paths for asset_name-based paths.

        Args:
            data_asset_name: name of data asset for which to construct the path
            expectation_suite_name: name of expectation suite for which to construct the path
            base_path: base path from which to construct the path. If None, uses the DataContext root directory
            file_extension: the file extension to append to the path

        Returns:
            path (str): path for the requsted object.
        """
        if base_path is None:
            base_path = os.path.join(self.root_directory, "expectations")

        # We need to ensure data_asset_name is a valid filepath no matter its current state
        if isinstance(data_asset_name, NormalizedDataAssetName):
            name_parts = [name_part.replace("/", "__") for name_part in data_asset_name]
            relative_path = "/".join(name_parts)
        elif isinstance(data_asset_name, string_types):
            # if our delimiter is not '/', we need to first replace any slashes that exist in the name
            # to avoid extra layers of nesting (e.g. for dbt models)
            relative_path = data_asset_name
            if self.data_asset_name_delimiter != "/":
                relative_path.replace("/", "__")
                relative_path = relative_path.replace(self.data_asset_name_delimiter, "/")
        else:
            raise DataContextError("data_assset_name must be a NormalizedDataAssetName or string")

        expectation_suite_name += file_extension

        return os.path.join(
            base_path,
            relative_path,
            expectation_suite_name
        )

    def _save_project_config(self):
        """Save the current project to disk."""
        with open(os.path.join(self.root_directory, "great_expectations.yml"), "w") as data:
            yaml.dump(self._project_config, data)

    def _get_all_profile_credentials(self):
        """Get all profile credentials from the default location."""

        # TODO: support parameterized additional store locations
        try:
            with open(os.path.join(self.root_directory,
                                   "uncommitted/credentials/profiles.yml"), "r") as profiles_file:
                return yaml.load(profiles_file) or {}
        except IOError as e:
            if e.errno != errno.ENOENT:
                raise
            logger.debug("Generating empty profile store.")
            base_profile_store = yaml.load("{}")
            base_profile_store.yaml_set_start_comment(PROFILE_COMMENT)
            return base_profile_store

    def get_profile_credentials(self, profile_name):
        """Get named profile credentials.

        Args:
            profile_name (str): name of the profile for which to get credentials

        Returns:
            credentials (dict): dictionary of credentials
        """
        profiles = self._get_all_profile_credentials()
        if profile_name in profiles:
            return profiles[profile_name]
        else:
            return {}

    def add_profile_credentials(self, profile_name, **kwargs):
        """Add named profile credentials.

        Args:
            profile_name: name of the profile for which to add credentials
            **kwargs: credential key-value pairs

        Returns:
            None
        """
        profiles = self._get_all_profile_credentials()
        profiles[profile_name] = dict(**kwargs)
        profiles_filepath = os.path.join(self.root_directory, "uncommitted/credentials/profiles.yml")
        safe_mmkdir(os.path.dirname(profiles_filepath), exist_ok=True)
        if not os.path.isfile(profiles_filepath):
            logger.info("Creating new profiles store at {profiles_filepath}".format(
                profiles_filepath=profiles_filepath)
            )
        with open(profiles_filepath, "w") as profiles_file:
            yaml.dump(profiles, profiles_file)

    def get_datasource_config(self, datasource_name):
        """Get the configuration for a configured datasource

        Args:
            datasource_name: The datasource for which to get the config

        Returns:
            datasource_config (dict): dictionary containing datasource configuration
        """

        # TODO: Review logic, once described below but not implemented in datasource save, for splitting configuration
        # We allow a datasource to be defined in any combination of the following ways:

        # 1. It may be fully specified in the datasources section of the great_expectations.yml file
        # 2. It may be stored in a file by convention located in `datasources/<datasource_name>/config.yml`
        # 3. It may be listed in the great_expectations.yml file with a config_file key that provides a relative \
        # path to a different yml config file

        # Any key duplicated across configs will be updated by the last key read (in the order above)
        datasource_config = {}
        defined_config_path = None
        default_config_path = os.path.join(self.root_directory, "datasources", datasource_name, "config.yml")
        if datasource_name in self._project_config["datasources"]:
            base_datasource_config = copy.deepcopy(self._project_config["datasources"][datasource_name])
            if "config_file" in base_datasource_config:
                defined_config_path = os.path.join(self.root_directory, base_datasource_config.pop("config_file"))
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

    def get_available_data_asset_names(self, datasource_names=None, generator_names=None):
        """Inspect datasource and generators to provide available data_asset objects.

        Args:
            datasource_names: list of datasources for which to provide available data_asset_name objects. If None, \
            return available data assets for all datasources.
            generator_names: list of generators for which to provide available data_asset_name objects.

        Returns:
            data_asset_names (dict): Dictionary describing available data assets
            ::

                {
                  datasource_name: {
                    generator_name: [ data_asset_1, data_asset_2, ... ]
                    ...
                  }
                  ...
                }

        """
        data_asset_names = {}
        if datasource_names is None:
            datasource_names = [datasource["name"] for datasource in self.list_datasources()]
        elif isinstance(datasource_names, string_types):
            datasource_names = [datasource_names]
        elif not isinstance(datasource_names, list):
            raise ValueError(
                "Datasource names must be a datasource name, list of datasource names or None (to list all datasources)"
            )
        
        if generator_names is not None:
            if isinstance(generator_names, string_types):
                generator_names = [generator_names]
            if len(generator_names) == len(datasource_names): # Iterate over both together
                for idx, datasource_name in enumerate(datasource_names):
                    datasource = self.get_datasource(datasource_name)
                    data_asset_names[datasource_name] = \
                        datasource.get_available_data_asset_names(generator_names[idx])

            elif len(generator_names) == 1:
                datasource = self.get_datasource(datasource_names[0])
                datasource_names[datasource_names[0]] = datasource.get_available_data_asset_names(generator_names)

            else:
                raise ValueError(
                    "If providing generators, you must either specify one generator for each datasource or only "
                    "one datasource."
                )
        else: # generator_names is None
            for datasource_name in datasource_names:
                datasource = self.get_datasource(datasource_name)
                data_asset_names[datasource_name] = datasource.get_available_data_asset_names(None)

        return data_asset_names

    def get_batch(self, data_asset_name, expectation_suite_name="default", batch_kwargs=None, **kwargs):
        """
        Get a batch of data from the specified data_asset_name. Attaches the named expectation_suite, and uses the \
        provided batch_kwargs.

        Args:
            data_asset_name: name of the data asset. The name will be normalized. \
            (See :py:meth:`_normalize_data_asset_name` )
            expectation_suite_name: name of the expectation suite to attach to the data_asset returned
            batch_kwargs: key-value pairs describing the batch of data the datasource should fetch. \
            (See :class:`BatchGenerator` ) If no batch_kwargs are specified, then the context will get the next
            available batch_kwargs for the data_asset.
            **kwargs: additional key-value pairs to pass to the datasource when fetching the batch.

        Returns:
            Great Expectations data_asset with attached expectation_suite and DataContext
        """
        normalized_data_asset_name = self._normalize_data_asset_name(data_asset_name)

        datasource = self.get_datasource(normalized_data_asset_name.datasource)
        if not datasource:
            raise DataContextError(
                "Can't find datasource {0:s} in the config - please check your great_expectations.yml"
            )

        data_asset = datasource.get_batch(normalized_data_asset_name,
                                          expectation_suite_name,
                                          batch_kwargs,
                                          **kwargs)
        return data_asset

    def add_datasource(self, name, type_, **kwargs):
        """Add a new datasource to the data context.

        The type\_ parameter must match one of the recognized types for the DataContext

        Args:
            name (str): the name for the new datasource to add
            type_ (str): the type of datasource to add

        Returns:
            datasource (Datasource)
        """
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
        """Get the named datasource

        Args:
            datasource_name (str): the name of the datasource from the configuration

        Returns:
            datasource (Datasource)
        """
        if datasource_name in self._datasources:
            return self._datasources[datasource_name]
        elif datasource_name in self._project_config["datasources"]:
            datasource_config = copy.deepcopy(self._project_config["datasources"][datasource_name])
        # elif len(self._project_config["datasources"]) == 1:
        #     datasource_name = list(self._project_config["datasources"])[0]
        #     datasource_config = copy.deepcopy(self._project_config["datasources"][datasource_name])
        else:
            raise ValueError(
                "Unable to load datasource %s -- no configuration found or invalid configuration." % datasource_name
            )
        type_ = datasource_config.pop("type")
        datasource_class= self._get_datasource_class(type_)
        datasource = datasource_class(name=datasource_name, data_context=self, **datasource_config)
        self._datasources[datasource_name] = datasource
        return datasource
            
    def _load_evaluation_parameter_store(self):
        """Load the evaluation parameter store to use for managing cross data-asset parameterized expectations.

        By default, the Context uses an in-memory parameter store only suitable for evaluation on a single node.

        Returns:
            None
        """
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
                if run_id not in self.dict:
                    self.dict[run_id] = {}
                self.dict[run_id][name] = value

            def get_run_parameters(self, run_id):
                if run_id in self.dict:
                    return self.dict[run_id]
                else:
                    return {}

        #####
        #
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
        #
        #####
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
        except Exception:
            logger.exception("Failed to load evaluation_parameter_store class")
            raise

    def list_expectation_suites(self):
        """Returns currently-defined expectation suites available in a nested dictionary structure
        reflecting the namespace provided by this DataContext.

        Returns:
            Dictionary of currently-defined expectation suites::

            {
              datasource: {
                generator: {
                  generator_asset: [list_of_expectation_suites]
                }
              }
              ...
            }

        """

        expectation_suites_dict = {}

        # First, we construct the *actual* defined expectation suites
        for datasource in os.listdir(self.expectations_directory):
            datasource_path = os.path.join(self.expectations_directory, datasource)
            if not os.path.isdir(datasource_path):
                continue
            if datasource not in expectation_suites_dict:
                expectation_suites_dict[datasource] = {}
            for generator in os.listdir(datasource_path):
                generator_path = os.path.join(datasource_path, generator)
                if not os.path.isdir(generator_path):
                    continue
                if generator not in expectation_suites_dict[datasource]:
                    expectation_suites_dict[datasource][generator] = {}
                for generator_asset in os.listdir(generator_path):
                    generator_asset_path = os.path.join(generator_path, generator_asset)
                    if os.path.isdir(generator_asset_path):
                        candidate_suites = os.listdir(generator_asset_path)
                        expectation_suites_dict[datasource][generator][generator_asset] = [
                            suite_name[:-5] for suite_name in candidate_suites if suite_name.endswith(".json")
                        ]

        return expectation_suites_dict

    def list_datasources(self):
        """List currently-configured datasources on this context.

        Returns:
            List(dict): each dictionary includes "name" and "type" keys
        """
        return [{"name": key, "type": value["type"]} for key, value in self._project_config["datasources"].items()]

    def _normalize_data_asset_name(self, data_asset_name):
        """Normalizes data_asset_names for a data context.
        
        A data_asset_name is defined per-project and consists of three components that together define a "namespace"
        for data assets, encompassing both expectation suites and batches.

        Within a namespace, an expectation suite effectively defines candidate "types" for batches of data, and
        validating a batch of data determines whether that instance is of the candidate type.

        The data_asset_name namespace consists of three components:

          - a datasource name
          - a generator_name
          - a generator_asset

        It has a string representation consisting of each of those components delimited by a character defined in the
        data_context ('/' by default).

        Args:
            data_asset_name (str): The (unnormalized) data asset name to normalize. The name will be split \
                according to the currently-configured data_asset_name_delimiter

        Returns:
            NormalizedDataAssetName
        """
        if isinstance(data_asset_name, NormalizedDataAssetName):
            return data_asset_name

        split_name = data_asset_name.split(self.data_asset_name_delimiter)
        existing_expectation_suites = self.list_expectation_suites()
        existing_namespaces = []
        for datasource in existing_expectation_suites.keys():
            for generator in existing_expectation_suites[datasource].keys():
                for generator_asset in existing_expectation_suites[datasource][generator]:
                    existing_namespaces.append(
                        NormalizedDataAssetName(
                            datasource,
                            generator,
                            generator_asset
                        )
                    )

        if len(split_name) > 3:
            raise DataContextError(
                "Invalid data_asset_name '{data_asset_name}': found too many components using delimiter '{delimiter}'"
                .format(
                        data_asset_name=data_asset_name,
                        delimiter=self.data_asset_name_delimiter
                )
            )
        
        elif len(split_name) == 1:
            # In this case, the name *must* refer to a unique data_asset_name
            provider_names = set()
            generator_asset = split_name[0]
            for normalized_identifier in existing_namespaces:
                curr_generator_asset = normalized_identifier[2]
                if generator_asset == curr_generator_asset:
                    provider_names.add(
                        normalized_identifier
                    )

            # NOTE: Current behavior choice is to continue searching to see whether the namespace is ambiguous
            # based on configured generators *even* if there is *only one* namespace with expectation suites
            # in it.

            # If generators' namespaces are enormous or if they are slow to provide all their available names,
            # that behavior could become unwieldy, and perhaps should be revisited by using the escape hatch
            # commented out below.

            # if len(provider_names) == 1:
            #     return provider_names[0]
            #
            # elif len(provider_names) > 1:
            #     raise DataContextError(
            #         "Ambiguous data_asset_name '{data_asset_name}'. Multiple candidates found: {provider_names}"
            #         .format(data_asset_name=data_asset_name, provider_names=provider_names)
            #     )
                    
            available_names = self.get_available_data_asset_names()
            for datasource in available_names.keys():
                for generator in available_names[datasource].keys():
                    names_set = available_names[datasource][generator]
                    if generator_asset in names_set:
                        provider_names.add(
                            NormalizedDataAssetName(datasource, generator, generator_asset)
                        )
            
            if len(provider_names) == 1:
                return provider_names.pop()

            elif len(provider_names) > 1:
                raise DataContextError(
                    "Ambiguous data_asset_name '{data_asset_name}'. Multiple candidates found: {provider_names}"
                    .format(data_asset_name=data_asset_name, provider_names=provider_names)
                )

            # If we are here, then the data_asset_name does not belong to any configured datasource or generator
            # If there is only a single datasource and generator, we assume the user wants to create a new
            # namespace.
            if (len(available_names.keys()) == 1 and  # in this case, we know that the datasource name is valid
                    len(available_names[datasource].keys()) == 1):
                logger.info("Normalizing to a new generator name.")
                return NormalizedDataAssetName(
                    datasource,
                    generator,
                    generator_asset
                )

            if len(available_names.keys()) == 0:
                raise DataContextError(
                    "No datasource configured: a datasource is required to normalize an incomplete data_asset_name"
                )

            raise DataContextError(
                "Ambiguous data_asset_name: no existing data_asset has the provided name, no generator provides it, "
                " and there are multiple datasources and/or generators configured."
            )

        elif len(split_name) == 2:
            # In this case, the name must be a datasource_name/generator_asset

            # If the data_asset_name is already defined by a config in that datasource, return that normalized name.
            provider_names = set()
            for normalized_identifier in existing_namespaces:
                curr_datasource_name = normalized_identifier[0]
                curr_generator_asset = normalized_identifier[2]
                if curr_datasource_name == split_name[0] and curr_generator_asset == split_name[1]:
                    provider_names.add(normalized_identifier)

            # NOTE: Current behavior choice is to continue searching to see whether the namespace is ambiguous
            # based on configured generators *even* if there is *only one* namespace with expectation suites
            # in it.

            # If generators' namespaces are enormous or if they are slow to provide all their available names,
            # that behavior could become unwieldy, and perhaps should be revisited by using the escape hatch
            # commented out below.

            # if len(provider_names) == 1:
            #     return provider_names[0]
            #
            # elif len(provider_names) > 1:
            #     raise DataContextError(
            #         "Ambiguous data_asset_name '{data_asset_name}'. Multiple candidates found: {provider_names}"
            #         .format(data_asset_name=data_asset_name, provider_names=provider_names)
            #     )

            available_names = self.get_available_data_asset_names()
            for datasource_name in available_names.keys():
                for generator in available_names[datasource_name].keys():
                    generator_assets = available_names[datasource_name][generator]
                    if split_name[0] == datasource_name and split_name[1] in generator_assets:
                        provider_names.add(NormalizedDataAssetName(datasource_name, generator, split_name[1]))

            if len(provider_names) == 1:
                return provider_names.pop()
            
            elif len(provider_names) > 1:
                raise DataContextError(
                    "Ambiguous data_asset_name '{data_asset_name}'. Multiple candidates found: {provider_names}"
                    .format(data_asset_name=data_asset_name, provider_names=provider_names)
                )

            # If we are here, then the data_asset_name does not belong to any configured datasource or generator
            # If there is only a single generator for their provided datasource, we allow the user to create a new
            # namespace.
            if split_name[0] in available_names and len(available_names[split_name[0]]) == 1:
                logger.info("Normalizing to a new generator name.")
                return NormalizedDataAssetName(
                    split_name[0],
                    list(available_names[split_name[0]].keys())[0],
                    split_name[1]
                )

            if len(available_names.keys()) == 0:
                raise DataContextError(
                    "No datasource configured: a datasource is required to normalize an incomplete data_asset_name"
                )

            raise DataContextError(
                "No generator available to produce data_asset_name '{data_asset_name}' "
                "with datasource '{datasource_name}'"
                .format(data_asset_name=data_asset_name, datasource_name=datasource_name)
            )

        elif len(split_name) == 3:
            # In this case, we *do* check that the datasource and generator names are valid, but
            # allow the user to define a new generator asset
            datasources = [datasource["name"] for datasource in self.list_datasources()]
            if split_name[0] in datasources:
                datasource = self.get_datasource(split_name[0])
                generators = [generator["name"] for generator in datasource.list_generators()]
                if split_name[1] in generators:
                    return NormalizedDataAssetName(*split_name)

            raise DataContextError(
                "Invalid data_asset_name: no configured datasource '{datasource_name}' "
                "with generator '{generator_name}'"
                .format(datasource_name=split_name[0], generator_name=split_name[1])
            )

    def get_expectation_suite(self, data_asset_name, expectation_suite_name="default"):
        """Get or create a named expectation suite for the provided data_asset_name.

        Args:
            data_asset_name (str or NormalizedDataAssetName): the data asset name to which the expectation suite belongs
            expectation_suite_name (str): the name for the expectation suite

        Returns:
            expectation_suite
        """
        if not isinstance(data_asset_name, NormalizedDataAssetName):
            data_asset_name = self._normalize_data_asset_name(data_asset_name)

        config_file_path = self._get_normalized_data_asset_name_filepath(data_asset_name, expectation_suite_name)
        if os.path.isfile(config_file_path):
            with open(config_file_path, 'r') as json_file:
                read_config = json.load(json_file)
            # update the data_asset_name to correspond to the current name (in case the config has been moved/renamed)
            read_config["data_asset_name"] = self.data_asset_name_delimiter.join(data_asset_name)
            return read_config
        else:
            return get_empty_expectation_suite(
                self.data_asset_name_delimiter.join(data_asset_name),
                expectation_suite_name
            )

    def save_expectation_suite(self, expectation_suite, data_asset_name=None, expectation_suite_name=None):
        """Save the provided expectation suite into the DataContext.

        Args:
            expectation_suite: the suite to save
            data_asset_name: the data_asset_name for this expectation suite. If no name is provided, the name will\
                be read from the suite
            expectation_suite_name: the name of this expectation suite. If no name is provided the name will \
                be read from the suite

        Returns:
            None
        """
        if data_asset_name is None:
            try:
                data_asset_name = expectation_suite['data_asset_name']
            except KeyError:
                raise DataContextError(
                    "data_asset_name must either be specified or present in the provided expectation suite")
        if expectation_suite_name is None:
            try:
                expectation_suite_name = expectation_suite['expectation_suite_name']
            except KeyError:
                raise DataContextError(
                    "expectation_suite_name must either be specified or present in the provided expectation suite")
        if not isinstance(data_asset_name, NormalizedDataAssetName):
            data_asset_name = self._normalize_data_asset_name(data_asset_name)
        config_file_path = self._get_normalized_data_asset_name_filepath(data_asset_name, expectation_suite_name)
        safe_mmkdir(os.path.dirname(config_file_path), exist_ok=True)
        with open(config_file_path, 'w') as outfile:
            json.dump(expectation_suite, outfile, indent=2)
        self._compiled = False

    def bind_evaluation_parameters(self, run_id):  # , expectations):
        """Return current evaluation parameters stored for the provided run_id, ready to be bound to parameterized
        expectation values.

        Args:
            run_id: the run_id for which to return evaluation parameters

        Returns:
            evaluation_parameters (dict)
        """
        # TOOO: only return parameters requested by the given expectations
        return self._evaluation_parameter_store.get_run_parameters(run_id)

    def register_validation_results(self, run_id, validation_results, data_asset=None):
        """Process results of a validation run. This method is called by data_asset objects that are connected to
         a DataContext during validation. It performs several actions:
          - store the validation results to a result_store, if one is configured
          - store a snapshot of the data_asset, if so configured and a compatible data_asset is available
          - perform a callback action using the validation results, if one is configured
          - retrieve validation results referenced in other parameterized expectations and store them in the \
            evaluation parameter store.

        Args:
            run_id: the run_id for which to register validation results
            validation_results: the validation results object
            data_asset: the data_asset to snapshot, if snapshot is configured

        Returns:
            validation_results: Validation results object, with updated meta information including references to \
            stored data, if appropriate
        """

        try:
            data_asset_name = validation_results["meta"]["data_asset_name"]
        except KeyError:
            logger.warning("No data_asset_name found in validation results; using '_untitled'")
            data_asset_name = "_untitled"

        try:
            normalized_data_asset_name = self._normalize_data_asset_name(data_asset_name)
        except DataContextError:
            logger.warning(
                "Registering validation results for a data_asset_name that cannot be normalized in this context."
            )

        expectation_suite_name = validation_results["meta"].get("expectation_suite_name", "default")
        if "result_store" in self._project_config:
            result_store = self._project_config["result_store"]
            if isinstance(result_store, dict) and "filesystem" in result_store:
                validation_filepath = self._get_normalized_data_asset_name_filepath(
                    normalized_data_asset_name,
                    expectation_suite_name,
                    base_path=os.path.join(
                        self.root_directory,
                        result_store["filesystem"]["base_directory"],
                        run_id
                    )
                )
                logger.debug("Storing validation result: %s" % validation_filepath)
                safe_mmkdir(os.path.dirname(validation_filepath))
                with open(validation_filepath, "w") as outfile:
                    json.dump(validation_results, outfile, indent=2)
            if isinstance(result_store, dict) and "s3" in result_store:
                bucket = result_store["s3"]["bucket"]
                key_prefix = result_store["s3"]["key_prefix"]
                key = os.path.join(
                    key_prefix,
                    "validations/{run_id}/{data_asset_name}".format(
                        run_id=run_id,
                        data_asset_name=self._get_normalized_data_asset_name_filepath(
                            normalized_data_asset_name,
                            expectation_suite_name,
                            base_path=""
                        )
                    )
                )
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
                    safe_mmkdir(os.path.join(
                        self.root_directory,
                        data_asset_snapshot_store["filesystem"]["base_directory"],
                        run_id)
                    )
                    data_asset.to_csv(
                        self._get_normalized_data_asset_name_filepath(
                            normalized_data_asset_name,
                            expectation_suite_name,
                            base_path=os.path.join(
                                self.root_directory,
                                data_asset_snapshot_store["filesystem"]["base_directory"],
                                run_id
                            ),
                            file_extension=".csv.gz"
                        ),
                        compression="gzip"
                    )

                if isinstance(data_asset_snapshot_store, dict) and "s3" in data_asset_snapshot_store:
                    bucket = data_asset_snapshot_store["s3"]["bucket"]
                    key_prefix = data_asset_snapshot_store["s3"]["key_prefix"]
                    key = os.path.join(
                        key_prefix,
                        "validations/{run_id}/{data_asset_name}.csv.gz".format(
                            run_id=run_id,
                            data_asset_name=self._get_normalized_data_asset_name_filepath(
                                normalized_data_asset_name,
                                expectation_suite_name,
                                base_path="",
                                file_extension=".csv.gz"
                            )
                        )
                    )
                    validation_results["meta"]["data_asset_snapshot"] = "s3://{bucket}/{key}".format(
                        bucket=bucket,
                        key=key)

                    try:
                        import boto3
                        s3 = boto3.resource('s3')
                        result_s3 = s3.Object(bucket, key)
                        result_s3.put(Body=data_asset.to_csv(compression="gzip").encode('utf-8'))
                    except ImportError:
                        logger.error("Error importing boto3 for AWS support. Unable to save to result store.")
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
        """Store a new validation parameter.

        Args:
            run_id: current run_id
            key: parameter key
            value: parameter value

        Returns:
            None
        """
        self._evaluation_parameter_store.set(run_id, key, value)

    def get_validation_param(self, run_id, key):
        """Get a new validation parameter.

        Args:
            run_id: run_id for desired value
            key: parameter key

        Returns:
            value stored in evaluation_parameter_store for the provided run_id and key
        """
        return self._evaluation_parameter_store.get(run_id, key)

    def _compile(self):
        """Compiles all current expectation configurations in this context to be ready for result registration.
        
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

         :: json

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

        known_assets = self.list_expectation_suites()
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

    def get_validation_result(self, data_asset_name, expectation_suite_name="default", run_id=None, failed_only=False):
        """Get validation results from a configured store.

        Args:
            data_asset_name: name of data asset for which to get validation result
            expectation_suite_name: expectation_suite name for which to get validation result (default: "default")
            run_id: run_id for which to get validation result (if None, fetch the latest result by alphanumeric sort)
            failed_only: if True, filter the result to return only failed expectations

        Returns:
            validation_result

        """

        validation_location = self.get_validation_location(data_asset_name, expectation_suite_name, run_id)

        if 'filepath' in validation_location:
            validation_path = validation_location['filepath']
            with open(validation_path, "r") as infile:
                results_dict = json.load(infile)

            if failed_only:
                failed_results_list = [result for result in results_dict["results"] if not result["success"]]
                results_dict["results"] = failed_results_list
                return results_dict
            else:
                return results_dict
    
        elif 'bucket' in validation_location: # s3

            try:
                import boto3
                s3 = boto3.client('s3')
            except ImportError:
                raise ImportError("boto3 is required for retrieving a dataset from s3")
        
            bucket = validation_location["bucket"]
            key = validation_location["key"]
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
            raise DataContextError("Invalid result_store configuration: only 'filesystem' and 's3' are supported.")

    # TODO: refactor this into a snapshot getter based on project_config
    # def get_failed_dataset(self, validation_result, **kwargs):
    #     try:
    #         reference_url = validation_result["meta"]["dataset_reference"]
    #     except KeyError:
    #         raise ValueError("Validation result must have a dataset_reference in the meta object to fetch")
        
    #     if reference_url.startswith("s3://"):
    #         try:
    #             import boto3
    #             s3 = boto3.client('s3')
    #         except ImportError:
    #             raise ImportError("boto3 is required for retrieving a dataset from s3")
        
    #         parsed_url = urlparse(reference_url)
    #         bucket = parsed_url.netloc
    #         key = parsed_url.path[1:]
            
    #         s3_response_object = s3.get_object(Bucket=bucket, Key=key)
    #         if key.endswith(".csv"):
    #             # Materialize as dataset
    #             # TODO: check the associated config for the correct data_asset_type to use
    #             return read_csv(s3_response_object['Body'], **kwargs)
    #         else:
    #             return s3_response_object['Body']

    #     else:
    #         raise ValueError("Only s3 urls are supported.")

    def update_return_obj(self, data_asset, return_obj):
        """Helper called by data_asset.

        Args:
            data_asset: The data_asset whose validation produced the current return object
            return_obj: the return object to update

        Returns:
            return_obj: the return object, potentially changed into a widget by the configured expectation explorer
        """
        if self._expectation_explorer:
            return self._expectation_explorer_manager.create_expectation_widget(data_asset, return_obj)
        else:
            return return_obj

    def render_full_static_site(self):
        """
        Render the static site for the project.

        Returns:
            None
        """

        index_links = []

        validation_filepaths = [y for x in os.walk(self.fixtures_validations_directory) for y in glob(os.path.join(x[0], '*.json'))]
        for validation_filepath in validation_filepaths:
            logger.debug("Loading validation from: %s" % validation_filepath)
            with open(validation_filepath, "r") as infile:
                validation = json.load(infile)

            run_id = validation['meta']['run_id']
            data_asset_name = validation['meta']['data_asset_name']
            expectation_suite_name = validation['meta']['expectation_suite_name']
            model = DescriptivePageRenderer.render(validation)
            out_filepath = self.get_validation_doc_filepath(
                data_asset_name, "{run_id}-{expectation_suite_name}".format(
                    run_id=run_id.replace(':', ''),
                    expectation_suite_name=expectation_suite_name
                )
            )
            safe_mmkdir(os.path.dirname(out_filepath))

            with open(out_filepath, 'w') as writer:
                writer.write(DefaultJinjaPageView.render(model))

            index_links.append({
                "data_asset_name" : data_asset_name,
                "expectation_suite_name": expectation_suite_name,
                "run_id": run_id,
                "filepath": os.path.relpath(out_filepath, self.data_doc_directory)
            })

        expectation_suite_filepaths = [y for x in os.walk(self.expectations_directory) for y in glob(os.path.join(x[0], '*.json'))]
        for expectation_suite_filepath in expectation_suite_filepaths:
            with open(expectation_suite_filepath, "r") as infile:
                expectation_suite = json.load(infile)

            data_asset_name = expectation_suite['data_asset_name']
            expectation_suite_name = expectation_suite['expectation_suite_name']
            model = PrescriptivePageRenderer.render(expectation_suite)
            out_filepath = self.get_validation_doc_filepath(
                data_asset_name,
                expectation_suite_name
            )
            safe_mmkdir(os.path.dirname(out_filepath))
            
            with open(out_filepath, 'w') as writer:
                writer.write(DefaultJinjaPageView.render(model))

            index_links.append({
                "data_asset_name": data_asset_name,
                "expectation_suite_name": expectation_suite_name,
                "filepath": os.path.relpath(out_filepath, self.data_doc_directory)
            })
        
        index_links_dict = OrderedDict()

        for il in index_links:
            source, generator, asset = il["data_asset_name"].split('/')
            if not source in index_links_dict:
                index_links_dict[source] = OrderedDict()
            if not generator in index_links_dict[source]:
                index_links_dict[source][generator] = OrderedDict()
            if not asset in index_links_dict[source][generator]:
                index_links_dict[source][generator][asset] = {
                    'validation_links': [],
                    'expectation_suite_links': []
                }

            if "run_id" in il:
                index_links_dict[source][generator][asset]["validation_links"].append(
                    {
                        "full_data_asset_name": il["data_asset_name"],
                        "run_id": il["run_id"],
                        "expectation_suite_name": il["expectation_suite_name"],
                        "filepath": il["filepath"],
                        "source": source,
                        "generator": generator,
                        "asset": asset
                    }
                )
            else:
                index_links_dict[source][generator][asset]["expectation_suite_links"].append(
                    {
                        "full_data_asset_name": il["data_asset_name"],
                        "expectation_suite_name": il["expectation_suite_name"],
                        "filepath": il["filepath"],
                        "source": source,
                        "generator": generator,
                        "asset": asset
                    }
                )

        sections = []

        for source, generators in index_links_dict.items():
            content_blocks = []

            source_header_block = {
                "content_block_type": "header",
                "header": source,
                "styling": {
                    "classes": ["col-12"],
                    "header": {
                        "classes": ["alert", "alert-secondary"]
                    }
                }
            }
            content_blocks.append(source_header_block)

            for generator, data_assets in generators.items():
                generator_header_block = {
                    "content_block_type": "header",
                    "header": generator,
                    "styling": {
                        "classes": ["col-12", "ml-4"],
                    }
                }
                content_blocks.append(generator_header_block)

                horizontal_rule = {
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "",
                        "params": {},
                        "tag": "hr"
                    },
                    "styling": {
                        "classes": ["col-12"],
                    }
                }
                content_blocks.append(horizontal_rule)

                for data_asset, link_lists in data_assets.items():
                    data_asset_heading = {
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": "$data_asset",
                            "params": {
                                "data_asset": data_asset
                            },
                            "tag": "blockquote",
                            "styling": {
                                "params": {
                                    "data_asset": {
                                        "classes": ["blockquote"],
                                    }
                                }
                            }
                        },
                        "styling": {
                            "classes": ["col-4", "pl-5"],
                            "styles": {
                                "margin-top": "10px",
                                "word-break": "break-all"
                            }
                        }
                    }
                    content_blocks.append(data_asset_heading)

                    expectation_suite_links = link_lists["expectation_suite_links"]
                    expectation_suite_link_table_rows = [
                        [{
                            "template": "$link_text",
                            "params": {
                                "link_text": link_dict["expectation_suite_name"]
                            },
                            "tag": "a",
                            "styling": {
                                "params": {
                                    "link_text": {
                                        "attributes": {
                                            "href": link_dict["filepath"]
                                        }
                                    }
                                }
                            }
                        }] for link_dict in expectation_suite_links
                    ]
                    expectation_suite_link_table = {
                        "content_block_type": "table",
                        "subheader": "Expectation Suites",
                        "table_rows": expectation_suite_link_table_rows,
                        "styling": {
                            "classes": ["col-4"],
                            "styles": {
                                "margin-top": "10px"
                            },
                            "body": {
                                "classes": ["table", "table-sm", ],
                            }
                        },
                    }
                    content_blocks.append(expectation_suite_link_table)

                    validation_links = link_lists["validation_links"]
                    validation_link_table_rows = [
                        [{
                            "template": "$link_text",
                            "params": {
                                "link_text": link_dict["run_id"] + "-" + link_dict["expectation_suite_name"]
                            },
                            "tag": "a",
                            "styling": {
                                "params": {
                                    "link_text": {
                                        "attributes": {
                                            "href": link_dict["filepath"]
                                        }
                                    }
                                }
                            }
                        }] for link_dict in validation_links
                    ]
                    validation_link_table = {
                        "content_block_type": "table",
                        "subheader": "Batch Validations",
                        "table_rows": validation_link_table_rows,
                        "styling": {
                            "classes": ["col-4"],
                            "styles": {
                                "margin-top": "10px"
                            },
                            "body": {
                                "classes": ["table", "table-sm", ],
                            }
                        },
                    }
                    content_blocks.append(validation_link_table)

            section = {
                "section_name": source,
                "content_blocks": content_blocks
            }
            sections.append(section)

        with open(os.path.join(self.data_doc_directory, "index.html"), "w") as writer:
            writer.write(DefaultJinjaIndexPageView.render({
                "sections": sections
            }))

    def profile_datasource(self,
                           datasource_name,
                           generator_name=None,
                           profiler=BasicDatasetProfiler,
                           max_data_assets=10):
        """Profile the named datasource using the named profiler.

        Args:
            datasource_name: the name of the datasource for which to profile data_assets
            generator_name: the name of the generator to use to get batches
            profiler: the profiler to use
            max_data_assets: the maximum number of data_assets from the

        Returns:
            List of (expectation_suite, EVR) tuples for each of the data_assets found in the datasource
        """
        logger.info("Profiling '%s' with '%s'" % (datasource_name, profiler.__name__))
        profiling_results = []
        data_asset_names = self.get_available_data_asset_names(datasource_name)
        if generator_name is None:
            if len(data_asset_names[datasource_name].keys()) == 1:
                generator_name = list(data_asset_names[datasource_name].keys())[0]
        if generator_name not in data_asset_names[datasource_name]:
            raise ProfilerError("Generator %s not found for datasource %s" % (generator_name, datasource_name))

        data_asset_name_list = list(data_asset_names[datasource_name][generator_name])
        total_data_assets = len(data_asset_name_list)
        logger.info("Found %d data assets using generator '%s'" % (total_data_assets, generator_name))
                
        if max_data_assets is None or max_data_assets >= len(data_asset_name_list):
            logger.info("Profiling all %d." % (len(data_asset_name_list)))
        else:
            logger.info("Profiling the first %d, alphabetically." % (max_data_assets))
            data_asset_name_list.sort()
            data_asset_name_list = data_asset_name_list[:max_data_assets]

        total_columns, total_expectations, total_rows, skipped_data_assets = 0, 0, 0, 0
        total_start_time = datetime.datetime.now()
        run_id = total_start_time.isoformat().replace(":", "") + "Z"
        for name in data_asset_name_list:
            logger.info("\tProfiling '%s'..." % name)
            try:
                start_time = datetime.datetime.now()

                # FIXME: There needs to be an affordance here to limit to 100 rows, or downsample, etc.
                batch = self.get_batch(
                    data_asset_name=NormalizedDataAssetName(datasource_name, generator_name, name),
                    expectation_suite_name=profiler.__name__
                )

                if not profiler.validate(batch):
                    raise ProfilerError(
                        "batch '%s' is not a valid batch for the '%s' profiler" % (name, profiler.__name__)
                    )
  
                # Note: This logic is specific to DatasetProfilers, which profile a single batch. Multi-batch profilers
                # will have more to unpack.
                expectation_suite, validation_result = profiler.profile(batch, run_id=run_id)
                profiling_results.append((expectation_suite, validation_result))

                if isinstance(batch, Dataset):
                    # For datasets, we can produce some more detailed statistics
                    row_count = batch.get_row_count()
                    total_rows += row_count
                    new_column_count = len(set([exp["kwargs"]["column"] for exp in expectation_suite["expectations"] if "column" in exp["kwargs"]]))
                    total_columns += new_column_count
                
                new_expectation_count = len(expectation_suite["expectations"])
                total_expectations += new_expectation_count

                self.save_expectation_suite(expectation_suite)
                duration = (datetime.datetime.now() - start_time).total_seconds()
                logger.info("\tProfiled %d columns using %d rows from %s (%.3f sec)" %
                            (new_column_count, row_count, name, duration))

            except ProfilerError as err:
                logger.warning(err.message)
            except IOError as exc:
                logger.warning("IOError while profiling %s. (Perhaps a loading error?) Skipping." % (name))
                logger.debug(str(exc))
                skipped_data_assets += 1

        total_duration = (datetime.datetime.now() - total_start_time).total_seconds()
        logger.info("""
Profiled %d of %d named data assets, with %d total rows and %d columns in %.2f seconds.
Generated, evaluated, and stored %d candidate Expectations.
Note: You will need to review and revise Expectations before using them in production.""" % (
            len(data_asset_name_list),
            total_data_assets,
            total_rows,
            total_columns,
            total_duration,
            total_expectations,
        ))
        if skipped_data_assets > 0:
            logger.warning("Skipped %d data assets due to errors." % skipped_data_assets)

        return profiling_results


PROJECT_HELP_COMMENT = """# Welcome to great expectations. 
# This project configuration file allows you to define datasources, 
# generators, integrations, and other configuration artifacts that
# make it easier to use Great Expectations.

# For more help configuring great expectations, 
# see the documentation at: https://greatexpectations.io/config_file.html

# NOTE: GE uses the names of configured datasources and generators to manage
# how expectations and other configuration artifacts are stored in the 
# expectations/ and datasources/ folders. If you need to rename an existing
# datasource or generator, be sure to also update the paths for related artifacts.

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
