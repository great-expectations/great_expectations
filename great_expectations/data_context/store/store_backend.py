import random

from ..types import (
    DataAssetIdentifier,
    ValidationResultIdentifier,
)
from ..types.resource_identifiers import (
    DataContextResourceIdentifier,
)
from .types import (
    InMemoryStoreConfig,
    FilesystemStoreConfig,
    NamespacedFilesystemStoreConfig,
)
from ..util import safe_mmkdir
import pandas as pd
import six
import io
import os
import json
import logging
logger = logging.getLogger(__name__)
import importlib
import re
from six import string_types

from ..util import (
    parse_string_to_data_context_resource_identifier
)
from .store import (
    Store
)
from ...types import (
    ListOf,
    AllowedKeysDotDict,
)

class StoreBackendConfig(AllowedKeysDotDict):
    _allowed_keys = set()


class StoreBackend(object):
    """a key-value store, to abstract away reading and writing to a persistence layer
    """

    config_class = StoreBackendConfig

    def __init__(
        self,
        config,
    ):
        # NOTE: Implmentation of configs overall is pretty soft.
        # Suggested TODO: Implement a base Configurable class that knows how to
        #   * Ensure that an associated config class exists (for each subclass)
        #   * Validate configs
        #   * Initialize itself from a config
        # TONS of things could inherit from this: Stores, StoreBackends, Actions, DataSources, Generators, the DataContext itself, ...
        if not isinstance(config, self.get_config_class()):
            # Attempt to coerce config to a typed config
            config = self.get_config_class()(
                coerce_types=True,
                **config
            )

        self.config = config

        self._setup()


    def get(self, key):
        self._validate_key(key)
        value = self._get(key)
        return value

    def set(self, key, value):
        self._validate_key(key)
        self._validate_value(value)
        self._set(key, value)

    def has_key(self, key):
        self._validate_key(key)
        return self._has_key(key)


    @classmethod
    def get_config_class(cls):
        return cls.config_class

    def _validate_key(self, key):
        if not isinstance(key, tuple):
            raise TypeError("Keys in {0} must be instances of {1}, not {2}".format(
                self.__class__.__name__,
                tuple,
                type(key),
            ))
        
        for key_element in key:
            if not isinstance(key_element, string_types):
                raise TypeError("Elements within tuples passed as keys to {0} must be instances of {1}, not {2}".format(
                    self.__class__.__name__,
                    string_types,
                    type(key_element),
                ))

    def _validate_value(self, value):
        # TODO: Allow bytes as well.

        if not isinstance(value, string_types):
            raise TypeError("Values in {0} must be instances of {1}, not {2}".format(
                self.__class__.__name__,
                string_types,
                type(value),
            ))

    def _setup(self):
        pass

    def _get(self, key):
        raise NotImplementedError

    def _set(self, key, value):
        raise NotImplementedError

    def list_keys(self):
        raise NotImplementedError

    def _has_key(self, key):
        raise NotImplementedError


class InMemoryStoreBackendConfig(AllowedKeysDotDict):
    _allowed_keys = set([
        "separator",
    ])

    # NOTE: Abe 2019/08/29: For now, I'm defaulting to rigid typing. We can always back off later.
    _required_keys = set([
        "separator",
    ])


class InMemoryStoreBackend(StoreBackend):
    """Uses an in-memory dictionary as a store backend.

    Note: currently, this class turns the whole key into a single key_string.
    This works, but it's blunt.
    """

    config_class = InMemoryStoreBackendConfig

    # FIXME: separator should be pased in as part of the config, not an arg.
    # Or maybe __init__ should receive it as a config and propagate it here as an arg...
    def _setup(self, separator="."):
        self.store = {}
        self.separator = separator

    def _get(self, key):
        return self.store[self._convert_tuple_to_string(key)]

    def _set(self, key, value):
        self.store[self._convert_tuple_to_string(key)] = value

    def _validate_key(self, key):
        super(InMemoryStoreBackend, self)._validate_key(key)
        
        if self.separator in key:
            raise ValueError("Keys in {0} must not contain the separator character {1} : {2}".format(
                self.__class__.__name__,
                self.separator,
                key,
            ))
    
    def _convert_tuple_to_string(self, tuple_):
        return self.separator.join(tuple_)
    
    def _convert_string_to_tuple(self, string):
        return tuple(string.split(self.separator))

    def list_keys(self):
        return [self._convert_string_to_tuple(key_str) for key_str in list(self.store.keys())]

    def _has_key(self, key):
        return self._convert_tuple_to_string(key) in self.store


class FilesystemStoreBackendConfig(AllowedKeysDotDict):
    # TODO: Some of these should be in _required_keys
    # NOTE: What would be REALLY useful is to make them required with defaults. ex: separator : "."
    _allowed_keys = set([
        "base_directory",
        "file_extension",
        "key_length",
        "replaced_substring",
        "replacement_string",
        "filepath_template",
    ])

    # NOTE: Abe 2019/08/29: For now, I'm defaulting to rigid typing. We can always back off later.
    _required_keys = set([
        "base_directory",
        "key_length",
        "replaced_substring",
        "replacement_string",
        "filepath_template",
    ])

    _key_types = {
        "base_directory" : string_types,
        "file_extension" : string_types,
        "key_length" : int,
        "replaced_substring" : string_types, #NOTE: It would be nice to make this a ListOf(string_types), but if we did that, the string replacement wouldn't be reversible.
        "replacement_string" : string_types,
        "filepath_template" : string_types,
    }


class FilesystemStoreBackend(StoreBackend):
    """Uses a local filepath as a store.

    # FIXME : It's currently possible to break this Store by passing it a ResourceIdentifier that contains '/'.
    """

    config_class = FilesystemStoreBackendConfig

    def __init__(
        self,
        config,
        root_directory, #This argument is REQUIRED for this class
    ):
        """
        Q: What was the original rationale for keeping root_directory out of config?
        A: Because it's passed in separately to the DataContext. If we want the config to be serializable to yaml, we can't add extra arguments at runtime.

        HOWEVER, passing in root_directory as a separate parameter breaks the normal pattern we've been using for configurability.

        TODO: Figure this out. It might require adding root_directory to the data_context config...?
        """

        if not os.path.isabs(root_directory):
            raise ValueError("root_directory must be an absolute path. Got {0} instead.".format(root_directory))
            
        self.root_directory = root_directory
        
        super(FilesystemStoreBackend, self).__init__(config)


    def _setup(self):
        self.full_base_directory = os.path.join(
            self.root_directory,
            self.config.base_directory,
        )

        print(self.full_base_directory)
        # safe_mmkdir(str(os.path.dirname(self.full_base_directory)))

    def _get(self, key):
        filepath = os.path.join(
            self.full_base_directory,
            self._convert_key_to_filepath(key)
        )
        with open(filepath) as infile:
            return infile.read()

    def _set(self, key, value):
        filepath = os.path.join(
            self.full_base_directory,
            self._convert_key_to_filepath(key)
        )
        path, filename = os.path.split(filepath)

        safe_mmkdir(str(path))
        with open(filepath, "w") as outfile:
            outfile.write(value)

    def _validate_key(self, key):
        super(FilesystemStoreBackend, self)._validate_key(key)
        
        for key_element in key:
            if self.config.replacement_string in key_element:
                raise ValueError("Keys in {0} must not contain the replacement_string {1} : {2}".format(
                    self.__class__.__name__,
                    self.config.replacement_string,
                    key,
                ))

    def list_keys(self):
        # TODO : Rename "keys" in this method to filepaths, for clarity
        key_list = []
        for root, dirs, files in os.walk(self.full_base_directory):
            for file_ in files:
                full_path, file_name = os.path.split(os.path.join(root, file_))
                relative_path = os.path.relpath(
                    full_path,
                    self.full_base_directory,
                )
                if relative_path == ".":
                    key = file_name
                else:
                    key = os.path.join(
                        relative_path,
                        file_name
                    )

                key_list.append(
                    self._convert_filepath_to_key(key)
                )

        return key_list

    # TODO : Write tests for this method
    def has_key(self, key):
        assert isinstance(key, string_types)

        all_keys = self.list_keys()
        return key in all_keys

    def _convert_key_to_filepath(self, key):
        # NOTE: At some point in the future, it might be better to replace this logic with os.path.join.
        # That seems more correct, but the configs will be a lot less intuitive.
        # In the meantime, there is some chance that configs will not be cross-OS compatible.

        # NOTE : These methods support fixed-length keys, but not variable.
        # TODO : Figure out how to handle variable-length keys
        self._validate_key(key)

        replaced_key = [
            re.sub(
                self.config["replaced_substring"],
                self.config["replacement_string"],
                key_element
            ) for key_element in key
        ]

        converted_string = self.config["filepath_template"].format(*replaced_key, **self.config)

        return converted_string


    def _convert_filepath_to_key(self, filepath):

        # Convert the template to a regex
        indexed_string_substitutions = re.findall("\{\d+\}", self.config["filepath_template"])
        tuple_index_list = ["(?P<tuple_index_{0}>.*)".format(i,) for i in range(len(indexed_string_substitutions))]
        intermediate_filepath_regex = re.sub(
            "\{\d+\}",
            lambda m, r=iter(tuple_index_list): next(r),
            self.config["filepath_template"]
        )
        filepath_regex = intermediate_filepath_regex.format(*tuple_index_list, **self.config)

        #Apply the regex to the filepath
        matches = re.compile(filepath_regex).match(filepath)

        #Map key elements into the apprpriate parts of the tuple
        new_key = list([None for element in range(self.config.key_length)])
        for i in range(len(tuple_index_list)):
            tuple_index = int(re.search('\d+', indexed_string_substitutions[i]).group(0))
            key_element = matches.group('tuple_index_'+str(i))
            replaced_key_element = re.sub(self.config["replacement_string"], self.config["replaced_substring"], key_element)

            new_key[tuple_index] = replaced_key_element

        new_key = tuple(new_key)
        return new_key


    def verify_that_key_to_filepath_operation_is_reversible(self):
        # NOTE: There's actually a fairly complicated problem here, similar to magic autocomplete for dataAssetNames.
        # "Under what conditions does an incomplete key tuple fully specify an object within the GE namespace?"
        # This doesn't just depend on the structure of keys.
        # It also depends on uniqueness of combinations of named elements within the namespace tree.
        # For now, I do the blunt thing and force filepaths to fully specify keys.

        def get_random_hex(len=4):
            return "".join([random.choice(list("ABCDEF0123456789")) for i in range(len)])

        key = tuple([get_random_hex() for j in range(self.config.key_length)])
        filepath = self._convert_key_to_filepath(key)
        new_key = self._convert_filepath_to_key(filepath)
        print(key, new_key)
        if key != new_key:
            raise AssertionError("Cannot reverse key conversion in {}\nThis is most likely a problem with your filepath_template:\n\t{}".format(
                self.__class__.__name__,
                self.config.filepath_template
            ))

    # TODO: This is definitely not the right long-term home for this method.
    # Leaving it here temporarily, because factoring it out will require switching
    # DataContext to use NamespacedFilesystemStore for local_validation_result_store
    # and possibly others.
    def get_most_recent_run_id(self):
        run_id_list = os.listdir(self.full_base_directory)

        run_ids = [
            name for name in run_id_list if
            os.path.isdir(os.path.join(self.full_base_directory, name))
        ]
        most_recent_run_id = sorted(run_ids)[-1]

        return most_recent_run_id

# # class S3Store(ContextAwareStore):
# #     """Uses an S3 bucket+prefix as a store
# #     """

# #     def _get(self, key):
# #         raise NotImplementedError

# #     def _set(self, key, value):
# #         raise NotImplementedError

# # This code is from an earlier (untested) implementation of DataContext.register_validation_results
# # Storing it here in case it can be salvaged
# # if isinstance(data_asset_snapshot_store, dict) and "s3" in data_asset_snapshot_store:
# #     bucket = data_asset_snapshot_store["s3"]["bucket"]
# #     key_prefix = data_asset_snapshot_store["s3"]["key_prefix"]
# #     key = os.path.join(
# #         key_prefix,
# #         "validations/{run_id}/{data_asset_name}.csv.gz".format(
# #             run_id=run_id,
# #             data_asset_name=self._get_normalized_data_asset_name_filepath(
# #                 normalized_data_asset_name,
# #                 expectation_suite_name,
# #                 base_path="",
# #                 file_extension=".csv.gz"
# #             )
# #         )
# #     )
# #     validation_results["meta"]["data_asset_snapshot"] = "s3://{bucket}/{key}".format(
# #         bucket=bucket,
# #         key=key)
# #
# #     try:
# #         import boto3
# #         s3 = boto3.resource('s3')
# #         result_s3 = s3.Object(bucket, key)
# #         result_s3.put(Body=data_asset.to_csv(compression="gzip").encode('utf-8'))
# #     except ImportError:
# #         logger.error("Error importing boto3 for AWS support. Unable to save to result store.")
# #     except Exception:
# #         raise
