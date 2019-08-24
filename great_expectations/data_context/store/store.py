from ..types import (
    # NameSpaceDotDict,
    # NormalizedDataAssetName,
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

from ..util import (
    parse_string_to_data_context_resource_identifier
)

class Store(object):
    def __init__(
        self,
        config,
        root_directory=None,
    ):
        """

        TODO : Describe the rationale for keeping root_directory out of config:
            Because it's passed in separately. If we want the config to be serializable to yyml, we can't add extra arguments at runtime.
        """

        if root_directory is not None and not os.path.isabs(root_directory):
            raise ValueError("root_directory must be an absolute path. Got {0} instead.".format(root_directory))

        if not isinstance(config, self.get_config_class()):
            # Attempt to coerce config to a typed config
            config = self.get_config_class()(
                coerce_types=True,
                **config
            )

        self.config = config
        self.root_directory = root_directory

        self._setup()


    def get(self, key, serialization_type=None):
        value=self._get(key)

        if serialization_type:
            deserialization_method = self._get_deserialization_method(
                serialization_type)
        else:
            deserialization_method = self._get_deserialization_method(
                self.config.serialization_type)
        deserialized_value = deserialization_method(value)
        return deserialized_value

    def set(self, key, value, serialization_type=None):
        if serialization_type:
            serialization_method = self._get_serialization_method(
                serialization_type)
        else:
            serialization_method = self._get_serialization_method(
                self.config.serialization_type)

        serialized_value = serialization_method(value)
        self._set(key, serialized_value)


    @classmethod
    def get_config_class(cls):
        return cls.config_class

    def _get_serialization_method(self, serialization_type):
        if serialization_type == None:
            return lambda x: x

        elif serialization_type == "json":
            return json.dumps

        elif serialization_type == "pandas_csv":

            def convert_to_csv(df):
                logger.debug("Starting convert_to_csv")

                assert isinstance(df, pd.DataFrame)

                return df.to_csv(index=None)

            return convert_to_csv

        # TODO: Add more serialization methods as needed

    def _get_deserialization_method(self, serialization_type):
        if serialization_type == None:
            return lambda x: x

        elif serialization_type == "json":
            return json.loads

        elif serialization_type == "pandas_csv":
            # TODO:
            raise NotImplementedError

        # TODO: Add more serialization methods as needed

    def _get(self, key):
        raise NotImplementedError

    def _set(self, key, value):
        raise NotImplementedError

    def list_keys(self):
        raise NotImplementedError

    def has_key(self, key):
        raise NotImplementedError


class NamespaceAwareStore(Store):

    def get(self, key, serialization_type=None):

        if not isinstance(key, DataContextResourceIdentifier):
            raise TypeError("key: {!r} must be a DataContextResourceIdentifier, not {!r}".format(
                key,
                type(key),
            ))

        return super(NamespaceAwareStore, self).get(key, serialization_type)

    def set(self, key, value, serialization_type=None):

        if not isinstance(key, DataContextResourceIdentifier):
            raise TypeError("key: {!r} must be a DataContextResourceIdentifier, not {!r}".format(
                key,
                type(key),
            ))

        super(NamespaceAwareStore, self).set(key, value, serialization_type)


class InMemoryStore(Store):
    """Uses an in-memory dictionary as a store.
    """

    config_class = InMemoryStoreConfig

    def _setup(self):
        self.store = {}

    def _get(self, key):
        return self.store[key]

    def _set(self, key, value):
        self.store[key] = value

    def list_keys(self):
        return self.store.keys()

    def has_key(self, key):
        return key in self.store

class NamespacedInMemoryStore(NamespaceAwareStore, Store):
    """
    """

    # TODO : This probably needs to change
    config_class = InMemoryStoreConfig

    def _get(self, key):
        return self.store[key.to_string()]

    def _set(self, key, value):
        self.store[key.to_string()] = value

    def list_keys(self):
        # FIXME : Convert strings back into the appropriate type of DataContextResourceIdentifier
        # ??? Can strings be unambiguously converted into ResourceIdentifiers?
        # -> The DataContext can probably do this. It's a species of magic autocomplete. Or we can enforce it through stringifying conventions. I think conventions win.
        # ??? How does a Store know what its "appropriate type" is? -> Subclassing could work, but then we end up in a matrix world
        # -> Add a "resource_identifier_class_name". This may be replaced by 
        # ??? Can Stores mix types of ResourceIdentifiers? -> Again, easy to solve with subclassing, but will require lots of classes
        # -> Currently, no.
        # -> This is going to make fixtures/ tricky.
        return [parse_string_to_data_context_resource_identifier(key_string) for key_string in self.store.keys()]

    def has_key(self, key):
        return key.to_string() in self.store


class FilesystemStore(Store):
    """Uses a local filepath as a store.

    # FIXME : It's currently possible to break this Store by passing it a ResourceIdentifier that contains '/'.
    """

    config_class = FilesystemStoreConfig

    def __init__(
        self,
        config,
        root_directory, #This argument is REQUIRED for this class
    ):
        super(FilesystemStore, self).__init__(config, root_directory)

    def _setup(self):
        self.full_base_directory = os.path.join(
            self.root_directory,
            self.config.base_directory,
        )

        safe_mmkdir(str(os.path.dirname(self.full_base_directory)))

    def _get(self, key):
        filepath = self._get_filepath_from_key(key)
        with open(filepath) as infile:
            return infile.read()

    def _set(self, key, value):
        filepath = self._get_filepath_from_key(key)
        path, filename = os.path.split(filepath)

        safe_mmkdir(str(path))
        with open(filepath, "w") as outfile:
            outfile.write(value)

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
                    self._get_key_from_filepath(key)
                )

        return key_list

    def _get_filepath_from_key(self, key):
        # NOTE : This method is trivial in this class, but child classes can get pretty complex
        return os.path.join(self.full_base_directory, key)
    
    def _get_key_from_filepath(self, filepath):
        # NOTE : This method is trivial in this class, but child classes can get pretty complex
        return filepath

class NameSpacedFilesystemStore(FilesystemStore, NamespaceAwareStore):

    config_class = NamespacedFilesystemStoreConfig

    def _get_filepath_from_key(self, key):
        if isinstance(key, ValidationResultIdentifier):
            # NOTE : This might be easier to parse as a Jinja template
            middle_path = key.to_string(separator="/")

            filename_core = "-".join([
                key.expectation_suite_identifier.suite_name,
                key.expectation_suite_identifier.purpose,
            ])
            file_prefix = self.config.get("file_prefix", "")
            file_extension = self.config.get("file_extension", "")
            filename = file_prefix + filename_core + file_extension

            filepath = os.path.join(
                self.full_base_directory,
                key.run_id.to_string(include_class_prefix=False, separator="-"),
                key.expectation_suite_identifier.data_asset_identifier.to_string(include_class_prefix=False, separator="/"),
                filename
            )
            return filepath

        else:
            return os.path.join(
                self.full_base_directory,
                key.to_string(separator="/"),
            ) + self.config.file_extension

    def _get_key_from_filepath(self, filepath):
        if self.config.resource_identifier_class_name == "ValidationResultIdentifier":
            module = importlib.import_module("great_expectations.data_context.types.resource_identifiers")
            class_ = getattr(module, self.config.resource_identifier_class_name)

            file_prefix = self.config.get("file_prefix", "")
            file_extension = self.config.get("file_extension", "")
            matches = re.compile("(.*)-(.*)/(.*)/(.*)/(.*)/"+file_prefix+"(.*)-(.*)"+file_extension).match(filepath)

            args = (
                matches.groups()[2],
                matches.groups()[3],
                matches.groups()[4],

                matches.groups()[5],
                matches.groups()[6],

                matches.groups()[0],
                matches.groups()[1],
            )
            return class_(*args)

        else:
            file_extension_length = len(self.config.file_extension)
            filename_without_extension = filename[:-1*file_extension_length]

            key = parse_string_to_data_context_resource_identifier(filename_without_extension, separator="/")
            return key


    # TODO : This method is OBE. Remove entirely
    # TODO: This method should probably live in ContextAwareStore
    # For the moment, I'm leaving it here, because:
    #   1. This method and NameSpaceDotDict isn't yet general enough to handle all permutations of namespace objects
    #   2. Rewriting all the tests in test_store is more work than I can take on right now.
    #   3. Probably the best thing to do is to test BOTH classes that take simple strings as keys, and classes that take full NameSpaceDotDicts. But that relies on (1).
    #
    # DataContext.write_resource has some good inspiration for this...
    # Or we might conceivably bring over the full logic from _get_normalized_data_asset_name_filepath.

    def _get_namespaced_key(self, key):
        if not isinstance(key, ValidationResultIdentifier):
            raise TypeError(
                "key must be an instance of type ValidationResultIdentifier, not {0}".format(type(key)))

        # filepath = "foo/bar/not_a_real_filepath"
        filepath = self._get_normalized_data_asset_name_filepath(
            key.normalized_data_asset_name,
            key.expectation_suite_name,
            base_path=os.path.join(
                self.full_base_directory,
                key.run_id
            ),
            file_extension=self.config.file_extension
        )
        return filepath

    # TODO : This method is OBE. Remove entirely

    # FIXME : This method is duplicated in DataContext. That method should be deprecated soon, but that will require a larger refactor.
    # Specifically, get_, save_, and list_expectation_suite will need to be refactored into a store so that they don't rely on the method.
    # The same goes for write_resource.
    def _get_normalized_data_asset_name_filepath(self,
        data_asset_name,
        expectation_suite_name,
        base_path=None,
        file_extension=".json"
    ):
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
        if isinstance(data_asset_name, DataAssetIdentifier):
            name_parts = [name_part.replace("/", "__") for name_part in data_asset_name]
            relative_path = "/".join(name_parts)

        # elif isinstance(data_asset_name, string_types):
        #     # if our delimiter is not '/', we need to first replace any slashes that exist in the name
        #     # to avoid extra layers of nesting (e.g. for dbt models)
        #     relative_path = data_asset_name
        #     if self.data_asset_name_delimiter != "/":
        #         relative_path.replace("/", "__")
        #         relative_path = relative_path.replace(self.data_asset_name_delimiter, "/")
        else:
            raise DataContextError("data_assset_name must be a DataAssetIdentifier")

        expectation_suite_name += file_extension

        return os.path.join(
            base_path,
            relative_path,
            expectation_suite_name
        )

    # TODO: Not yet sure where this method should live
    def get_most_recent_run_id(self):
        run_id_list = os.listdir(self.full_base_directory)

        run_ids = [
            name for name in run_id_list if
            os.path.isdir(os.path.join(self.full_base_directory, name))
        ]
        most_recent_run_id = sorted(run_ids)[-1]

        return most_recent_run_id


# class S3Store(ContextAwareStore):
#     """Uses an S3 bucket+prefix as a store
#     """

#     def _get(self, key):
#         raise NotImplementedError

#     def _set(self, key, value):
#         raise NotImplementedError

# This code is from an earlier (untested) implementation of DataContext.register_validation_results
# Storing it here in case it can be salvaged
# if isinstance(data_asset_snapshot_store, dict) and "s3" in data_asset_snapshot_store:
#     bucket = data_asset_snapshot_store["s3"]["bucket"]
#     key_prefix = data_asset_snapshot_store["s3"]["key_prefix"]
#     key = os.path.join(
#         key_prefix,
#         "validations/{run_id}/{data_asset_name}.csv.gz".format(
#             run_id=run_id,
#             data_asset_name=self._get_normalized_data_asset_name_filepath(
#                 normalized_data_asset_name,
#                 expectation_suite_name,
#                 base_path="",
#                 file_extension=".csv.gz"
#             )
#         )
#     )
#     validation_results["meta"]["data_asset_snapshot"] = "s3://{bucket}/{key}".format(
#         bucket=bucket,
#         key=key)
#
#     try:
#         import boto3
#         s3 = boto3.resource('s3')
#         result_s3 = s3.Object(bucket, key)
#         result_s3.put(Body=data_asset.to_csv(compression="gzip").encode('utf-8'))
#     except ImportError:
#         logger.error("Error importing boto3 for AWS support. Unable to save to result store.")
#     except Exception:
#         raise
