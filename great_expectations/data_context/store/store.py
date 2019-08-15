import json
import os
import io
from ..util import safe_mmkdir

# from ..data_context import (
#     DataContext
# )
from .types import (
    InMemoryStoreConfig,
    FilesystemStoreConfig,
)
from ..types import (
    NameSpaceDotDict,
)

class ContextAwareStore(object):
    def __init__(
        self,
        data_context,
        config,
    ):
        #FIXME: Eek. This causes circular imports. What to do?        
        # if not isinstance(data_context, DataContext):
        #     raise TypeError("data_context must be an instance of type DataContext")

        self.data_context = data_context

        if not isinstance(config, self.get_config_class()):
            #Attempt to coerce config to a typed config
            config = self.get_config_class()(
                coerce_types=True,
                **config
            )

        self.config = config

        self._setup()

    def get(self, key, serialization_type=None):
        namespaced_key = self._get_namespaced_key(key)
        value = self._get(namespaced_key)

        if serialization_type:
            deserialization_method = self._get_deserialization_method(serialization_type)
        else:
            deserialization_method = self._get_deserialization_method(self.config.serialization_type)
        deserialized_value = deserialization_method(value)
        return deserialized_value

    def set(self, key, value, serialization_type=None):
        namespaced_key = self._get_namespaced_key(key)

        if serialization_type:
            serialization_method = self._get_serialization_method(serialization_type)
        else:
            serialization_method = self._get_serialization_method(self.config.serialization_type)
        
        serialized_value = serialization_method(value)
        self._set(namespaced_key, serialized_value)

    @classmethod
    def get_config_class(cls):
        return cls.config_class
    
    def _get_namespaced_key(self, key):
        #TODO: This method is a placeholder until we bring in _get_namespaced_key from NameSpacedFilesystemStore
        return key

    def _get_serialization_method(self, serialization_type):
        if serialization_type == None:
            return lambda x: x

        elif serialization_type == "json":
            return json.dumps

        elif serialization_type == "pandas_csv":
            #!!! This is a fast, janky, untested implementation
            def convert_to_csv(df):
                s_buf = io.StringIO()
                df.to_csv(s_buf)
                return s_buf.read()

            return convert_to_csv

        #TODO: Add more serialization methods as needed

    def _get_deserialization_method(self, serialization_type):
        if serialization_type == None:
            return lambda x: x

        elif serialization_type == "json":
            return json.loads

        elif serialization_type == "pandas_csv":
            #TODO:
            raise NotImplementedError

        #TODO: Add more serialization methods as needed

    def _get(self, key):
        raise NotImplementedError

    def _set(self, key, value):
        raise NotImplementedError
    
    def list_keys(self):
        raise NotImplementedError


class InMemoryStore(ContextAwareStore):
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



class FilesystemStore(ContextAwareStore):
    """Uses a local filepath as a store.
    """

    config_class = FilesystemStoreConfig

    def _setup(self):
        self.full_base_directory = self.config.base_directory
        safe_mmkdir(str(os.path.dirname(self.full_base_directory)))

    def _get(self, key):
        with open(os.path.join(self.full_base_directory, key)) as infile:
            return infile.read()

    def _set(self, key, value):
        filename = os.path.join(self.full_base_directory, key)
        safe_mmkdir(str(os.path.split(filename)[0]))
        with open(filename, "w") as outfile:
            outfile.write(value)

    def list_keys(self):
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
                key_list.append(key)

        return key_list


class NameSpacedFilesystemStore(FilesystemStore):

    def _setup(self):
        self.full_base_directory = os.path.join(
            self.data_context.root_directory,
            self.config.base_directory,
        )

        safe_mmkdir(str(os.path.dirname(self.full_base_directory)))

    #TODO: This method should probably live in ContextAwareStore
    #For the moment, I'm leaving it here, because:
    #   1. This method and NameSpaceDotDict isn't yet general enough to handle all permutations of namespace objects
    #   2. Rewriting all the tests in test_store is more work than I can take on right now.
    #   3. Probably the best thing to do is to test BOTH classes that take simple strings as keys, and classes that take full NameSpaceDotDicts. But that relies on (1).
    #
    # DataContext.write_resource has some good inspiration for this...
    # Or we might conceivably bring over the full logic from _get_normalized_data_asset_name_filepath.

    def _get_namespaced_key(self, key):
        if not isinstance(key, NameSpaceDotDict):
            raise TypeError("key must be an instance of type NameSpaceDotDict, not {0}".format(type(key)))

        filepath = self.data_context._get_normalized_data_asset_name_filepath(
            key.normalized_data_asset_name,
            key.expectation_suite_name,
            base_path=os.path.join(
                self.full_base_directory,
                key.run_id
            ),
            file_extension=self.config.file_extension
        )
        return filepath
    
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

#This code is from an earlier (untested) implementation of DataContext.register_validation_results
#Storing it here in case it can be salvaged
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