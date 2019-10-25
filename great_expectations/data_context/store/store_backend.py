import random

from ..types import (
    DataAssetIdentifier,
    ValidationResultIdentifier,
)
from ..types.base_resource_identifiers import (
    DataContextKey,
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
from ...types import (
    ListOf,
    AllowedKeysDotDict,
)

# TODO : Add docstrings to these classes.
# TODO : Implement S3StoreBackend with mocks and tests

# NOTE : Abe 2019/08/30 : Currently, these classes behave as key-value stores.
# We almost certainly want to extend that functionality to allow other operations

class StoreBackend(object):
    """a key-value store, to abstract away reading and writing to a persistence layer
    """

    def __init__(
        self,
        root_directory=None, # NOTE: Eugene: 2019-09-06: I think this should be moved into filesystem-specific children classes
    ):
        self.root_directory = root_directory

    def get(self, key):
        self._validate_key(key)
        value = self._get(key)
        return value

    def set(self, key, value, **kwargs):
        self._validate_key(key)
        self._validate_value(value)
        # Allow the implementing setter to return something (e.g. a path used for its key)
        return self._set(key, value, **kwargs)

    def has_key(self, key):
        self._validate_key(key)
        return self._has_key(key)

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
        pass

    def _get(self, key):
        raise NotImplementedError

    def _set(self, key, value, **kwargs):
        raise NotImplementedError

    def list_keys(self):
        raise NotImplementedError

    def _has_key(self, key):
        raise NotImplementedError


class InMemoryStoreBackend(StoreBackend):
    """Uses an in-memory dictionary as a store backend.

    Note: currently, this class turns the whole key into a single key_string.
    This works, but it's blunt.
    """

    def __init__(
        self,
        separator=".",
        root_directory=None
    ):
        self.store = {}
        self.separator = separator

    def _get(self, key):
        return self.store[self._convert_tuple_to_string(key)]

    def _set(self, key, value, **kwargs):
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


class FixedLengthTupleStoreBackend(StoreBackend):
    """

    The key to this StoreBackend abstract class must be a tuple with fixed length equal to key_length.
    The filepath_template is a string template used to convert the key to a filepath.
    There's a bit of regex magic in _convert_filepath_to_key that reverses this process,
    so that we can write AND read using filenames as keys.

    Another class should get this logic through multiple inheritance.
    """

    def __init__(
        self,
        # base_directory,
        filepath_template,
        key_length,
        root_directory,
        forbidden_substrings=["/", "\\"],
    ):
        assert isinstance(key_length, int)
        self.key_length = key_length
        self.forbidden_substrings = forbidden_substrings

        self.filepath_template = filepath_template
        self.verify_that_key_to_filepath_operation_is_reversible()

    def _validate_key(self, key):
        super(FixedLengthTupleStoreBackend, self)._validate_key(key)

        for key_element in key:
            for substring in self.forbidden_substrings:
                if substring in key_element:
                    raise ValueError("Keys in {0} must not contain substrings in {1} : {2}".format(
                        self.__class__.__name__,
                        self.forbidden_substrings,
                        key,
                    ))

    def _validate_value(self, value):
        # NOTE: We may want to allow bytes here as well.

        if not isinstance(value, string_types):
            raise TypeError("Values in {0} must be instances of {1}, not {2}".format(
                self.__class__.__name__,
                string_types,
                type(value),
            ))


    def _convert_key_to_filepath(self, key):
        # NOTE: At some point in the future, it might be better to replace this logic with os.path.join.
        # That seems more correct, but the configs will be a lot less intuitive.
        # In the meantime, there is some chance that configs will not be cross-OS compatible.

        # NOTE : These methods support fixed-length keys, but not variable.
        self._validate_key(key)

        converted_string = self.filepath_template.format(*list(key))

        return converted_string

    def _convert_filepath_to_key(self, filepath):

        # Convert the template to a regex
        indexed_string_substitutions = re.findall("\{\d+\}", self.filepath_template)
        tuple_index_list = ["(?P<tuple_index_{0}>.*)".format(i, ) for i in range(len(indexed_string_substitutions))]
        intermediate_filepath_regex = re.sub(
            "\{\d+\}",
            lambda m, r=iter(tuple_index_list): next(r),
            self.filepath_template
        )
        filepath_regex = intermediate_filepath_regex.format(*tuple_index_list)

        # Apply the regex to the filepath
        matches = re.compile(filepath_regex).match(filepath)
        if matches is None:
            return None

        #Map key elements into the appropriate parts of the tuple
        # TODO: A common configuration error is for the key length to not match the number of elements in the filepath_template. We should trap this error and add a more informative message.
        new_key = list([None for element in range(self.key_length)])
        for i in range(len(tuple_index_list)):
            tuple_index = int(re.search('\d+', indexed_string_substitutions[i]).group(0))
            key_element = matches.group('tuple_index_' + str(i))
            new_key[tuple_index] = key_element

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

        key = tuple([get_random_hex() for j in range(self.key_length)])
        filepath = self._convert_key_to_filepath(key)
        new_key = self._convert_filepath_to_key(filepath)
        if key != new_key:
            raise ValueError(
                "filepath template {0} for class {1} is not reversible for a tuple of length {2}. Have you included all elements in the key tuple?".format(
                    self.filepath_template,
                    self.__class__.__name__,
                    self.key_length,
                ))
            # raise AssertionError("Cannot reverse key conversion in {}\nThis is most likely a problem with your filepath_template:\n\t{}".format(
            #     self.__class__.__name__,
            #     self.filepath_template
            # ))


class FixedLengthTupleFilesystemStoreBackend(FixedLengthTupleStoreBackend):
    """Uses a local filepath as a store.

    The key to this StoreBackend must be a tuple with fixed length equal to key_length.
    The filepath_template is a string template used to convert the key to a filepath.
    There's a bit of regex magic in _convert_filepath_to_key that reverses this process,
    so that we can write AND read using filenames as keys.
    """

    def __init__(
        self,
        base_directory,
        filepath_template,
        key_length,
        root_directory,
        forbidden_substrings=["/", "\\"],
    ):
        super(FixedLengthTupleFilesystemStoreBackend, self).__init__(
            root_directory=root_directory,
            filepath_template=filepath_template,
            key_length=key_length,
            forbidden_substrings=forbidden_substrings,
        )

        self.base_directory = base_directory

        if not os.path.isabs(root_directory):
            raise ValueError("root_directory must be an absolute path. Got {0} instead.".format(root_directory))

        self.root_directory = root_directory

        self.full_base_directory = os.path.join(
            self.root_directory,
            self.base_directory,
        )

        safe_mmkdir(str(os.path.dirname(self.full_base_directory)))

    def _get(self, key):
        filepath = os.path.join(
            self.full_base_directory,
            self._convert_key_to_filepath(key)
        )
        with open(filepath, 'r') as infile:
            return infile.read()

    def _set(self, key, value, **kwargs):
        filepath = os.path.join(
            self.full_base_directory,
            self._convert_key_to_filepath(key)
        )
        path, filename = os.path.split(filepath)

        safe_mmkdir(str(path))
        with open(filepath, "wb") as outfile:
            outfile.write(value.encode("utf-8"))
        return filepath

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
                    filepath = file_name
                else:
                    filepath = os.path.join(
                        relative_path,
                        file_name
                    )

                key = self._convert_filepath_to_key(filepath)
                if key:
                    key_list.append(key)

        return key_list

    def has_key(self, key):
        assert isinstance(key, string_types)

        all_keys = self.list_keys()
        return key in all_keys


class FixedLengthTupleS3StoreBackend(FixedLengthTupleStoreBackend):
    """
    Uses an S3 bucket as a store.

    The key to this StoreBackend must be a tuple with fixed length equal to key_length.
    The filepath_template is a string template used to convert the key to a filepath.
    There's a bit of regex magic in _convert_filepath_to_key that reverses this process,
    so that we can write AND read using filenames as keys.
    """
    def __init__(
        self,
        root_directory,
        filepath_template,
        key_length,
        bucket,
        prefix="",
        forbidden_substrings=["/", "\\"],
    ):
        super(FixedLengthTupleS3StoreBackend, self).__init__(
            root_directory=root_directory,
            filepath_template=filepath_template,
            key_length=key_length,
            forbidden_substrings=forbidden_substrings,
        )
        self.bucket = bucket
        self.prefix = prefix


    def _get(self, key):
        s3_object_key = os.path.join(
            self.prefix,
            self._convert_key_to_filepath(key)
        )

        import boto3
        s3 = boto3.client('s3')
        s3_response_object = s3.get_object(Bucket=self.bucket, Key=s3_object_key)
        return s3_response_object['Body'].read().decode(s3_response_object.get("ContentEncoding", 'utf-8'))

    def _set(self, key, value, content_encoding='utf-8', content_type='application/json'):
        s3_object_key = os.path.join(
            self.prefix,
            self._convert_key_to_filepath(key)
        )

        import boto3
        s3 = boto3.resource('s3')
        result_s3 = s3.Object(self.bucket, s3_object_key)
        result_s3.put(Body=value.encode(content_encoding), ContentEncoding=content_encoding, ContentType=content_type)
        return s3_object_key

    def list_keys(self):
        key_list = []

        import boto3
        s3 = boto3.client('s3')

        for s3_object_info in s3.list_objects(Bucket=self.bucket, Prefix=self.prefix)['Contents']:
            s3_object_key = s3_object_info['Key']
            s3_object_key = os.path.relpath(
                s3_object_key,
                self.prefix,
            )

            key = self._convert_filepath_to_key(s3_object_key)
            if key:
                key_list.append(key)

        return key_list

    def has_key(self, key):
        assert isinstance(key, string_types)

        all_keys = self.list_keys()
        return key in all_keys

