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
        self._validate_key(key)

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
        self._validate_key(key)
        
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

    def _validate_key(self, key):
        raise NotImplementedError

    def _get(self, key):
        raise NotImplementedError

    def _set(self, key, value):
        raise NotImplementedError

    def list_keys(self):
        raise NotImplementedError

    def has_key(self, key):
        raise NotImplementedError
