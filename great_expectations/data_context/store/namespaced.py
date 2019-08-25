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
    NamespacedInMemoryStoreConfig,
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

from .store import (
    Store
)
from .basic import (
    InMemoryStore,
    FilesystemStore,
)

class NamespacedStore(Store):

    @property
    def resource_identifier_class(self):
        module = importlib.import_module("great_expectations.data_context.types.resource_identifiers")
        class_ = getattr(module, self.config.resource_identifier_class_name)
        return class_

    def _validate_key(self, key):
        if not isinstance(key, self.resource_identifier_class):
            raise TypeError("key: {!r} must be a DataContextResourceIdentifier, not {!r}".format(
                key,
                type(key),
            ))


class NamespacedInMemoryStore(NamespacedStore, InMemoryStore): 
    """
    """

    config_class = NamespacedInMemoryStoreConfig

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


class NamespacedFilesystemStore(NamespacedStore, FilesystemStore):

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
            return self.resource_identifier_class(*args)

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