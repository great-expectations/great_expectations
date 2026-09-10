# PYTHON 2 - py2 - update to ABC direct use rather than __metaclass__ once we drop py2 support
from __future__ import annotations

import errno
import logging
import os
import pathlib
import random
import re
import shutil
from abc import ABCMeta
from pathlib import Path
from typing import Any, List, Tuple

from great_expectations.compatibility.typing_extensions import override
from great_expectations.data_context.store.store_backend import StoreBackend
from great_expectations.exceptions import InvalidKeyError, StoreBackendError
from great_expectations.util import filter_properties_dict

logger = logging.getLogger(__name__)


class TupleStoreBackend(StoreBackend, metaclass=ABCMeta):
    r"""
    If filepath_template is provided, the key to this StoreBackend abstract class must be a tuple with
    fixed length equal to the number of unique components matching the regex r"{\d+}"

    For example, in the following template path: expectations/{0}/{1}/{2}/prefix-{2}.json, keys must have
    three components.
    """  # noqa: E501 # FIXME CoP

    def __init__(  # noqa: PLR0913 # FIXME CoP
        self,
        filepath_template=None,
        filepath_prefix=None,
        filepath_suffix=None,
        forbidden_substrings=None,
        platform_specific_separator=True,
        fixed_length_key=False,
        suppress_store_backend_id=False,
        manually_initialize_store_backend_id: str = "",
        base_public_path=None,
        store_name=None,
    ) -> None:
        super().__init__(
            fixed_length_key=fixed_length_key,
            suppress_store_backend_id=suppress_store_backend_id,
            manually_initialize_store_backend_id=manually_initialize_store_backend_id,
            store_name=store_name,
        )
        if forbidden_substrings is None:
            forbidden_substrings = ["/", "\\"]
        self.forbidden_substrings = forbidden_substrings
        self.platform_specific_separator = platform_specific_separator

        if filepath_template is not None and filepath_suffix is not None:
            raise ValueError("filepath_suffix may only be used when filepath_template is None")  # noqa: TRY003 # FIXME CoP

        self.filepath_template = filepath_template
        if filepath_prefix and len(filepath_prefix) > 0:
            # Validate that the filepath prefix does not end with a forbidden substring
            if filepath_prefix[-1] in self.forbidden_substrings:
                raise StoreBackendError(
                    "Unable to initialize TupleStoreBackend: filepath_prefix may not end with a "
                    "forbidden substring. Current forbidden substrings are "
                    + str(forbidden_substrings)
                )
        self.filepath_prefix = filepath_prefix
        self.filepath_suffix = filepath_suffix
        self.base_public_path = base_public_path

        if filepath_template is not None:
            # key length is the number of unique values to be substituted in the filepath_template
            self.key_length = len(set(re.findall(r"{\d+}", filepath_template)))

            self.verify_that_key_to_filepath_operation_is_reversible()
            self._fixed_length_key = True

    @staticmethod
    def _is_missing_prefix_or_suffix(filepath_prefix: str, filepath_suffix: str, key: str) -> bool:
        missing_prefix = bool(filepath_prefix and not key.startswith(filepath_prefix))
        missing_suffix = bool(filepath_suffix and not key.endswith(filepath_suffix))
        return missing_prefix or missing_suffix

    @override
    def _validate_key(self, key) -> None:
        super()._validate_key(key)

        for key_element in key:
            for substring in self.forbidden_substrings:
                if substring in key_element:
                    raise ValueError(  # noqa: TRY003 # FIXME CoP
                        f"Keys in {self.__class__.__name__} must not contain substrings in {self.forbidden_substrings} : {key}"  # noqa: E501 # FIXME CoP
                    )

    @override
    def _validate_value(self, value) -> None:
        if not isinstance(value, str) and not isinstance(value, bytes):
            raise TypeError(  # noqa: TRY003 # FIXME CoP
                f"Values in {self.__class__.__name__} must be instances of {str} or {bytes}, not {type(value)}"  # noqa: E501 # FIXME CoP
            )

    def _convert_key_to_filepath(self, key):
        # NOTE: This method uses a hard-coded forward slash as a separator,
        # and then replaces that with a platform-specific separator if requested (the default)
        self._validate_key(key)
        # Handle store_backend_id separately
        if key == self.STORE_BACKEND_ID_KEY:
            filepath = f"{self.filepath_prefix or ''}{'/' if self.filepath_prefix else ''}{key[0]}"
            return filepath if not self.platform_specific_separator else os.path.normpath(filepath)
        if self.filepath_template:
            converted_string = self.filepath_template.format(*list(key))
        else:
            converted_string = "/".join(key)

        if self.filepath_prefix:
            converted_string = f"{self.filepath_prefix}/{converted_string}"
        if self.filepath_suffix:
            converted_string += self.filepath_suffix
        if self.platform_specific_separator:
            converted_string = os.path.normpath(converted_string)

        return converted_string

    def _convert_filepath_to_key(self, filepath):  # noqa: C901, PLR0912 # FIXME CoP
        if filepath == self.STORE_BACKEND_ID_KEY[0]:
            return self.STORE_BACKEND_ID_KEY
        if self.platform_specific_separator:
            filepath = os.path.normpath(filepath)

        if self.filepath_prefix:
            if (
                not filepath.startswith(self.filepath_prefix)
                and len(filepath) >= len(self.filepath_prefix) + 1
            ):
                # If filepath_prefix is set, we expect that it is the first component of a valid filepath.  # noqa: E501 # FIXME CoP
                raise ValueError(  # noqa: TRY003 # FIXME CoP
                    "filepath must start with the filepath_prefix when one is set by the store_backend"  # noqa: E501 # FIXME CoP
                )
            else:
                # Remove the prefix before processing
                # Also remove the separator that was added, which may have been platform-dependent
                filepath = filepath[len(self.filepath_prefix) + 1 :]

        if self.filepath_suffix:
            if not filepath.endswith(self.filepath_suffix):
                # If filepath_suffix is set, we expect that it is the last component of a valid filepath.  # noqa: E501 # FIXME CoP
                raise ValueError(  # noqa: TRY003 # FIXME CoP
                    "filepath must end with the filepath_suffix when one is set by the store_backend"  # noqa: E501 # FIXME CoP
                )
            else:
                # Remove the suffix before processing
                filepath = filepath[: -len(self.filepath_suffix)]

        if self.filepath_template:
            # filepath_template is always specified with forward slashes, but it is then
            # used to (1) dynamically construct and evaluate a regex, and (2) split the provided (observed) filepath  # noqa: E501 # FIXME CoP
            if self.platform_specific_separator:
                filepath_template = os.path.join(  # noqa: PTH118 # FIXME CoP
                    *self.filepath_template.split("/")
                )
                filepath_template = filepath_template.replace("\\", "\\\\")
            else:
                filepath_template = self.filepath_template

            # Convert the template to a regex
            indexed_string_substitutions = re.findall(r"{\d+}", filepath_template)
            tuple_index_list = [
                f"(?P<tuple_index_{i}>.*)" for i in range(len(indexed_string_substitutions))
            ]
            intermediate_filepath_regex = re.sub(
                r"{\d+}",
                lambda m,
                r=iter(  # noqa: B008 # function-call-in-default-argument
                    tuple_index_list
                ): next(r),
                filepath_template,
            )
            filepath_regex = intermediate_filepath_regex.format(*tuple_index_list)

            # Apply the regex to the filepath
            matches = re.compile(filepath_regex).match(filepath)
            if matches is None:
                return None

            # Map key elements into the appropriate parts of the tuple
            new_key = [None] * self.key_length
            for i in range(len(tuple_index_list)):
                tuple_index = int(re.search(r"\d+", indexed_string_substitutions[i]).group(0))
                key_element = matches.group(f"tuple_index_{i!s}")
                new_key[tuple_index] = key_element

            new_key = tuple(new_key)
        else:
            new_key = pathlib.Path(filepath).parts
        return new_key

    def verify_that_key_to_filepath_operation_is_reversible(self):
        def get_random_hex(size=4):
            return "".join([random.choice(list("ABCDEF0123456789")) for _ in range(size)])

        key = tuple(get_random_hex() for _ in range(self.key_length))
        filepath = self._convert_key_to_filepath(key)
        new_key = self._convert_filepath_to_key(filepath)
        if key != new_key:
            raise ValueError(  # noqa: TRY003 # FIXME CoP
                f"filepath template {self.filepath_template} for class {self.__class__.__name__} is not reversible for a tuple of length {self.key_length}. "  # noqa: E501 # FIXME CoP
                "Have you included all elements in the key tuple?"
            )

    @property
    @override
    def config(self) -> dict:
        return self._config  # type: ignore[attr-defined] # FIXME CoP


class TupleFilesystemStoreBackend(TupleStoreBackend):
    """Uses a local filepath as a store.

    The key to this StoreBackend must be a tuple with fixed length based on the filepath_template,
    or a variable-length tuple may be used and returned with an optional filepath_suffix (to be) added.
    The filepath_template is a string template used to convert the key to a filepath.
    """  # noqa: E501 # FIXME CoP

    def __init__(  # noqa: PLR0913 # FIXME CoP
        self,
        base_directory,
        filepath_template=None,
        filepath_prefix=None,
        filepath_suffix=None,
        forbidden_substrings=None,
        platform_specific_separator=True,
        root_directory=None,
        fixed_length_key=False,
        suppress_store_backend_id=False,
        manually_initialize_store_backend_id: str = "",
        base_public_path=None,
        store_name=None,
    ) -> None:
        super().__init__(
            filepath_template=filepath_template,
            filepath_prefix=filepath_prefix,
            filepath_suffix=filepath_suffix,
            forbidden_substrings=forbidden_substrings,
            platform_specific_separator=platform_specific_separator,
            fixed_length_key=fixed_length_key,
            suppress_store_backend_id=suppress_store_backend_id,
            manually_initialize_store_backend_id=manually_initialize_store_backend_id,
            base_public_path=base_public_path,
            store_name=store_name,
        )
        if os.path.isabs(base_directory):  # noqa: PTH117 # FIXME CoP
            self.full_base_directory = base_directory
        else:  # noqa: PLR5501 # FIXME CoP
            if root_directory is None:
                raise ValueError(  # noqa: TRY003 # FIXME CoP
                    "base_directory must be an absolute path if root_directory is not provided"
                )
            elif not os.path.isabs(root_directory):  # noqa: PTH117 # FIXME CoP
                raise ValueError(  # noqa: TRY003 # FIXME CoP
                    f"root_directory must be an absolute path. Got {root_directory} instead."
                )
            else:
                self.full_base_directory = os.path.join(  # noqa: PTH118 # FIXME CoP
                    root_directory, base_directory
                )

        # The parent directory is created eagerly here for convenience only: _set and _move
        # (below) create their own leaf directory before writing, so this eager creation is
        # redundant for an actual write. On a read-only filesystem it must not abort
        # construction: reads require no directory, and a genuine write still raises clearly
        # later at write time. The tolerance is narrowed to the two read-only signals so a real
        # fault on a writable filesystem (e.g. out-of-space, a path collision) still fails fast
        # at construction, exactly as before:
        #   - read-only mount -> OSError with errno EROFS
        #   - read-only dir   -> PermissionError (EACCES/EPERM), a subclass of OSError
        try:
            os.makedirs(  # noqa: PTH103 # FIXME CoP
                str(os.path.dirname(self.full_base_directory)),  # noqa: PTH120 # FIXME CoP
                exist_ok=True,
            )
        except OSError as e:
            if not isinstance(e, PermissionError) and e.errno != errno.EROFS:
                raise
            logger.debug(
                f"Could not pre-create directory for store backend at "
                f"{self.full_base_directory}: {e}. It will be created on first write."
            )
        # Initialize with store_backend_id if not part of an HTMLSiteStore
        if not self._suppress_store_backend_id:
            _ = self.store_backend_id

        # Gather the call arguments of the present function (include the "module_name" and add the "class_name"), filter  # noqa: E501 # FIXME CoP
        # out the Falsy values, and set the instance "_config" variable equal to the resulting dictionary.  # noqa: E501 # FIXME CoP
        self._config = {
            "base_directory": base_directory,
            "filepath_template": filepath_template,
            "filepath_prefix": filepath_prefix,
            "filepath_suffix": filepath_suffix,
            "forbidden_substrings": forbidden_substrings,
            "platform_specific_separator": platform_specific_separator,
            "root_directory": root_directory,
            "fixed_length_key": fixed_length_key,
            "suppress_store_backend_id": suppress_store_backend_id,
            "manually_initialize_store_backend_id": manually_initialize_store_backend_id,
            "base_public_path": base_public_path,
            "store_name": store_name,
            "module_name": self.__class__.__module__,
            "class_name": self.__class__.__name__,
        }
        filter_properties_dict(properties=self._config, clean_falsy=True, inplace=True)

    def _get(self, key):  # type: ignore[explicit-override] # FIXME
        filepath: str = os.path.join(  # noqa: PTH118 # FIXME CoP
            self.full_base_directory, self._convert_key_to_filepath(key)
        )
        try:
            with open(filepath, encoding="utf-8") as infile:
                contents: str = infile.read().rstrip("\n")
        except FileNotFoundError as e:
            raise InvalidKeyError(  # noqa: TRY003 # FIXME CoP
                f"Unable to retrieve object from TupleFilesystemStoreBackend with the following Key: {filepath!s}"  # noqa: E501 # FIXME CoP
            ) from e

        return contents

    @override
    def _get_all(self) -> list[Any]:
        keys = [key for key in self.list_keys() if key != StoreBackend.STORE_BACKEND_ID_KEY]
        return [self._get(key) for key in keys]

    def _set(self, key, value, **kwargs):  # type: ignore[explicit-override] # FIXME
        if not isinstance(key, tuple):
            key = key.to_tuple()
        filepath = os.path.join(  # noqa: PTH118 # FIXME CoP
            self.full_base_directory, self._convert_key_to_filepath(key)
        )
        path, _filename = os.path.split(filepath)

        os.makedirs(str(path), exist_ok=True)  # noqa: PTH103 # FIXME CoP
        with open(filepath, "wb") as outfile:
            if isinstance(value, str):
                outfile.write(value.encode("utf-8"))
            else:
                outfile.write(value)
        return filepath

    def _move(self, source_key, dest_key, **kwargs):  # type: ignore[explicit-override] # FIXME
        source_path = os.path.join(  # noqa: PTH118 # FIXME CoP
            self.full_base_directory, self._convert_key_to_filepath(source_key)
        )

        dest_path = os.path.join(  # noqa: PTH118 # FIXME CoP
            self.full_base_directory, self._convert_key_to_filepath(dest_key)
        )
        dest_dir, _dest_filename = os.path.split(dest_path)

        if os.path.exists(source_path):  # noqa: PTH110 # FIXME CoP
            os.makedirs(dest_dir, exist_ok=True)  # noqa: PTH103 # FIXME CoP
            shutil.move(source_path, dest_path)
            return dest_key

        return False

    @override
    def list_keys(self, prefix: Tuple = ()) -> List[Tuple]:
        key_list = []
        for root, dirs, files in os.walk(
            os.path.join(self.full_base_directory, *prefix)  # noqa: PTH118 # FIXME CoP
        ):
            for file_ in files:
                full_path, file_name = os.path.split(
                    os.path.join(root, file_)  # noqa: PTH118 # FIXME CoP
                )
                relative_path = os.path.relpath(
                    full_path,
                    self.full_base_directory,
                )
                if relative_path == ".":
                    filepath = file_name
                else:
                    filepath = os.path.join(relative_path, file_name)  # noqa: PTH118 # FIXME CoP

                if self._is_missing_prefix_or_suffix(
                    filepath_prefix=self.filepath_prefix,
                    filepath_suffix=self.filepath_suffix,
                    key=filepath,
                ):
                    continue
                key = self._convert_filepath_to_key(filepath)
                if key and not self.is_ignored_key(key):
                    key_list.append(key)

        return key_list

    def rrmdir(self, mroot, curpath) -> None:
        """
        recursively removes empty dirs between curpath and mroot inclusive
        """
        try:
            while (
                not Path(curpath).iterdir()
                and os.path.exists(curpath)  # noqa: PTH110 # FIXME CoP
                and mroot != curpath
            ):
                f2 = os.path.dirname(curpath)  # noqa: PTH120 # FIXME CoP
                os.rmdir(curpath)  # noqa: PTH106 # FIXME CoP
                curpath = f2
        except (NotADirectoryError, FileNotFoundError):
            pass

    def remove_key(self, key):  # type: ignore[explicit-override] # FIXME
        if not isinstance(key, tuple):
            key = key.to_tuple()

        filepath = os.path.join(  # noqa: PTH118 # FIXME CoP
            self.full_base_directory, self._convert_key_to_filepath(key)
        )

        if os.path.exists(filepath):  # noqa: PTH110 # FIXME CoP
            d_path = os.path.dirname(filepath)  # noqa: PTH120 # FIXME CoP
            os.remove(filepath)  # noqa: PTH107 # FIXME CoP
            self.rrmdir(self.full_base_directory, d_path)
            return True
        return False

    @override
    def get_url_for_key(self, key, protocol=None) -> str:
        path = self._convert_key_to_filepath(key)
        escaped_path = self._url_path_escape_special_characters(path=path)
        full_path = os.path.join(self.full_base_directory, escaped_path)  # noqa: PTH118 # FIXME CoP

        if protocol is None:
            protocol = "file:"
        url = f"{protocol}//{full_path}"
        return url

    def get_public_url_for_key(self, key, protocol=None):
        if not self.base_public_path:
            raise StoreBackendError(  # noqa: TRY003 # FIXME CoP
                """Error: No base_public_path was configured!
                    - A public URL was requested base_public_path was not configured for the TupleFilesystemStoreBackend
                """  # noqa: E501 # FIXME CoP
            )
        path = self._convert_key_to_filepath(key)
        public_url = self.base_public_path + path
        return public_url

    def _has_key(self, key):  # type: ignore[explicit-override] # FIXME
        return os.path.isfile(  # noqa: PTH113 # FIXME CoP
            os.path.join(  # noqa: PTH118 # FIXME CoP
                self.full_base_directory, self._convert_key_to_filepath(key)
            )
        )

    @property
    @override
    def config(self) -> dict:
        return self._config
