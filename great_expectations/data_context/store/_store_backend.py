import logging
import urllib
import uuid
from abc import ABCMeta, abstractmethod
from typing import Any, List, Optional, Union

import pyparsing as pp

from great_expectations.exceptions import InvalidKeyError, StoreBackendError, StoreError

logger = logging.getLogger(__name__)


class StoreBackend(metaclass=ABCMeta):
    """A store backend acts as a key-value store that can accept tuples as keys, to abstract away
    reading and writing to a persistence layer.

    In general a StoreBackend implementation must provide implementations of:
      - _get
      - _set
      - list_keys
      - _has_key
    """

    IGNORED_FILES = [".ipynb_checkpoints"]
    STORE_BACKEND_ID_KEY = (".ge_store_backend_id",)
    STORE_BACKEND_ID_PREFIX = "store_backend_id = "
    STORE_BACKEND_INVALID_CONFIGURATION_ID = "00000000-0000-0000-0000-00000000e003"

    def __init__(
        self,
        fixed_length_key=False,
        suppress_store_backend_id=False,
        manually_initialize_store_backend_id: str = "",
        store_name="no_store_name",
    ) -> None:
        """
        Initialize a StoreBackend
        Args:
            fixed_length_key:
            suppress_store_backend_id: skip construction of a StoreBackend.store_backend_id
            manually_initialize_store_backend_id: UUID as a string to use if the store_backend_id is not already set
            store_name: store name given in the DataContextConfig (via either in-code or yaml configuration)
        """
        self._fixed_length_key = fixed_length_key
        self._suppress_store_backend_id = suppress_store_backend_id
        self._manually_initialize_store_backend_id = (
            manually_initialize_store_backend_id
        )
        self._store_name = store_name

    @property
    def fixed_length_key(self):
        return self._fixed_length_key

    @property
    def store_name(self):
        return self._store_name

    def _construct_store_backend_id(
        self, suppress_warning: bool = False
    ) -> Optional[str]:
        """
        Create a store_backend_id if one does not exist, and return it if it exists
        If a valid UUID store_backend_id is passed in param manually_initialize_store_backend_id
        and there is not already an existing store_backend_id then the store_backend_id
        from param manually_initialize_store_backend_id is used to create it.
        Args:
            suppress_warning: boolean flag for whether warnings are logged

        Returns:
            store_backend_id which is a UUID(version=4)
        """
        if self._suppress_store_backend_id:
            if not suppress_warning:
                logger.warning(
                    f"You are attempting to access the store_backend_id of a store or store_backend named {self.store_name} that has been explicitly suppressed."
                )
            return None
        try:
            try:
                ge_store_backend_id_file_contents = self.get(
                    key=self.STORE_BACKEND_ID_KEY
                )
                store_backend_id_file_parser = self.STORE_BACKEND_ID_PREFIX + pp.Word(
                    f"{pp.hexnums}-"
                )
                parsed_store_backend_id = store_backend_id_file_parser.parseString(
                    ge_store_backend_id_file_contents
                )
                return parsed_store_backend_id[1]
            except InvalidKeyError:
                store_id = (
                    self._manually_initialize_store_backend_id
                    if self._manually_initialize_store_backend_id
                    else str(uuid.uuid4())
                )
                self.set(
                    key=self.STORE_BACKEND_ID_KEY,
                    value=f"{self.STORE_BACKEND_ID_PREFIX}{store_id}\n",
                )
                return store_id
        except Exception as e:
            if not suppress_warning:
                logger.warning(
                    f"Invalid store configuration: Please check the configuration of your {self.__class__.__name__} named {self.store_name}. Exception was: \n {e}"
                )
            return self.STORE_BACKEND_INVALID_CONFIGURATION_ID

    # NOTE: AJB20201130 This store_backend_id and store_backend_id_warnings_suppressed was implemented to remove multiple warnings in DataContext.__init__ but this can be done more cleanly by more carefully going through initialization order in DataContext
    @property
    def store_backend_id(self):
        return self._construct_store_backend_id(suppress_warning=False)

    @property
    def store_backend_id_warnings_suppressed(self):
        return self._construct_store_backend_id(suppress_warning=True)

    def get(self, key, **kwargs):
        self._validate_key(key)
        value = self._get(key, **kwargs)
        return value

    def set(self, key, value, **kwargs):
        self._validate_key(key)
        self._validate_value(value)
        # Allow the implementing setter to return something (e.g. a path used for its key)
        try:
            return self._set(key, value, **kwargs)
        except ValueError as e:
            logger.debug(str(e))
            raise StoreBackendError("ValueError while calling _set on store backend.")

    def add(self, key, value, **kwargs):
        """
        Essentially `set` but validates that a given key-value pair does not already exist.
        """
        return self._add(key=key, value=value, **kwargs)

    def _add(self, key, value, **kwargs):
        if self.has_key(key):
            raise StoreBackendError(f"Store already has the following key: {key}.")
        return self.set(key=key, value=value, **kwargs)

    def update(self, key, value, **kwargs):
        """
        Essentially `set` but validates that a given key-value pair does already exist.
        """
        return self._update(key=key, value=value, **kwargs)

    def _update(self, key, value, **kwargs):
        if not self.has_key(key):
            raise StoreBackendError(
                f"Store does not have a value associated the following key: {key}."
            )
        return self.set(key=key, value=value, **kwargs)

    def add_or_update(self, key, value, **kwargs):
        """
        Conditionally calls `add` or `update` based on the presence of the given key.
        """
        return self._add_or_update(key=key, value=value, **kwargs)

    def _add_or_update(self, key, value, **kwargs):
        if self.has_key(key):
            return self.update(key=key, value=value, **kwargs)
        return self.add(key=key, value=value, **kwargs)

    def move(self, source_key, dest_key, **kwargs):
        self._validate_key(source_key)
        self._validate_key(dest_key)
        return self._move(source_key, dest_key, **kwargs)

    def has_key(self, key) -> bool:
        self._validate_key(key)
        return self._has_key(key)

    @staticmethod
    def _url_path_escape_special_characters(path: str) -> str:
        # will replace special characters with %xx escape
        # this is meant to be used only on the path section of a URL
        # https://docs.python.org/3/library/urllib.parse.html#url-quoting
        return urllib.parse.quote(path)

    def get_url_for_key(self, key, protocol=None) -> None:
        raise StoreError(
            "Store backend of type {:s} does not have an implementation of get_url_for_key".format(
                type(self).__name__
            )
        )

    def _validate_key(self, key) -> None:
        if isinstance(key, tuple):
            for key_element in key:
                if not isinstance(key_element, str):
                    raise TypeError(
                        "Elements within tuples passed as keys to {} must be instances of {}, not {}".format(
                            self.__class__.__name__,
                            str,
                            type(key_element),
                        )
                    )
        else:
            raise TypeError(
                "Keys in {} must be instances of {}, not {}".format(
                    self.__class__.__name__,
                    tuple,
                    type(key),
                )
            )

    def _validate_value(self, value) -> None:  # noqa: B027 # no abstract decorator
        pass

    @abstractmethod
    def _get(self, key) -> None:
        raise NotImplementedError

    @abstractmethod
    def _set(self, key, value, **kwargs) -> None:
        raise NotImplementedError

    @abstractmethod
    def _move(self, source_key, dest_key, **kwargs) -> None:
        raise NotImplementedError

    @abstractmethod
    def list_keys(self, prefix=()) -> Union[List[str], List[tuple]]:
        raise NotImplementedError

    @abstractmethod
    def remove_key(self, key) -> None:
        raise NotImplementedError

    def _has_key(self, key) -> bool:
        raise NotImplementedError

    def is_ignored_key(self, key):
        for ignored in self.IGNORED_FILES:
            if ignored in key:
                return True

        return False

    @property
    def config(self) -> dict:
        raise NotImplementedError

    def build_key(
        self,
        id: Optional[str] = None,
        name: Optional[str] = None,
    ) -> Any:
        """Build a key specific to the store backend implementation."""
        raise NotImplementedError
