from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from hashlib import md5
from typing import Any, List, Optional, Tuple

from great_expectations.core.usage_statistics.util import (
    aggregate_all_core_expectation_types,
)
from great_expectations.util import load_class

logger = logging.getLogger(__name__)


class BaseAnonymizer(ABC):
    # Any class that starts with this __module__ is considered a "core" object
    CORE_GX_OBJECT_MODULE_PREFIX = "great_expectations"

    CORE_GX_EXPECTATION_TYPES = aggregate_all_core_expectation_types()

    def __init__(self, salt: Optional[str] = None) -> None:
        if salt is not None and not isinstance(salt, str):
            logger.error("invalid salt: must provide a string. Setting a random salt.")
            salt = None
        if salt is None:
            import secrets

            self._salt = secrets.token_hex(8)
        else:
            self._salt = salt

    @abstractmethod
    def anonymize(self, obj: Optional[object], **kwargs) -> Any:
        raise NotImplementedError

    @abstractmethod
    def can_handle(self, obj: Optional[object], **kwargs) -> bool:
        raise NotImplementedError

    @staticmethod
    def get_parent_class(
        classes_to_check: Optional[List[type]] = None,
        object_: Optional[object] = None,
        object_class: Optional[type] = None,
        object_config: Optional[dict] = None,
    ) -> Optional[str]:
        """Check if the parent class is a subclass of any core GX class.

        These anonymizers define and provide an optional list of core GX classes_to_check.
        If not provided, the object's inheritance hierarchy is traversed.

        Args:
            classes_to_check: An optinal list of candidate parent classes to iterate through.
            object_: The specific object to analyze.
            object_class: The class of the specific object to analyze.
            object_config: The dictionary configuration of the specific object to analyze.

        Returns:
            The name of the parent class found, or None if no parent class was found.

        Raises:
            AssertionError if no object_, object_class, or object_config is provided.
        """
        assert (
            object_ or object_class or object_config
        ), "Must pass either object_ or object_class or object_config."
        try:
            if object_class is None and object_ is not None:
                object_class = object_.__class__
            elif object_class is None and object_config is not None:
                object_class_name: str = object_config["class_name"]
                object_module_name: str = object_config["module_name"]
                object_class = load_class(object_class_name, object_module_name)

            # Utilize candidate list if provided.
            if classes_to_check:
                for class_to_check in classes_to_check:
                    if issubclass(object_class, class_to_check):  # type: ignore[arg-type] # object_class could be None
                        return class_to_check.__name__
                return None

            # Otherwise, iterate through parents in inheritance hierarchy.
            parents: Tuple[type, ...] = object_class.__bases__  # type: ignore[union-attr] # object_class could be None
            parent_class: type
            for parent_class in parents:
                parent_module_name: str = parent_class.__module__
                if BaseAnonymizer._is_core_great_expectations_class(parent_module_name):
                    return parent_class.__name__

        except AttributeError:
            pass

        return None

    def _anonymize_string(self, string_: Optional[str]) -> Optional[str]:
        """Obsfuscates a given string using an MD5 hash.

        Utilized to anonymize user-specific/sensitive strings in usage statistics payloads.

        Args:
            string_ (Optional[str]): The input string to anonymize.

        Returns:
            The MD5 hash of the input string. If an input of None is provided, the string is untouched.

        Raises:
            TypeError if input string does not adhere to type signature Optional[str].
        """
        if string_ is None:
            return None

        if not isinstance(string_, str):
            raise TypeError(
                f"""The type of the "string_" argument must be a string (Python "str").  The type given is
"{str(type(string_))}", which is illegal.
            """
            )

        salted = self._salt + string_
        return md5(salted.encode("utf-8")).hexdigest()

    def _anonymize_object_info(  # noqa: PLR0913
        self,
        anonymized_info_dict: dict,
        object_: Optional[object] = None,
        object_class: Optional[type] = None,
        object_config: Optional[dict] = None,
        runtime_environment: Optional[dict] = None,
    ) -> dict:
        """Given an object, anonymize relevant fields and return result as a dictionary.

        Args:
            anonymized_info_dict: The payload object to hydrate with anonymized values.
            object_: The specific object to anonymize.
            object_class: The class of the specific object to anonymize.
            object_config: The dictionary configuration of the specific object to anonymize.
            runtime_environment: A dictionary containing relevant runtime information (like class_name and module_name)

        Returns:
            The anonymized_info_dict that's been populated with anonymized values.

        Raises:
            AssertionError if no object_, object_class, or object_config is provided.
        """
        assert (
            object_ or object_class or object_config
        ), "Must pass either object_ or object_class or object_config."

        if runtime_environment is None:
            runtime_environment = {}

        object_class_name: Optional[str] = None
        object_module_name: Optional[str] = None
        try:
            if object_class is None and object_ is not None:
                object_class = object_.__class__
            elif object_class is None and object_config is not None:
                object_class_name = object_config["class_name"]
                object_module_name = object_config.get(
                    "module_name"
                ) or runtime_environment.get("module_name")
                object_class = load_class(
                    object_class_name,  # type: ignore[arg-type] # object_class_name could be None
                    object_module_name,  # type: ignore[arg-type] # object_module_name could be None
                )

            object_class_name = object_class.__name__  # type: ignore[union-attr] # object_class could be None
            object_module_name = object_class.__module__
            parents: Tuple[type, ...] = object_class.__bases__  # type: ignore[union-attr] # object_class could be None

            if self._is_core_great_expectations_class(object_module_name):
                anonymized_info_dict["parent_class"] = object_class_name
            else:
                # Chetan - 20220311 - If we can't identify the class in question, we iterate through the parents.
                # While GX rarely utilizes multiple inheritance when defining core objects (as of v0.14.10),
                # it is important to recognize that this is possibility.
                #
                # In the presence of multiple valid parents, we generate a comma-delimited list.

                parent_class_list: List[str] = []

                parent_class: type
                for parent_class in parents:
                    parent_module_name: str = parent_class.__module__
                    if BaseAnonymizer._is_core_great_expectations_class(
                        parent_module_name
                    ):
                        parent_class_list.append(parent_class.__name__)

                if parent_class_list:
                    concatenated_parent_classes: str = ",".join(
                        cls for cls in parent_class_list
                    )
                    anonymized_info_dict["parent_class"] = concatenated_parent_classes
                    anonymized_info_dict["anonymized_class"] = self._anonymize_string(
                        object_class_name
                    )

            # Catch-all to prevent edge cases from slipping past
            if not anonymized_info_dict.get("parent_class"):
                anonymized_info_dict["parent_class"] = "__not_recognized__"
                anonymized_info_dict["anonymized_class"] = self._anonymize_string(
                    object_class_name
                )

        except AttributeError:
            anonymized_info_dict["parent_class"] = "__not_recognized__"
            anonymized_info_dict["anonymized_class"] = self._anonymize_string(
                object_class_name
            )

        return anonymized_info_dict

    @staticmethod
    def _is_core_great_expectations_class(class_name: str) -> bool:
        return class_name.startswith(BaseAnonymizer.CORE_GX_OBJECT_MODULE_PREFIX)
