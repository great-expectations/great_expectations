import logging
from hashlib import md5
from typing import Optional

from great_expectations.util import load_class

logger = logging.getLogger(__name__)


class Anonymizer:
    """Anonymize string names in an optionally-consistent way."""

    def __init__(self, salt=None):
        if salt is not None and not isinstance(salt, str):
            logger.error("invalid salt: must provide a string. Setting a random salt.")
            salt = None
        if salt is None:
            import secrets

            self._salt = secrets.token_hex(8)
        else:
            self._salt = salt

    @property
    def salt(self):
        return self._salt

    def anonymize(self, string_):
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

    def anonymize_object_info(
        self,
        anonymized_info_dict: dict,
        object_: Optional[object] = None,
        object_class: Optional[type] = None,
        object_config: Optional[dict] = None,
        runtime_environment: Optional[dict] = None,
    ) -> dict:
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
                object_class_name = object_config.get("class_name")
                object_module_name = object_config.get(
                    "module_name"
                ) or runtime_environment.get("module_name")
                object_class = load_class(object_class_name, object_module_name)

            object_class_name = object_class.__name__
            object_module_name = object_class.__module__

            # Is the class in question a core GE class?
            # What is the parent of the class in question?

            if not object_module_name.startswith("great_expectations"):
                anonymized_info_dict["anonymized_class"] = self.anonymize(
                    object_class_name
                )

            bases = object_class.__bases__
            parent_class = bases[0]
            parent_module_name = parent_class.__module__
            if parent_module_name.startswith("great_expectations"):
                anonymized_info_dict["parent_class"] = parent_class.__name__
            else:
                anonymized_info_dict["parent_class"] = "__not_recognized__"

        except (AttributeError, IndexError):
            anonymized_info_dict["parent_class"] = "__not_recognized__"
            anonymized_info_dict["anonymized_class"] = self.anonymize(object_class_name)

        return anonymized_info_dict

    @staticmethod
    def _is_parent_class_recognized(
        classes_to_check,
        object_=None,
        object_class=None,
        object_config=None,
    ) -> Optional[str]:
        """
        Check if the parent class is a subclass of any core GE class.
        This private method is intended to be used by anonymizers in a public `is_parent_class_recognized()` method. These anonymizers define and provide the core GE classes_to_check.
        Returns:
            The name of the parent class found, or None if no parent class was found
        """
        assert (
            object_ or object_class or object_config
        ), "Must pass either object_ or object_class or object_config."
        try:
            if object_class is None and object_ is not None:
                object_class = object_.__class__
            elif object_class is None and object_config is not None:
                object_class_name = object_config.get("class_name")
                object_module_name = object_config.get("module_name")
                object_class = load_class(object_class_name, object_module_name)

            for class_to_check in classes_to_check:
                if issubclass(object_class, class_to_check):
                    return class_to_check.__name__

            return None

        except AttributeError:
            return None
