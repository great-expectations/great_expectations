
import logging
from abc import ABC, abstractmethod
from hashlib import md5
from typing import Any, List, Optional, Tuple
from great_expectations.core.usage_statistics.util import aggregate_all_core_expectation_types
from great_expectations.util import load_class
logger = logging.getLogger(__name__)

class BaseAnonymizer(ABC):
    CORE_GE_OBJECT_MODULE_PREFIX = 'great_expectations'
    CORE_GE_EXPECTATION_TYPES = aggregate_all_core_expectation_types()

    def __init__(self, salt: Optional[str]=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if ((salt is not None) and (not isinstance(salt, str))):
            logger.error('invalid salt: must provide a string. Setting a random salt.')
            salt = None
        if (salt is None):
            import secrets
            self._salt = secrets.token_hex(8)
        else:
            self._salt = salt

    @abstractmethod
    def anonymize(self, obj: Optional[object], **kwargs) -> Any:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        raise NotImplementedError

    @abstractmethod
    def can_handle(self, obj: Optional[object], **kwargs) -> bool:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        raise NotImplementedError

    @staticmethod
    def get_parent_class(classes_to_check: Optional[List[type]]=None, object_: Optional[object]=None, object_class: Optional[type]=None, object_config: Optional[dict]=None) -> Optional[str]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        "Check if the parent class is a subclass of any core GE class.\n\n        These anonymizers define and provide an optional list of core GE classes_to_check.\n        If not provided, the object's inheritance hierarchy is traversed.\n\n        Args:\n            classes_to_check: An optinal list of candidate parent classes to iterate through.\n            object_: The specific object to analyze.\n            object_class: The class of the specific object to analyze.\n            object_config: The dictionary configuration of the specific object to analyze.\n\n        Returns:\n            The name of the parent class found, or None if no parent class was found.\n\n        Raises:\n            AssertionError if no object_, object_class, or object_config is provided.\n        "
        assert (object_ or object_class or object_config), 'Must pass either object_ or object_class or object_config.'
        try:
            if ((object_class is None) and (object_ is not None)):
                object_class = object_.__class__
            elif ((object_class is None) and (object_config is not None)):
                object_class_name = object_config.get('class_name')
                object_module_name = object_config.get('module_name')
                object_class = load_class(object_class_name, object_module_name)
            if classes_to_check:
                for class_to_check in classes_to_check:
                    if issubclass(object_class, class_to_check):
                        return class_to_check.__name__
                return None
            parents: Tuple[(type, ...)] = object_class.__bases__
            parent_class: type
            for parent_class in parents:
                parent_module_name: str = parent_class.__module__
                if BaseAnonymizer._is_core_great_expectations_class(parent_module_name):
                    return parent_class.__name__
        except AttributeError:
            pass
        return None

    def _anonymize_string(self, string_: Optional[str]) -> Optional[str]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Obsfuscates a given string using an MD5 hash.\n\n        Utilized to anonymize user-specific/sensitive strings in usage statistics payloads.\n\n        Args:\n            string_ (Optional[str]): The input string to anonymize.\n\n        Returns:\n            The MD5 hash of the input string. If an input of None is provided, the string is untouched.\n\n        Raises:\n            TypeError if input string does not adhere to type signature Optional[str].\n        '
        if (string_ is None):
            return None
        if (not isinstance(string_, str)):
            raise TypeError(f'''The type of the "string_" argument must be a string (Python "str").  The type given is
"{str(type(string_))}", which is illegal.
            ''')
        salted = (self._salt + string_)
        return md5(salted.encode('utf-8')).hexdigest()

    def _anonymize_object_info(self, anonymized_info_dict: dict, object_: Optional[object]=None, object_class: Optional[type]=None, object_config: Optional[dict]=None, runtime_environment: Optional[dict]=None) -> dict:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        "Given an object, anonymize relevant fields and return result as a dictionary.\n\n        Args:\n            anonymized_info_dict: The payload object to hydrate with anonymized values.\n            object_: The specific object to anonymize.\n            object_class: The class of the specific object to anonymize.\n            object_config: The dictionary configuration of the specific object to anonymize.\n            runtime_environment: A dictionary containing relevant runtime information (like class_name and module_name)\n\n        Returns:\n            The anonymized_info_dict that's been populated with anonymized values.\n\n        Raises:\n            AssertionError if no object_, object_class, or object_config is provided.\n        "
        assert (object_ or object_class or object_config), 'Must pass either object_ or object_class or object_config.'
        if (runtime_environment is None):
            runtime_environment = {}
        object_class_name: Optional[str] = None
        object_module_name: Optional[str] = None
        try:
            if ((object_class is None) and (object_ is not None)):
                object_class = object_.__class__
            elif ((object_class is None) and (object_config is not None)):
                object_class_name = object_config.get('class_name')
                object_module_name = (object_config.get('module_name') or runtime_environment.get('module_name'))
                object_class = load_class(object_class_name, object_module_name)
            object_class_name = object_class.__name__
            object_module_name = object_class.__module__
            parents: Tuple[(type, ...)] = object_class.__bases__
            if self._is_core_great_expectations_class(object_module_name):
                anonymized_info_dict['parent_class'] = object_class_name
            else:
                parent_class_list: List[str] = []
                parent_class: type
                for parent_class in parents:
                    parent_module_name: str = parent_class.__module__
                    if BaseAnonymizer._is_core_great_expectations_class(parent_module_name):
                        parent_class_list.append(parent_class.__name__)
                if parent_class_list:
                    concatenated_parent_classes: str = ','.join((cls for cls in parent_class_list))
                    anonymized_info_dict['parent_class'] = concatenated_parent_classes
                    anonymized_info_dict['anonymized_class'] = self._anonymize_string(object_class_name)
            if (not anonymized_info_dict.get('parent_class')):
                anonymized_info_dict['parent_class'] = '__not_recognized__'
                anonymized_info_dict['anonymized_class'] = self._anonymize_string(object_class_name)
        except AttributeError:
            anonymized_info_dict['parent_class'] = '__not_recognized__'
            anonymized_info_dict['anonymized_class'] = self._anonymize_string(object_class_name)
        return anonymized_info_dict

    @staticmethod
    def _is_core_great_expectations_class(class_name: str) -> bool:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return class_name.startswith(BaseAnonymizer.CORE_GE_OBJECT_MODULE_PREFIX)
