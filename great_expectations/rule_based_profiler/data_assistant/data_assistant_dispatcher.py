
import logging
from typing import Dict, Optional, Set, Type
from great_expectations.rule_based_profiler.data_assistant import DataAssistant
from great_expectations.rule_based_profiler.data_assistant.data_assistant_runner import DataAssistantRunner
logger = logging.getLogger(__name__)

class DataAssistantDispatcher():
    '\n    DataAssistantDispatcher intercepts requests for "DataAssistant" classes by their registered names and manages their\n    associated "DataAssistantRunner" objects, which process invocations of calls to "DataAssistant" "run()" methods.\n    '
    _registered_data_assistants: Dict[(str, Type[DataAssistant])] = {}

    def __init__(self, data_context: 'BaseDataContext') -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Args:\n            data_context: BaseDataContext associated with DataAssistantDispatcher\n        '
        self._data_context = data_context
        self._data_assistant_runner_cache = {}

    def __getattr__(self, name: str) -> DataAssistantRunner:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        data_assistant_cls: Optional[Type[DataAssistant]] = DataAssistantDispatcher.get_data_assistant_impl(name=name)
        if (data_assistant_cls is None):
            raise AttributeError(f'"{type(self).__name__}" object has no attribute "{name}".')
        data_assistant_name: str = data_assistant_cls.data_assistant_type
        data_assistant_runner: Optional[DataAssistantRunner] = self._data_assistant_runner_cache.get(data_assistant_name)
        if (data_assistant_runner is None):
            data_assistant_runner = DataAssistantRunner(data_assistant_cls=data_assistant_cls, data_context=self._data_context)
            self._data_assistant_runner_cache[data_assistant_name] = data_assistant_runner
        return data_assistant_runner

    @classmethod
    def register_data_assistant(cls, data_assistant: Type[DataAssistant]) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        This method registers "DataAssistant" subclass for future instantiation and execution of its "run()" method.\n\n        Args:\n            data_assistant: "DataAssistant" class to be registered\n        '
        data_assistant_type = data_assistant.data_assistant_type
        cls._register(data_assistant_type, data_assistant)
        alias: Optional[str] = data_assistant.__alias__
        if (alias is not None):
            cls._register(alias, data_assistant)

    @classmethod
    def _register(cls, name: str, data_assistant: Type[DataAssistant]) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        registered_data_assistants = cls._registered_data_assistants
        if (name in registered_data_assistants):
            raise ValueError(f'Existing declarations of DataAssistant "{name}" found.')
        logger.debug(f'Registering the declaration of DataAssistant "{name}" took place.')
        registered_data_assistants[name] = data_assistant

    @classmethod
    def get_data_assistant_impl(cls, name: Optional[str]) -> Optional[Type[DataAssistant]]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        This method obtains (previously registered) "DataAssistant" class from DataAssistant Registry.\n\n        Note that it will clean the input string before checking against registered assistants.\n\n        Args:\n            name: String representing "snake case" version of "DataAssistant" class type\n\n        Returns:\n            Class inheriting "DataAssistant" if found; otherwise, None\n        '
        if (name is None):
            return None
        name = name.lower()
        return cls._registered_data_assistants.get(name)

    def __dir__(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        This custom magic method is used to enable tab completion on "DataAssistantDispatcher" objects.\n        '
        data_assistant_dispatcher_attrs: Set[str] = set(super().__dir__())
        data_assistant_registered_names: Set[str] = get_registered_data_assistant_names()
        combined_dir_attrs: Set[str] = (data_assistant_dispatcher_attrs | data_assistant_registered_names)
        return list(combined_dir_attrs)

def get_registered_data_assistant_names() -> Set[str]:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    '\n    This method returns names (registered data_assistant_type and alias name) of registered "DataAssistant" classes.\n    '
    return set(DataAssistantDispatcher._registered_data_assistants.keys())
