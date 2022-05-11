
import abc
from typing import Callable

class DataSampler(abc.ABC):
    'Abstract base class containing methods for sampling data accessible via Execution Engines.'

    def get_sampler_method(self, sampler_method_name: str) -> Callable:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Get the appropriate sampler method from the method name.\n\n        Args:\n            sampler_method_name: name of the sampler to retrieve.\n\n        Returns:\n            sampler method.\n        '
        sampler_method_name: str = self._get_sampler_method_name(sampler_method_name)
        return getattr(self, sampler_method_name)

    def _get_sampler_method_name(self, sampler_method_name: str) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Accept sampler methods with or without starting with `_`.\n\n        Args:\n            sampler_method_name: sampler name starting with or without preceding `_`.\n\n        Returns:\n            sampler method name stripped of preceding underscore.\n        '
        if sampler_method_name.startswith('_'):
            return sampler_method_name[1:]
        else:
            return sampler_method_name
