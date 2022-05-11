import abc
from typing import Callable


class DataSampler(abc.ABC):
    """Abstract base class containing methods for sampling data accessible via Execution Engines."""

    def get_sampler_method(self, sampler_method_name: str) -> Callable:
        """Get the appropriate sampler method from the method name.

        Args:
            sampler_method_name: name of the sampler to retrieve.

        Returns:
            sampler method.
        """
        sampler_method_name: str = self._get_sampler_method_name(sampler_method_name)

        return getattr(self, sampler_method_name)

    def _get_sampler_method_name(self, sampler_method_name: str) -> str:
        """Accept sampler methods with or without starting with `_`.

        Args:
            sampler_method_name: sampler name starting with or without preceding `_`.

        Returns:
            sampler method name stripped of preceding underscore.
        """
        if sampler_method_name.startswith("_"):
            return sampler_method_name[1:]
        else:
            return sampler_method_name
