import abc
from typing import Callable

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.id_dict import BatchSpec


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

    def verify_batch_spec_sampling_kwargs_exists(self, batch_spec: BatchSpec) -> None:
        """Verify that sampling_kwargs key exists in batch_spec or raise error.

        Args:
            batch_spec: Can contain sampling_kwargs.

        Returns:
            None

        Raises:
            SamplerError
        """
        if batch_spec.get("sampling_kwargs") is None:
            raise ge_exceptions.SamplerError(
                "Please make sure to provide sampling_kwargs in addition to your sampling_method."
            )

    def verify_batch_spec_sampling_kwargs_key_exists(
        self, key: str, batch_spec: BatchSpec
    ) -> None:
        """Verify that a key within sampling_kwargs exists in batch_spec or raise error.

        Args:
            batch_spec: Can contain sampling_kwargs with nested keys.

        Returns:
            None

        Raises:
            SamplerError
        """
        if batch_spec["sampling_kwargs"].get(key) is None:
            raise ge_exceptions.SamplerError(
                f"Please make sure to provide the {key} key in sampling_kwargs in addition to your sampling_method."
            )
