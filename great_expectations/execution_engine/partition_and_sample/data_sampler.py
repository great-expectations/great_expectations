from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Any, Callable, Optional, TypeVar

import great_expectations.exceptions as gx_exceptions

if TYPE_CHECKING:
    from great_expectations.core.id_dict import BatchSpec

T = TypeVar("T")


class DataSampler(abc.ABC):  # noqa: B024 # abstract-base-class-without-abstract-method
    """Abstract base class containing methods for sampling data accessible via Execution Engines."""

    def get_sampler_method(self, sampler_method_name: str) -> Callable:
        """Get the appropriate sampler method from the method name.

        Args:
            sampler_method_name: name of the sampler to retrieve.

        Returns:
            sampler method.
        """
        sampler_method_name = self._get_sampler_method_name(sampler_method_name)

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
            raise gx_exceptions.SamplerError(
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
            raise gx_exceptions.SamplerError(
                f"Please make sure to provide the {key} key in sampling_kwargs in addition to your sampling_method."
            )

    @staticmethod
    def get_sampling_kwargs_value_or_default(
        batch_spec: BatchSpec,
        sampling_kwargs_key: str,
        default_value: Optional[T] = None,
    ) -> T | Any:
        """Get value from batch_spec or default if provided and key doesn't exist.

        Args:
            batch_spec: BatchSpec to retrieve value from.
            sampling_kwargs_key: key for value to retrieve.
            default_value: value to return if key doesn't exist.

        Returns:
            Value from batch_spec corresponding to key or default_value if key doesn't exist.
        """
        if "sampling_kwargs" in batch_spec:
            if sampling_kwargs_key in batch_spec["sampling_kwargs"]:
                return batch_spec["sampling_kwargs"][sampling_kwargs_key]
            else:
                return default_value
        else:
            return default_value
