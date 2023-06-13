import hashlib
import random

import pandas as pd

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.id_dict import BatchSpec
from great_expectations.execution_engine.split_and_sample.data_sampler import (
    DataSampler,
)


class PandasDataSampler(DataSampler):
    """Methods for sampling a pandas dataframe."""

    def sample_using_limit(
        self, df: pd.DataFrame, batch_spec: BatchSpec
    ) -> pd.DataFrame:
        """Sample the first n rows of data.

        Args:
            df: pandas dataframe.
            batch_spec: Should contain key `n` in sampling_kwargs, the number of
                values in the sample e.g. sampling_kwargs={"n": 100}.

        Returns:
            Sampled dataframe

        Raises:
            SamplerError
        """
        self.verify_batch_spec_sampling_kwargs_exists(batch_spec)
        self.verify_batch_spec_sampling_kwargs_key_exists("n", batch_spec)

        n: int = batch_spec["sampling_kwargs"]["n"]
        return df.head(n)

    def sample_using_random(
        self,
        df: pd.DataFrame,
        batch_spec: BatchSpec,
    ) -> pd.DataFrame:
        """Take a random sample of rows, retaining proportion p.

        Args:
            df: dataframe to sample
            batch_spec: Can contain key `p` (float) which defaults to 0.1
                if not provided.

        Returns:
            Sampled dataframe

        Raises:
            SamplerError
        """
        p: float = self.get_sampling_kwargs_value_or_default(
            batch_spec=batch_spec, sampling_kwargs_key="p", default_value=0.1
        )
        return df[df.index.map(lambda x: random.random() < p)]

    def sample_using_mod(
        self,
        df: pd.DataFrame,
        batch_spec: BatchSpec,
    ) -> pd.DataFrame:
        """Take the mod of named column, and only keep rows that match the given value.

        Args:
            df: dataframe to sample
            batch_spec: should contain keys `column_name`, `mod` and `value`

        Returns:
            Sampled dataframe

        Raises:
            SamplerError
        """
        self.verify_batch_spec_sampling_kwargs_exists(batch_spec)
        self.verify_batch_spec_sampling_kwargs_key_exists("column_name", batch_spec)
        self.verify_batch_spec_sampling_kwargs_key_exists("mod", batch_spec)
        self.verify_batch_spec_sampling_kwargs_key_exists("value", batch_spec)
        column_name: str = self.get_sampling_kwargs_value_or_default(
            batch_spec, "column_name"
        )
        mod: int = self.get_sampling_kwargs_value_or_default(batch_spec, "mod")
        value: int = self.get_sampling_kwargs_value_or_default(batch_spec, "value")

        return df[df[column_name].map(lambda x: x % mod == value)]

    def sample_using_a_list(
        self,
        df: pd.DataFrame,
        batch_spec: BatchSpec,
    ) -> pd.DataFrame:
        """Match the values in the named column against value_list, and only keep the matches.

        Args:
            df: dataframe to sample
            batch_spec: should contain keys `column_name` and `value_list`

        Returns:
            Sampled dataframe

        Raises:
            SamplerError
        """
        self.verify_batch_spec_sampling_kwargs_exists(batch_spec)
        self.verify_batch_spec_sampling_kwargs_key_exists("column_name", batch_spec)
        self.verify_batch_spec_sampling_kwargs_key_exists("value_list", batch_spec)

        column_name: str = self.get_sampling_kwargs_value_or_default(
            batch_spec, "column_name"
        )
        value_list: int = self.get_sampling_kwargs_value_or_default(
            batch_spec, "value_list"
        )

        return df[df[column_name].isin(value_list)]

    def sample_using_hash(
        self,
        df: pd.DataFrame,
        batch_spec: BatchSpec,
    ) -> pd.DataFrame:
        """Hash the values in the named column, and only keep rows that match the given hash_value.

        Args:
            df: dataframe to sample
            batch_spec: should contain keys `column_name` and optionally `hash_digits`
                (default is 1 if not provided), `hash_value` (default is "f" if not provided),
                and `hash_function_name` (default is "md5" if not provided)

        Returns:
            Sampled dataframe

        Raises:
            SamplerError
        """
        self.verify_batch_spec_sampling_kwargs_exists(batch_spec)
        self.verify_batch_spec_sampling_kwargs_key_exists("column_name", batch_spec)
        column_name: str = self.get_sampling_kwargs_value_or_default(
            batch_spec, "column_name"
        )
        hash_digits: int = self.get_sampling_kwargs_value_or_default(
            batch_spec=batch_spec, sampling_kwargs_key="hash_digits", default_value=1
        )
        hash_value: str = self.get_sampling_kwargs_value_or_default(
            batch_spec=batch_spec, sampling_kwargs_key="hash_value", default_value="f"
        )

        hash_function_name: str = self.get_sampling_kwargs_value_or_default(
            batch_spec=batch_spec,
            sampling_kwargs_key="hash_function_name",
            default_value="md5",
        )

        try:
            hash_func = getattr(hashlib, hash_function_name)
        except (TypeError, AttributeError):
            raise (
                gx_exceptions.ExecutionEngineError(
                    f"""The sampling method used with PandasExecutionEngine has a reference to an invalid hash_function_name.
                       Reference to {hash_function_name} cannot be found."""
                )
            )

        matches = df[column_name].map(
            lambda x: hash_func(str(x).encode()).hexdigest()[-1 * hash_digits :]
            == hash_value
        )
        return df[matches]
