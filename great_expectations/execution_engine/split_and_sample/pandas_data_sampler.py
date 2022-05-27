import hashlib
import random

import pandas as pd

import great_expectations.exceptions as ge_exceptions
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
            batch_spec: BatchSpec with sampling_kwargs for limit sampling e.g. sampling_kwargs={"n": 100}.

        Returns:
            Pandas dataframe reduced to number of items specified in BatchSpec sampling kwargs.
        """
        self.verify_batch_spec_sampling_kwargs_exists(batch_spec)
        self.verify_batch_spec_sampling_kwargs_key_exists("n", batch_spec)

        n: int = batch_spec["sampling_kwargs"]["n"]
        return df.head(n)

    @staticmethod
    def sample_using_random(
        df: pd.DataFrame,
        p: float = 0.1,
    ):
        """Take a random sample of rows, retaining proportion p"""
        return df[df.index.map(lambda x: random.random() < p)]

    @staticmethod
    def sample_using_mod(
        df: pd.DataFrame,
        column_name: str,
        mod: int,
        value: int,
    ):
        """Take the mod of named column, and only keep rows that match the given value"""
        return df[df[column_name].map(lambda x: x % mod == value)]

    @staticmethod
    def sample_using_a_list(
        df: pd.DataFrame,
        column_name: str,
        value_list: list,
    ):
        """Match the values in the named column against value_list, and only keep the matches"""
        return df[df[column_name].isin(value_list)]

    @staticmethod
    def sample_using_hash(
        df: pd.DataFrame,
        column_name: str,
        hash_digits: int = 1,
        hash_value: str = "f",
        hash_function_name: str = "md5",
    ):
        """Hash the values in the named column, and only keep rows that match the given hash_value"""
        try:
            hash_func = getattr(hashlib, hash_function_name)
        except (TypeError, AttributeError):
            raise (
                ge_exceptions.ExecutionEngineError(
                    f"""The sampling method used with PandasExecutionEngine has a reference to an invalid hash_function_name.
                       Reference to {hash_function_name} cannot be found."""
                )
            )

        matches = df[column_name].map(
            lambda x: hash_func(str(x).encode()).hexdigest()[-1 * hash_digits :]
            == hash_value
        )
        return df[matches]
