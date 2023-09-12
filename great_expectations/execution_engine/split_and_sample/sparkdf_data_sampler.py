import hashlib
import logging

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility import pyspark
from great_expectations.compatibility.pyspark import functions as F
from great_expectations.core.id_dict import BatchSpec
from great_expectations.execution_engine.split_and_sample.data_sampler import (
    DataSampler,
)

logger = logging.getLogger(__name__)


class SparkDataSampler(DataSampler):
    """Methods for sampling a Spark dataframe."""

    def sample_using_limit(
        self, df: pyspark.DataFrame, batch_spec: BatchSpec
    ) -> pyspark.DataFrame:
        """Sample the first n rows of data.

        Args:
            df: Spark dataframe.
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
        return df.limit(n)

    def sample_using_random(
        self, df: pyspark.DataFrame, batch_spec: BatchSpec
    ) -> pyspark.DataFrame:
        """Take a random sample of rows, retaining proportion p.

        Args:
            df: dataframe to sample
            batch_spec: Can contain keys `p` (float), `seed` (int) which
                default to 0.1 and 1 respectively if not provided.

        Returns:
            Sampled dataframe

        Raises:
            SamplerError
        """
        p: float = self.get_sampling_kwargs_value_or_default(
            batch_spec=batch_spec, sampling_kwargs_key="p", default_value=0.1
        )
        seed: int = self.get_sampling_kwargs_value_or_default(
            batch_spec=batch_spec, sampling_kwargs_key="seed", default_value=1
        )
        res = (
            df.withColumn("rand", F.rand(seed=seed))
            .filter(F.col("rand") < p)
            .drop("rand")
        )
        return res

    def sample_using_mod(
        self, df: pyspark.DataFrame, batch_spec: BatchSpec
    ) -> pyspark.DataFrame:
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
        res = (
            df.withColumn(
                "mod_temp", (F.col(column_name) % mod).cast(pyspark.types.IntegerType())
            )
            .filter(F.col("mod_temp") == value)
            .drop("mod_temp")
        )
        return res

    def sample_using_a_list(
        self,
        df: pyspark.DataFrame,
        batch_spec: BatchSpec,
    ) -> pyspark.DataFrame:
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
        value_list: list = self.get_sampling_kwargs_value_or_default(
            batch_spec, "value_list"
        )

        return df.where(F.col(column_name).isin(value_list))

    def sample_using_hash(
        self,
        df: pyspark.DataFrame,
        batch_spec: BatchSpec,
    ) -> pyspark.DataFrame:
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
            getattr(hashlib, str(hash_function_name))
        except (TypeError, AttributeError):
            raise (
                gx_exceptions.ExecutionEngineError(
                    f"""The sampling method used with SparkDFExecutionEngine has a reference to an invalid hash_function_name.
                    Reference to {hash_function_name} cannot be found."""
                )
            )

        def _encrypt_value(to_encode):
            to_encode_str = str(to_encode)
            hash_func = getattr(hashlib, hash_function_name)
            hashed_value = hash_func(to_encode_str.encode()).hexdigest()[
                -1 * hash_digits :
            ]
            return hashed_value

        encrypt_udf = F.udf(_encrypt_value, pyspark.types.StringType())
        res = (
            df.withColumn("encrypted_value", encrypt_udf(column_name))
            .filter(F.col("encrypted_value") == hash_value)
            .drop("encrypted_value")
        )
        return res
