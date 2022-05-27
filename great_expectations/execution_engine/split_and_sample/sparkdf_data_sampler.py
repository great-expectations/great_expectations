import hashlib
import logging

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.id_dict import BatchSpec
from great_expectations.execution_engine.split_and_sample.data_sampler import (
    DataSampler,
)

logger = logging.getLogger(__name__)

try:
    import pyspark
    import pyspark.sql.functions as F

    # noinspection SpellCheckingInspection
    import pyspark.sql.types as sparktypes
    from pyspark.sql import DataFrame

except ImportError:
    pyspark = None
    DataFrame = None
    F = None
    # noinspection SpellCheckingInspection
    sparktypes = None

    logger.debug(
        "Unable to load pyspark; install optional spark dependency for support."
    )


class SparkDataSampler(DataSampler):
    """Methods for sampling a Spark dataframe."""

    def sample_using_limit(self, df: DataFrame, batch_spec: BatchSpec) -> DataFrame:
        """Sample the first n rows of data.

        Args:
            df: Spark dataframe.
            batch_spec: BatchSpec with sampling_kwargs for limit sampling e.g. sampling_kwargs={"n": 100}.

        Returns:
            Pandas dataframe reduced to number of items specified in BatchSpec sampling kwargs.
        """
        self.verify_batch_spec_sampling_kwargs_exists(batch_spec)
        self.verify_batch_spec_sampling_kwargs_key_exists("n", batch_spec)

        n: int = batch_spec["sampling_kwargs"]["n"]
        return df.limit(n)

    @staticmethod
    def sample_using_random(df: DataFrame, p: float = 0.1, seed: int = 1):
        """Take a random sample of rows, retaining proportion p"""
        res = (
            df.withColumn("rand", F.rand(seed=seed))
            .filter(F.col("rand") < p)
            .drop("rand")
        )
        return res

    @staticmethod
    def sample_using_mod(
        df: DataFrame,
        column_name: str,
        mod: int,
        value: int,
    ):
        """Take the mod of named column, and only keep rows that match the given value"""
        res = (
            df.withColumn(
                "mod_temp", (F.col(column_name) % mod).cast(sparktypes.IntegerType())
            )
            .filter(F.col("mod_temp") == value)
            .drop("mod_temp")
        )
        return res

    @staticmethod
    def sample_using_a_list(
        df: DataFrame,
        column_name: str,
        value_list: list,
    ):
        """Match the values in the named column against value_list, and only keep the matches"""
        return df.where(F.col(column_name).isin(value_list))

    @staticmethod
    def sample_using_hash(
        df: DataFrame,
        column_name: str,
        hash_digits: int = 1,
        hash_value: str = "f",
        hash_function_name: str = "md5",
    ):
        try:
            getattr(hashlib, str(hash_function_name))
        except (TypeError, AttributeError):
            raise (
                ge_exceptions.ExecutionEngineError(
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

        encrypt_udf = F.udf(_encrypt_value, sparktypes.StringType())
        res = (
            df.withColumn("encrypted_value", encrypt_udf(column_name))
            .filter(F.col("encrypted_value") == hash_value)
            .drop("encrypted_value")
        )
        return res
