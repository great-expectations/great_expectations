import datetime

import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch_spec import RuntimeDataBatchSpec

try:
    pyspark = pytest.importorskip("pyspark")

except ImportError:
    pyspark = None


@pytest.mark.parametrize(
    "underscore_prefix",
    [
        pytest.param("_", id="underscore prefix"),
        pytest.param("", id="no underscore prefix"),
    ],
)
@pytest.mark.parametrize(
    "sampling_kwargs,num_sampled_rows",
    [
        pytest.param({"n": 3}, 3, id="sample n=3"),
        pytest.param({"n": 1000}, 120, id="sample n=1000 larger than df"),
        pytest.param({"n": 0}, 0, id="sample n=0"),
        pytest.param(
            {},
            0,
            id="n missing from sampling_kwargs",
            marks=pytest.mark.xfail(strict=True, raises=ge_exceptions.SamplerError),
        ),
        pytest.param(
            None,
            0,
            id="sampling_kwargs are None",
            marks=pytest.mark.xfail(strict=True, raises=ge_exceptions.SamplerError),
        ),
    ],
)
def test_limit_sampler_get_batch_data(
    sampling_kwargs,
    num_sampled_rows,
    underscore_prefix,
    test_sparkdf,
    basic_spark_df_execution_engine,
):
    sampled_df = basic_spark_df_execution_engine.get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf,
            sampling_method=f"{underscore_prefix}sample_using_limit",
            sampling_kwargs=sampling_kwargs,
        )
    ).dataframe

    assert sampled_df.count() == num_sampled_rows


def test_get_batch_empty_sampler(test_sparkdf, basic_spark_df_execution_engine):
    sampled_df = basic_spark_df_execution_engine.get_batch_data(
        RuntimeDataBatchSpec(batch_data=test_sparkdf, sampling_method=None)
    ).dataframe
    assert sampled_df.count() == 120
    assert len(sampled_df.columns) == 10


def test_sample_using_random(test_sparkdf, basic_spark_df_execution_engine):
    sampled_df = basic_spark_df_execution_engine.get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf, sampling_method="_sample_using_random"
        )
    ).dataframe
    # The test dataframe contains 10 columns and 120 rows.
    assert len(sampled_df.columns) == 10
    assert 0 <= sampled_df.count() <= 120
    # The sampling probability "p" used in "SparkDFExecutionEngine._sample_using_random()" is 0.1 (the equivalent of an
    # unfair coin with the 10% chance of coming up as "heads").  Hence, we should never get as much as 20% of the rows.
    assert sampled_df.count() < 25


def test_sample_using_mod(test_sparkdf, basic_spark_df_execution_engine):
    sampled_df = basic_spark_df_execution_engine.get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf,
            sampling_method="_sample_using_mod",
            sampling_kwargs={
                "column_name": "id",
                "mod": 5,
                "value": 4,
            },
        )
    ).dataframe
    assert sampled_df.count() == 24
    assert len(sampled_df.columns) == 10


def test_sample_using_a_list(test_sparkdf, basic_spark_df_execution_engine):
    sampled_df = basic_spark_df_execution_engine.get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf,
            sampling_method="_sample_using_a_list",
            sampling_kwargs={
                "column_name": "id",
                "value_list": [3, 5, 7, 11],
            },
        )
    ).dataframe
    assert sampled_df.count() == 4
    assert len(sampled_df.columns) == 10


def test_sample_using_md5_wrong_hash_function_name(
    test_sparkdf, basic_spark_df_execution_engine
):
    with pytest.raises(ge_exceptions.ExecutionEngineError):
        # noinspection PyUnusedLocal
        sampled_df = basic_spark_df_execution_engine.get_batch_data(
            RuntimeDataBatchSpec(
                batch_data=test_sparkdf,
                sampling_method="_sample_using_hash",
                sampling_kwargs={
                    "column_name": "date",
                    "hash_function_name": "I_wont_work",
                },
            )
        ).dataframe


def test_sample_using_md5(test_sparkdf, basic_spark_df_execution_engine):
    sampled_df = basic_spark_df_execution_engine.get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf,
            sampling_method="_sample_using_hash",
            sampling_kwargs={
                "column_name": "date",
                "hash_function_name": "md5",
            },
        )
    ).dataframe
    assert sampled_df.count() == 10
    assert len(sampled_df.columns) == 10

    collected = sampled_df.collect()
    for val in collected:
        assert val.date in [datetime.date(2020, 1, 15), datetime.date(2020, 1, 29)]
