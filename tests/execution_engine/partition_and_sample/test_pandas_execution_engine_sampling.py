import datetime
import random

import pandas as pd
import pytest

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.batch_spec import RuntimeDataBatchSpec
from great_expectations.execution_engine import PandasExecutionEngine


@pytest.mark.unit
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
        pytest.param({"n": 15}, 6, id="sample n=15 larger than df"),
        pytest.param({"n": 0}, 0, id="sample n=0"),
        pytest.param(
            {},
            0,
            id="n missing from sampling_kwargs",
            marks=pytest.mark.xfail(strict=True, raises=gx_exceptions.SamplerError),
        ),
        pytest.param(
            None,
            0,
            id="sampling_kwargs are None",
            marks=pytest.mark.xfail(strict=True, raises=gx_exceptions.SamplerError),
        ),
    ],
)
def test_limit_sampler_get_batch_data(sampling_kwargs, num_sampled_rows, underscore_prefix):
    sampled_df = (
        PandasExecutionEngine()
        .get_batch_data(
            RuntimeDataBatchSpec(
                batch_data=pd.DataFrame(
                    {"a": [1, 5, 22, 3, 5, 10]},
                ),
                sampling_method=f"{underscore_prefix}sample_using_limit",
                sampling_kwargs=sampling_kwargs,
            )
        )
        .dataframe
    )

    assert len(sampled_df) == num_sampled_rows


@pytest.mark.unit
def test_sample_using_random(test_df):
    random.seed(1)
    sampled_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(batch_data=test_df, sampling_method="_sample_using_random")
    )
    assert sampled_df.dataframe.shape == (13, 10)


@pytest.mark.unit
def test_sample_using_mod(test_df):
    sampled_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            sampling_method="_sample_using_mod",
            sampling_kwargs={
                "column_name": "id",
                "mod": 5,
                "value": 4,
            },
        )
    )
    assert sampled_df.dataframe.shape == (24, 10)


@pytest.mark.unit
def test_sample_using_a_list(test_df):
    sampled_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            sampling_method="_sample_using_a_list",
            sampling_kwargs={
                "column_name": "id",
                "value_list": [3, 5, 7, 11],
            },
        )
    )
    assert sampled_df.dataframe.shape == (4, 10)


@pytest.mark.unit
def test_sample_using_md5(test_df):
    with pytest.raises(gx_exceptions.ExecutionEngineError):
        # noinspection PyUnusedLocal
        sampled_df = PandasExecutionEngine().get_batch_data(
            RuntimeDataBatchSpec(
                batch_data=test_df,
                sampling_method="_sample_using_hash",
                sampling_kwargs={
                    "column_name": "date",
                    "hash_function_name": "I_am_not_valid",
                },
            )
        )

    sampled_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            sampling_method="_sample_using_hash",
            sampling_kwargs={"column_name": "date", "hash_function_name": "md5"},
        )
    )
    assert sampled_df.dataframe.shape == (10, 10)
    assert sampled_df.dataframe.date.isin(
        [
            datetime.date(2020, 1, 15),
            datetime.date(2020, 1, 29),
        ]
    ).all()
