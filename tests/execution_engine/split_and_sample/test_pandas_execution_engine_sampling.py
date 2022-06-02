import pandas as pd
import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch_spec import RuntimeDataBatchSpec
from great_expectations.execution_engine import PandasExecutionEngine


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
    sampling_kwargs, num_sampled_rows, underscore_prefix
):

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
