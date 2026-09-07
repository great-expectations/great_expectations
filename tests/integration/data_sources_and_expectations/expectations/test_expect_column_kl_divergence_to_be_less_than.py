import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.core.result_format import ResultFormat
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.data_sources_and_expectations.data_source_lists import (
    JUST_PANDAS_DATA_SOURCES,
)
from tests.integration.test_utils.data_source_config import (
    ALL_DATA_SOURCES,
)

# TODO: Full coverage to replace test_expect_column_kl_divergence_to_be_less_than.py

COL_NAME = "my_col"

DATA = pd.DataFrame(
    {
        COL_NAME: ["A"] * 5 + ["B"] * 3 + ["C"] * 2,
    }
)


@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=DATA)
def test_success(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnKLDivergenceToBeLessThan(
        column=COL_NAME,
        partition_object={"weights": [0.5, 0.3, 0.2], "values": ["A", "B", "C"]},
        threshold=0.01,
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success
    assert result.to_json_dict()["result"] == {
        "observed_value": 0.0,
        "details": {
            "observed_partition": {"values": ["A", "B", "C"], "weights": [0.5, 0.3, 0.2]},
            "expected_partition": {"values": ["A", "B", "C"], "weights": [0.5, 0.3, 0.2]},
        },
    }


@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_failure(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnKLDivergenceToBeLessThan(
        column=COL_NAME,
        partition_object={
            "weights": [0.3333333333333333, 0.3333333333333333, 0.3333333333333333],
            "values": ["A", "B", "C"],
        },
        threshold=0.01,
    )
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@pytest.mark.parametrize(
    "suite_param_value,expected_result",
    [
        pytest.param({"weights": [0.5, 0.3, 0.2], "values": ["A", "B", "C"]}, True, id="success"),
        pytest.param(
            {
                "weights": [0.3333333333333333, 0.3333333333333333, 0.3333333333333333],
                "values": ["A", "B", "C"],
            },
            False,
            id="failure",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success_with_suite_param_partition_object_(
    batch_for_datasource: Batch, suite_param_value: dict, expected_result: bool
) -> None:
    suite_param_key = "expect_column_kl_divergence_to_be_less_than"
    expectation = gxe.ExpectColumnKLDivergenceToBeLessThan(
        column=COL_NAME,
        partition_object={"$PARAMETER": suite_param_key},
        threshold=0.01,
        result_format=ResultFormat.SUMMARY,
    )
    result = batch_for_datasource.validate(
        expectation, expectation_parameters={suite_param_key: suite_param_value}
    )
    assert result.success == expected_result


@pytest.mark.parametrize(
    "suite_param_value,expected_result",
    [
        pytest.param(0.01, True, id="success"),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success_with_suite_param_threshold_(
    batch_for_datasource: Batch, suite_param_value: float, expected_result: bool
) -> None:
    suite_param_key = "expect_column_kl_divergence_to_be_less_than"
    expectation = gxe.ExpectColumnKLDivergenceToBeLessThan(
        column=COL_NAME,
        partition_object={"weights": [0.5, 0.3, 0.2], "values": ["A", "B", "C"]},
        threshold={"$PARAMETER": suite_param_key},
        result_format=ResultFormat.SUMMARY,
    )
    result = batch_for_datasource.validate(
        expectation, expectation_parameters={suite_param_key: suite_param_value}
    )
    assert result.success == expected_result


@pytest.mark.parametrize(
    "suite_param_value,expected_result",
    [
        pytest.param(0, True, id="success"),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success_with_suite_param_internal_weight_holdout_(
    batch_for_datasource: Batch, suite_param_value: float, expected_result: bool
) -> None:
    suite_param_key = "expect_column_kl_divergence_to_be_less_than"
    expectation = gxe.ExpectColumnKLDivergenceToBeLessThan(
        column=COL_NAME,
        partition_object={"weights": [0.5, 0.3, 0.2], "values": ["A", "B", "C"]},
        threshold=0.01,
        internal_weight_holdout={"$PARAMETER": suite_param_key},
        result_format=ResultFormat.SUMMARY,
    )
    result = batch_for_datasource.validate(
        expectation, expectation_parameters={suite_param_key: suite_param_value}
    )
    assert result.success == expected_result


@pytest.mark.parametrize(
    "suite_param_value,expected_result",
    [
        pytest.param(0.1, True, id="success"),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success_with_suite_param_tail_weight_holdout_(
    batch_for_datasource: Batch, suite_param_value: float, expected_result: bool
) -> None:
    suite_param_key = "expect_column_kl_divergence_to_be_less_than"
    expectation = gxe.ExpectColumnKLDivergenceToBeLessThan(
        column=COL_NAME,
        partition_object={"weights": [0.5, 0.3, 0.2], "values": ["A", "B", "C"]},
        threshold=0.01,
        tail_weight_holdout={"$PARAMETER": suite_param_key},
        result_format=ResultFormat.SUMMARY,
    )
    result = batch_for_datasource.validate(
        expectation, expectation_parameters={suite_param_key: suite_param_value}
    )
    assert result.success == expected_result


@pytest.mark.parametrize(
    "suite_param_value,expected_result",
    [
        pytest.param(True, True, id="success"),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success_with_suite_param_bucketize_data_(
    batch_for_datasource: Batch, suite_param_value: bool, expected_result: bool
) -> None:
    suite_param_key = "expect_column_kl_divergence_to_be_less_than"
    expectation = gxe.ExpectColumnKLDivergenceToBeLessThan(
        column=COL_NAME,
        partition_object={"weights": [0.5, 0.3, 0.2], "values": ["A", "B", "C"]},
        threshold=0.01,
        bucketize_data={"$PARAMETER": suite_param_key},
        result_format=ResultFormat.SUMMARY,
    )
    result = batch_for_datasource.validate(
        expectation, expectation_parameters={suite_param_key: suite_param_value}
    )
    assert result.success == expected_result
