from typing import Union

import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.compatibility import pydantic
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.data_sources_and_expectations.data_source_lists import (
    JUST_PANDAS_DATA_SOURCES,
)
from tests.integration.test_utils.data_source_config import (
    ALL_DATA_SOURCES,
)

COL_A = "col_a"
COL_B = "col_b"
COL_C = "col_c"


DATA = pd.DataFrame(
    {
        COL_A: [1],
        COL_B: [2],
        COL_C: [3],
    }
)


@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=DATA)
def test_golden_path(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectTableColumnsToMatchOrderedList(column_list=[COL_A, COL_B, COL_C])
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectTableColumnsToMatchOrderedList(column_list=[COL_A, COL_B]),
            id="missing_cols",
        ),
        pytest.param(
            gxe.ExpectTableColumnsToMatchOrderedList(column_list=[COL_A, COL_B, COL_C, "col_d"]),
            id="extra_cols",
        ),
        pytest.param(
            gxe.ExpectTableColumnsToMatchOrderedList(column_list=[COL_A, COL_B, COL_C.upper()]),
            id="wrong_value",
        ),
        pytest.param(
            gxe.ExpectTableColumnsToMatchOrderedList(column_list=[COL_C, COL_B, COL_A]),
            id="wrong_order",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_failure(
    batch_for_datasource: Batch, expectation: gxe.ExpectTableColumnsToMatchOrderedList
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success


# `None` is not a valid `column_list`, and an empty one is meaningless, so
# both are rejected with a validation error when the expectation is created,
# and again at validation time if a suite parameter resolves to one of them.
# (Sibling of the `column_set` fix for
# https://github.com/fivetran/great_expectations/issues/12288.)
@pytest.mark.unit
@pytest.mark.parametrize("column_list", [None, [], set()])
def test_invalid_column_list(column_list: Union[list, set, None]) -> None:
    with pytest.raises(pydantic.ValidationError):
        gxe.ExpectTableColumnsToMatchOrderedList(column_list=column_list)


@pytest.mark.parametrize("suite_param_value", [None, []])
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_column_list_suite_parameter_resolving_to_invalid_value(
    batch_for_datasource: Batch, suite_param_value: Union[list, None]
) -> None:
    suite_param_key = "column_list"
    expectation = gxe.ExpectTableColumnsToMatchOrderedList(
        column_list={"$PARAMETER": suite_param_key}
    )
    with pytest.raises(pydantic.ValidationError):
        batch_for_datasource.validate(
            expectation, expectation_parameters={suite_param_key: suite_param_value}
        )


@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_column_list_suite_parameter_resolving_to_valid_value(
    batch_for_datasource: Batch,
) -> None:
    suite_param_key = "column_list"
    expectation = gxe.ExpectTableColumnsToMatchOrderedList(
        column_list={"$PARAMETER": suite_param_key}
    )
    result = batch_for_datasource.validate(
        expectation, expectation_parameters={suite_param_key: [COL_A, COL_B, COL_C]}
    )
    assert result.success
