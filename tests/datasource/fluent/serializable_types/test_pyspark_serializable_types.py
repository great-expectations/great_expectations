from __future__ import annotations

import pytest

from great_expectations.compatibility.pyspark import types as pyspark_types
from great_expectations.datasource.fluent.serializable_types.pyspark import (
    SerializableStructType,
)


@pytest.fixture
def skip_if_spark_not_selected(test_backends) -> None:
    # NOTE: This should be edited when we organize tests to use marks instead of skips.
    if "SparkDFDataset" not in test_backends:
        pytest.skip("No spark backend selected.")


if pyspark_types:
    struct_type_test_params = [
        pytest.param(None, {"fields": [], "type": "struct"}, id="None"),
        pytest.param([], {"fields": [], "type": "struct"}, id="empty_list"),
        pytest.param(
            [pyspark_types.StructField("f1", pyspark_types.StringType(), True)],
            {
                "fields": [{"metadata": {}, "name": "f1", "nullable": True, "type": "string"}],
                "type": "struct",
            },
            id="fields_list",
        ),
        pytest.param(
            pyspark_types.StructType(
                [pyspark_types.StructField("f1", pyspark_types.StringType(), True)]
            ),
            {
                "fields": [{"metadata": {}, "name": "f1", "nullable": True, "type": "string"}],
                "type": "struct",
            },
            id="struct_type",
        ),
    ]
else:
    struct_type_test_params = []


@pytest.mark.unit
@pytest.mark.parametrize("fields_or_struct_type,serialized_output", struct_type_test_params)
def test_serializable_struct_type(
    fields_or_struct_type: pyspark_types.StructType | list[pyspark_types.StructField] | None,
    serialized_output: dict,
    skip_if_spark_not_selected: None,
):
    sst = SerializableStructType(fields_or_struct_type=fields_or_struct_type)
    assert isinstance(sst.struct_type, pyspark_types.StructType)
    assert isinstance(sst, dict)
    assert sst == serialized_output


@pytest.mark.unit
@pytest.mark.parametrize(
    "fields_or_struct_type,serialized_output",
    struct_type_test_params,
)
def test_serializable_struct_type_validate(
    fields_or_struct_type: pyspark_types.StructType | list[pyspark_types.StructField] | None,
    serialized_output: dict,
    skip_if_spark_not_selected: None,
):
    sst = SerializableStructType.validate(fields_or_struct_type)
    assert isinstance(sst.struct_type, pyspark_types.StructType)
    assert isinstance(sst, dict)
    assert sst == serialized_output
