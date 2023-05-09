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


@pytest.mark.parametrize(
    "fields_or_struct_type,serialized_output",
    [pytest.param([], {"fields": [], "type": "struct"}, id="empty_list")],
)
def test_serializable_struct_type(
    fields_or_struct_type: pyspark_types.StructType
    | list[pyspark_types.StructField]
    | None,
    serialized_output: dict,
    skip_if_spark_not_selected: None,
):
    sst = SerializableStructType(fields_or_struct_type=fields_or_struct_type)
    assert isinstance(sst.struct_type, pyspark_types.StructType)
    assert isinstance(sst, dict)
    assert sst == serialized_output


def test_serializable_struct_type_from_empty_list(skip_if_spark_not_selected):
    sst = SerializableStructType([])
    assert isinstance(sst.struct_type, pyspark_types.StructType)
    assert isinstance(sst, dict)
    assert sst == {"fields": [], "type": "struct"}


def test_serializable_struct_type_from_fields(skip_if_spark_not_selected):
    sst = SerializableStructType(
        [pyspark_types.StructField("f1", pyspark_types.StringType(), True)]
    )
    assert isinstance(sst.struct_type, pyspark_types.StructType)
    assert isinstance(sst, dict)
    assert sst == {
        "fields": [{"metadata": {}, "name": "f1", "nullable": True, "type": "string"}],
        "type": "struct",
    }


def test_serializable_struct_type_from_struct_type(skip_if_spark_not_selected):
    struct_type = pyspark_types.StructType(
        [pyspark_types.StructField("f1", pyspark_types.StringType(), True)]
    )

    sst = SerializableStructType(struct_type)
    assert isinstance(sst.struct_type, pyspark_types.StructType)
    assert isinstance(sst, dict)
    assert sst == {
        "fields": [{"metadata": {}, "name": "f1", "nullable": True, "type": "string"}],
        "type": "struct",
    }


@pytest.mark.parametrize(
    "fields_or_struct_type,serialized_output",
    [pytest.param([], {"fields": [], "type": "struct"}, id="empty_list")],
)
def test_serializable_struct_type_validate(
    fields_or_struct_type: pyspark_types.StructType
    | list[pyspark_types.StructField]
    | None,
    serialized_output: dict,
    skip_if_spark_not_selected: None,
):
    pass
