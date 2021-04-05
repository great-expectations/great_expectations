import logging
from decimal import Decimal

from great_expectations.core.util import (
    convert_to_json_serializable,
    requires_lossy_conversion,
)


def test_lossy_serialization_warning(caplog):
    caplog.set_level(logging.WARNING, logger="great_expectations.core")

    d = Decimal("12345.678901234567890123456789")

    convert_to_json_serializable(d)
    assert len(caplog.messages) == 1
    assert caplog.messages[0].startswith(
        "Using lossy conversion for decimal 12345.678901234567890123456789"
    )

    caplog.clear()
    d = Decimal("0.1")
    convert_to_json_serializable(d)
    print(caplog.messages)
    assert len(caplog.messages) == 0


def test_lossy_conversion():
    d = Decimal("12345.678901234567890123456789")
    assert requires_lossy_conversion(d)

    d = Decimal("12345.67890123456")
    assert requires_lossy_conversion(d)

    d = Decimal("12345.6789012345")
    assert not requires_lossy_conversion(d)

    d = Decimal("0.12345678901234567890123456789")
    assert requires_lossy_conversion(d)

    d = Decimal("0.1234567890123456")
    assert requires_lossy_conversion(d)

    d = Decimal("0.123456789012345")
    assert not requires_lossy_conversion(d)

    d = Decimal("0.1")
    assert not requires_lossy_conversion(d)


# TODO add unittests for convert_to_json_serializable() and ensure_json_serializable()
def test_serialization_of_spark_df(spark_session):
    import pandas as pd

    df = pd.DataFrame({"a": [1, 2, 3]})
    sdf = spark_session.createDataFrame(df)
    assert convert_to_json_serializable(sdf) == {"a": [1, 2, 3]}

    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    sdf = spark_session.createDataFrame(df)
    assert convert_to_json_serializable(sdf) == {"a": [1, 2, 3], "b": [4, 5, 6]}
