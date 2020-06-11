import logging
from decimal import Decimal

from great_expectations.core import convert_to_json_serializable


def test_lossy_serialization_warning(caplog):
    caplog.set_level(logging.WARNING, logger="great_expectations.core")

    d = (
        Decimal(7091.17117297555159893818) ** Decimal(2)
        + Decimal(7118.70008070942458289210) ** Decimal(2)
        + (Decimal(-1513.67274389594149397453)) ** Decimal(2)
    ) ** Decimal(1.5)
    f_1 = (
        7091.17117297555159893818 ** 2
        + 7118.70008070942458289210 ** 2
        + (-1513.67274389594149397453) ** 2
    ) ** 1.5
    f_2 = float(d)
    assert not (-1e-55 < Decimal.from_float(f_1) - d < 1e-55)
    assert not (-1e-55 < Decimal.from_float(f_2) - d < 1e-55)

    convert_to_json_serializable(d)
    assert len(caplog.messages) == 1
    assert caplog.messages[0].startswith("Using lossy conversion for decimal")

    caplog.clear()
    d = Decimal(0.1)
    f_1 = 0.1
    f_2 = float(d)

    assert -1e-55 < Decimal.from_float(f_1) - d < 1e-55
    assert -1e-55 < Decimal.from_float(f_2) - d < 1e-55
    convert_to_json_serializable(d)
    assert len(caplog.messages) == 0
