"""What a fixture frame declares is what the backend must store.

Every assertion in this directory rests on an unstated assumption: that the values a test
declares in a pandas frame are the values the backend holds once the harness has written them.
Where that fails it fails silently -- the DDL is valid, no error is raised, and an assertion
about a value quietly becomes an assertion about a different value.

The shapes below are the ones that have actually broken. A decimal type carrying no scale is read
by several servers as scale zero, which rounds every fractional value to an integer on write. A
float type carrying no width is read by some servers as their 4-byte type, which stores a Python
float -- a double -- at half the width it was declared at. A datetime type that a server does not
have makes the table impossible to create at all.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pandas as pd

import great_expectations.expectations as gxe
from great_expectations.core.result_format import ResultFormat
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.data_sources_and_expectations.data_source_lists import (
    SPARK_DATA_SOURCES,
)
from tests.integration.test_utils.data_source_config import (
    ALL_DATA_SOURCES,
    CURATED_SQL_DATA_SOURCES,
    SQL_DATA_SOURCES,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.interfaces import Batch

# Every data source the harness declares, not just the canonical-expectations tier. The curated
# tier's four backends are where the shared type map is overridden most, and reaching them found
# SingleStore storing a declared value narrowed -- a defect no lane would otherwise have run into.
#
# Read from the tier lists directly rather than through `data_sources_for_tier_case`, which
# `test_curated_backend_suite.py` requires of its own cases so that a member can sit one out. That
# mechanism is for a case a backend legitimately cannot pass, and no such case exists here: a
# backend that does not store what a fixture declared has a defect to fix, not a case to opt out
# of. Keyed by label on the way in, so a data source declaring both tiers is parameterized once.
EVERY_DATA_SOURCE = list(
    {config.label: config for config in [*ALL_DATA_SOURCES, *CURATED_SQL_DATA_SOURCES]}.values()
)
EVERY_SQL_DATA_SOURCE = list(
    {config.label: config for config in [*SQL_DATA_SOURCES, *CURATED_SQL_DATA_SOURCES]}.values()
)

FRACTIONAL_COL = "fractional_value"
MOMENT_COL = "moment"

# The largest and smallest values below each defeat a different silent substitution, and the
# assertions pin exactly those two.
#
# `16777217.0` is the smallest positive integer a 4-byte float cannot represent: at 2**24 the
# spacing between representable values is 2, so it sits midway between 16777216 and 16777218 and
# resolves to a neighbour. A double holds it exactly. Nothing smaller in this column would do --
# 1.5, 2.25 and -3.75 are all exactly representable at single precision, so a narrowed column
# would round-trip them and agree.
#
# `-3.75` differs from its nearest integer, so a column a server read as scale zero returns -4.
LARGEST_VALUE = 16777217.0
SMALLEST_VALUE = -3.75

# The last moment declared, which the datetime assertion pins the stored maximum against.
LATEST_MOMENT = datetime(2024, 1, 4, 12, 34, 56)  # noqa: DTZ001

ROUND_TRIP_DATA = pd.DataFrame(
    {
        FRACTIONAL_COL: [1.5, 2.25, SMALLEST_VALUE, LARGEST_VALUE],
        # Plain Python datetimes in an object column, deliberately, rather than a pandas
        # datetime64 column. Two backends cannot take a pandas timestamp as a bound parameter
        # at all, and one infers an empty struct for it rather than a time, so a frame built the
        # convenient way would fail on them for reasons that have nothing to do with the column
        # type under test here. Naive rather than tz-aware for the same reason: whether a
        # timezone survives the trip is a separate question with its own failure mode.
        MOMENT_COL: pd.Series(
            [
                datetime(2024, 1, 1, 12, 0, 0),  # noqa: DTZ001
                datetime(2024, 1, 2, 12, 0, 0),  # noqa: DTZ001
                datetime(2024, 1, 3, 12, 0, 0),  # noqa: DTZ001
                LATEST_MOMENT,
            ],
            dtype=object,
        ),
    }
)


@parameterize_batch_for_data_sources(data_source_configs=EVERY_DATA_SOURCE, data=ROUND_TRIP_DATA)
def test_the_largest_fractional_value_is_stored_as_declared(batch_for_datasource: Batch) -> None:
    """A column narrowed to single precision returns a neighbour of the declared maximum."""
    result = batch_for_datasource.validate(
        gxe.ExpectColumnMaxToBeBetween(
            column=FRACTIONAL_COL, min_value=LARGEST_VALUE, max_value=LARGEST_VALUE
        ),
        result_format=ResultFormat.COMPLETE,
    )

    observed = result.result.get("observed_value")
    assert result.success, (
        f"the maximum of {FRACTIONAL_COL} came back as {observed!r} rather than {LARGEST_VALUE}, "
        "so this backend is storing the column at a narrower width than a Python float"
    )


@parameterize_batch_for_data_sources(data_source_configs=EVERY_DATA_SOURCE, data=ROUND_TRIP_DATA)
def test_the_smallest_fractional_value_is_stored_as_declared(batch_for_datasource: Batch) -> None:
    """A column stored at scale zero rounds the declared minimum to an integer."""
    result = batch_for_datasource.validate(
        gxe.ExpectColumnMinToBeBetween(
            column=FRACTIONAL_COL, min_value=SMALLEST_VALUE, max_value=SMALLEST_VALUE
        ),
        result_format=ResultFormat.COMPLETE,
    )

    observed = result.result.get("observed_value")
    assert result.success, (
        f"the minimum of {FRACTIONAL_COL} came back as {observed!r} rather than "
        f"{SMALLEST_VALUE}, so this backend rounded the column's fractional part away"
    )


@parameterize_batch_for_data_sources(data_source_configs=EVERY_DATA_SOURCE, data=ROUND_TRIP_DATA)
def test_a_datetime_column_can_be_created_and_read(batch_for_datasource: Batch) -> None:
    """Reaching this assertion at all is most of the point: a backend whose type vocabulary
    does not include the declared datetime type fails during table creation, before any
    expectation runs.
    """
    result = batch_for_datasource.validate(
        gxe.ExpectColumnValuesToNotBeNull(column=MOMENT_COL),
        result_format=ResultFormat.COMPLETE,
    )

    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=EVERY_SQL_DATA_SOURCE, data=ROUND_TRIP_DATA
)
def test_a_datetime_value_is_stored_as_declared(batch_for_datasource: Batch) -> None:
    """The declared maximum is a specific moment, not just a non-null one.

    Creating the table proves the type exists; it does not prove the column holds what was put
    in it. A type that resolves to a date rather than a datetime accepts the write and drops the
    time of day, which is not null and so passes every assertion above.

    The SQL data sources rather than every one, because the column type map this pins is a SQL
    declaration: a CSV-backed source reads the column back as text no matter what any type map
    says, and comparing text to a datetime would fail here for a reason that has nothing to do
    with what this file is about.

    Pinned to the second, which is as fine as every backend here can hold in common: two of them
    resolve `datetime` to a type with no sub-second component (T-SQL's `DATETIME` keeps about
    3ms; MySQL's keeps none unless a fractional-seconds precision is declared on it), so a
    declared microsecond would be rounded away on those two by design. Making the harness carry
    sub-second values everywhere is a change to what those two backends declare, not to this
    test, and belongs with that change.
    """
    result = batch_for_datasource.validate(
        gxe.ExpectColumnMaxToBeBetween(
            column=MOMENT_COL, min_value=LATEST_MOMENT, max_value=LATEST_MOMENT
        ),
        result_format=ResultFormat.COMPLETE,
    )

    observed = result.result.get("observed_value")
    assert result.success, (
        f"the maximum of {MOMENT_COL} came back as {observed!r} rather than {LATEST_MOMENT}, so "
        "this backend is not holding the moment the fixture declared"
    )


# A pandas-native datetime column, which is what a frame built the convenient way holds. The
# column above deliberately avoids this shape so that its own assertions are about the column
# type the backend was given; this one exists to pin that the convenient shape works too.
PANDAS_TIMESTAMP_DATA = pd.DataFrame(
    {MOMENT_COL: pd.to_datetime(["2024-01-01 12:00:00", "2024-01-02 12:00:00"])}
)


@parameterize_batch_for_data_sources(
    data_source_configs=SPARK_DATA_SOURCES, data=PANDAS_TIMESTAMP_DATA
)
def test_a_pandas_timestamp_column_is_written_as_a_time(batch_for_datasource: Batch) -> None:
    """Spark infers a column's type from the values when a test declares none, and matches on
    exact type. A pandas timestamp is a subclass rather than the type it looks for, so it reads
    as an empty struct -- which no file format can write, and which fails during setup rather
    than as an assertion. Reaching this assertion at all is the substance of the test.
    """
    result = batch_for_datasource.validate(
        gxe.ExpectColumnValuesToNotBeNull(column=MOMENT_COL),
        result_format=ResultFormat.COMPLETE,
    )

    assert result.success
