from __future__ import annotations

from enum import Enum
import datetime


class TimeInterval(str, Enum):
    YEAR = "year"
    MONTH = "month"
    DAY = "day"
    HOUR = "hour"
    MINUTE = "minute"
    SECOND = "second"


def datetimes(
    start: datetime.datetime, end: datetime.datetime, interval: TimeInterval
) -> list[datetime.datetime]:
    """Yields datetimes from start to end inclusive, separated by interval.

    Note, start is truncated so it falls onto the nearest interval boundary. For example
    if start=2/7/2023 and we are incrementing by month the first returned valued will be
    2/0/2023. This is because we are only concerned with the month resolution so all days
    are equivalent. Our canonical representation sets the equivalent datetime fields to
    their minimum value (0 or 1).

    Args:
        start: The start time to iterate over
        end: The end time to iterate over. The end will be included in the output, truncated
             to the desired resolution.

    Returns:
        A list of datetimes starting at start, ending at end, seperated by interval.
    """
    bins: list[datetime.datetime] = []
    current_dt = _truncate_datetime(start, interval)
    while current_dt <= end:
        yield current_dt
        bins.append(current_dt)
        current_dt = _increment_by_interval(current_dt, interval)


def _truncate_datetime(dt: datetime, interval: TimeInterval) -> datetime.datetime:
    if interval == TimeInterval.YEAR:
        return datetime.datetime(year=dt.year, month=1, day=1)
    elif interval == TimeInterval.MONTH:
        return datetime.datetime(year=dt.year, month=dt.month, day=1)
    elif interval == TimeInterval.DAY:
        return datetime.datetime(year=dt.year, month=dt.month, day=dt.day)
    elif interval == TimeInterval.HOUR:
        return datetime.datetime(year=dt.year, month=dt.month, day=dt.day, hour=dt.hour)
    elif interval == TimeInterval.MINUTE:
        return datetime.datetime(
            year=dt.year, month=dt.month, day=dt.day, hour=dt.hour, minute=dt.minute
        )
    elif interval == TimeInterval.SECOND:
        return datetime.datetime(
            year=dt.year,
            month=dt.month,
            day=dt.day,
            hour=dt.hour,
            minute=dt.minute,
            second=dt.second,
        )
    else:
        raise ValueError(
            f"{interval} is not a valid TimeInterval for truncate_datetime"
        )


def _increment_by_interval(dt: datetime, interval: TimeInterval) -> datetime.datetime:
    if interval == TimeInterval.MONTH:
        return _increment_month(dt)
    # We could cache the delta associated with a key.
    delta_key = interval + "s"
    delta = datetime.timedelta(**{delta_key: 1})
    return dt + delta


def _increment_month(dt: datetime) -> datetime:
    if dt.month == 12:
        return dt.replace(year=dt.year + 1, month=1)
    return dt.replace(month=dt.month + 1)
