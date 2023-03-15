from __future__ import annotations

import datetime
import json
import warnings
from copy import deepcopy
from typing import Dict, Optional, Union

from dateutil.parser import parse
from marshmallow import Schema, fields, post_load, pre_dump

from great_expectations.alias_types import JSONValues  # noqa: TCH001
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.data_context_key import DataContextKey


@public_api
class RunIdentifier(DataContextKey):
    """A RunIdentifier identifies a run (collection of validations) by run_name and run_time.

    Args:
        run_name: a string or None.
        run_time: a Datetime.datetime instance, a string, or None.
    """

    def __init__(
        self,
        run_name: Optional[str] = None,
        run_time: Optional[Union[datetime.datetime, str]] = None,
    ) -> None:
        super().__init__()
        assert run_name is None or isinstance(
            run_name, str
        ), "run_name must be an instance of str"
        assert run_time is None or isinstance(run_time, (datetime.datetime, str)), (
            "run_time must be either None or " "an instance of str or datetime"
        )
        self._run_name = run_name

        if isinstance(run_time, str):
            try:
                run_time = parse(run_time)
            except (ValueError, TypeError):
                warnings.warn(
                    f'Unable to parse provided run_time str ("{run_time}") to datetime. Defaulting '
                    f"run_time to current time."
                )
                run_time = datetime.datetime.now(datetime.timezone.utc)

        if not run_time:
            try:
                run_time = parse(run_name)  # type: ignore[arg-type]
            except (ValueError, TypeError):
                run_time = None

        run_time = run_time or datetime.datetime.now(tz=datetime.timezone.utc)
        if not run_time.tzinfo:
            # This will change the timzeone to UTC, and convert the time based
            # on assuming that the current time is in local.
            run_time = run_time.astimezone(tz=datetime.timezone.utc)

        self._run_time = run_time

    @property
    def run_name(self):
        return self._run_name

    @property
    def run_time(self):
        return self._run_time

    def to_tuple(self):
        return (
            self._run_name or "__none__",
            self._run_time.astimezone(tz=datetime.timezone.utc).strftime(
                "%Y%m%dT%H%M%S.%fZ"
            ),
        )

    def to_fixed_length_tuple(self):
        return (
            self._run_name or "__none__",
            self._run_time.astimezone(tz=datetime.timezone.utc).strftime(
                "%Y%m%dT%H%M%S.%fZ"
            ),
        )

    def __repr__(self):
        return json.dumps(self.to_json_dict())

    def __str__(self):
        return json.dumps(self.to_json_dict(), indent=2)

    @public_api
    def to_json_dict(self) -> Dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this RunIdentifier.

        Returns:
            A JSON-serializable dict representation of this RunIdentifier.
        """
        myself = runIdentifierSchema.dump(self)
        return myself

    def set_run_time_tz(self, tz: datetime.timezone | None):
        """Localize the run_time to the given timezone, or default to system local tz.

        Args:
            tz: The timezone to localize to.
        """
        self._run_time = self._run_time.astimezone(tz=tz)

    @classmethod
    def from_tuple(cls, tuple_):
        return cls(tuple_[0], tuple_[1])

    @classmethod
    def from_fixed_length_tuple(cls, tuple_):
        return cls(tuple_[0], tuple_[1])


class RunIdentifierSchema(Schema):
    run_name = fields.Str()
    run_time = fields.AwareDateTime(
        format="iso", default_timezone=datetime.timezone.utc
    )

    @pre_dump
    def prepare_dump(self, data, **kwargs):
        data = deepcopy(data)
        data.set_run_time_tz(tz=None)  # sets to system local tz
        return data

    @post_load
    def make_run_identifier(self, data, **kwargs):
        return RunIdentifier(**data)


runIdentifierSchema = RunIdentifierSchema()
