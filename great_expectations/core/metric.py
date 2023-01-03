import warnings

from dateutil.parser import parse

from great_expectations.core.data_context_key import DataContextKey
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
)
from great_expectations.exceptions import GreatExpectationsError


class MetricIdentifier(DataContextKey):
    """A MetricIdentifier serves as a key to store and retrieve Metrics."""

    def __init__(self, metric_name, metric_kwargs_id) -> None:
        self._metric_name = metric_name
        self._metric_kwargs_id = metric_kwargs_id

    @property
    def metric_name(self):
        return self._metric_name

    @property
    def metric_kwargs_id(self):
        return self._metric_kwargs_id

    def to_fixed_length_tuple(self):
        return self.to_tuple()

    def to_tuple(self):
        if self._metric_kwargs_id is None:
            tuple_metric_kwargs_id = "__"
        else:
            tuple_metric_kwargs_id = self._metric_kwargs_id
        return tuple(
            (self.metric_name, tuple_metric_kwargs_id)
        )  # We use the placeholder in to_tuple

    @classmethod
    def from_fixed_length_tuple(cls, tuple_):
        return cls.from_tuple(tuple_)

    @classmethod
    def from_tuple(cls, tuple_):
        if tuple_[-1] == "__":
            return cls(*tuple_[:-1], None)
        return cls(*tuple_)


class ValidationMetricIdentifier(MetricIdentifier):
    def __init__(
        self,
        run_id,
        data_asset_name,
        expectation_suite_identifier,
        metric_name,
        metric_kwargs_id,
    ) -> None:
        super().__init__(metric_name, metric_kwargs_id)
        if not isinstance(expectation_suite_identifier, ExpectationSuiteIdentifier):
            expectation_suite_identifier = ExpectationSuiteIdentifier(
                expectation_suite_name=expectation_suite_identifier
            )

        if isinstance(run_id, str):
            # deprecated-v0.11.0
            warnings.warn(
                "String run_ids are deprecated as of v0.11.0 and support will be removed in v0.16. Please provide a run_id of type "
                "RunIdentifier(run_name=None, run_time=None), or a dictionary containing run_name "
                "and run_time (both optional).",
                DeprecationWarning,
            )
            try:
                run_time = parse(run_id)
            except (ValueError, TypeError):
                run_time = None
            run_id = RunIdentifier(run_name=run_id, run_time=run_time)
        elif isinstance(run_id, dict):
            run_id = RunIdentifier(**run_id)
        elif run_id is None:
            run_id = RunIdentifier()
        elif not isinstance(run_id, RunIdentifier):
            run_id = RunIdentifier(run_name=str(run_id))

        self._run_id = run_id
        self._data_asset_name = data_asset_name
        self._expectation_suite_identifier = expectation_suite_identifier

    @property
    def run_id(self):
        return self._run_id

    @property
    def data_asset_name(self):
        return self._data_asset_name

    @property
    def expectation_suite_identifier(self):
        return self._expectation_suite_identifier

    def to_tuple(self):
        if self.data_asset_name is None:
            tuple_data_asset_name = "__"
        else:
            tuple_data_asset_name = self.data_asset_name
        return tuple(
            list(self.run_id.to_tuple())
            + [tuple_data_asset_name]
            + list(self.expectation_suite_identifier.to_tuple())
            + [self.metric_name, self.metric_kwargs_id or "__"]
        )

    def to_fixed_length_tuple(self):
        if self.data_asset_name is None:
            tuple_data_asset_name = "__"
        else:
            tuple_data_asset_name = self.data_asset_name
        return tuple(
            list(self.run_id.to_tuple())
            + [tuple_data_asset_name]
            + list(self.expectation_suite_identifier.to_fixed_length_tuple())
            + [self.metric_name, self.metric_kwargs_id or "__"]
        )

    def to_evaluation_parameter_urn(self):
        if self._metric_kwargs_id is None:
            return "urn:great_expectations:validations:" + ":".join(
                list(self.expectation_suite_identifier.to_fixed_length_tuple())
                + [self.metric_name]
            )
        else:
            return "urn:great_expectations:validations:" + ":".join(
                list(self.expectation_suite_identifier.to_fixed_length_tuple())
                + [self.metric_name, self._metric_kwargs_id]
            )

    @classmethod
    def from_tuple(cls, tuple_):
        if len(tuple_) < 6:
            raise GreatExpectationsError(
                "ValidationMetricIdentifier tuple must have at least six components."
            )
        if tuple_[2] == "__":
            tuple_data_asset_name = None
        else:
            tuple_data_asset_name = tuple_[2]
        metric_id = MetricIdentifier.from_tuple(tuple_[-2:])
        return cls(
            run_id=RunIdentifier.from_tuple((tuple_[0], tuple_[1])),
            data_asset_name=tuple_data_asset_name,
            expectation_suite_identifier=ExpectationSuiteIdentifier.from_tuple(
                tuple_[3:-2]
            ),
            metric_name=metric_id.metric_name,
            metric_kwargs_id=metric_id.metric_kwargs_id,
        )

    @classmethod
    def from_fixed_length_tuple(cls, tuple_):
        if len(tuple_) != 6:
            raise GreatExpectationsError(
                "ValidationMetricIdentifier fixed length tuple must have exactly six "
                "components."
            )
        if tuple_[2] == "__":
            tuple_data_asset_name = None
        else:
            tuple_data_asset_name = tuple_[2]
        metric_id = MetricIdentifier.from_tuple(tuple_[-2:])
        return cls(
            run_id=RunIdentifier.from_fixed_length_tuple((tuple_[0], tuple_[1])),
            data_asset_name=tuple_data_asset_name,
            expectation_suite_identifier=ExpectationSuiteIdentifier.from_fixed_length_tuple(
                tuple((tuple_[3],))
            ),
            metric_name=metric_id.metric_name,
            metric_kwargs_id=metric_id.metric_kwargs_id,
        )
