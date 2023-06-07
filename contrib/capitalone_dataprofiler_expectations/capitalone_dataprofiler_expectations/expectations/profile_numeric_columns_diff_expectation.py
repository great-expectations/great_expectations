from typing import Dict, Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import BatchExpectation
from great_expectations.expectations.registry import get_metric_kwargs
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.validator import ValidationDependencies


class ProfileNumericColumnsDiffExpectation(BatchExpectation):
    profile_metric = None

    @classmethod
    def is_abstract(cls):
        return cls.profile_metric is None or super().is_abstract()

    def get_validation_dependencies(
        self,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> ValidationDependencies:
        dependencies: ValidationDependencies = super().get_validation_dependencies(
            configuration, execution_engine, runtime_configuration
        )
        assert isinstance(
            self.profile_metric, str
        ), "ProfileNumericColumnsDiffExpectation must override get_validation_dependencies or declare exactly one profile_metric"
        assert (
            self.metric_dependencies == tuple()
        ), "ProfileNumericColumnsDiffExpectation must be configured using profile_metric, and cannot have metric_dependencies declared."
        # convenient name for updates

        metric_kwargs = get_metric_kwargs(
            metric_name=f"{self.profile_metric}",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )

        dependencies.set_metric_configuration(
            metric_name=f"{self.profile_metric}",
            metric_configuration=MetricConfiguration(
                metric_name=f"{self.profile_metric}",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            ),
        )

        return dependencies

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        delta_between_thresholds = metrics.get(f"{self.profile_metric}")
        mostly = self.get_success_kwargs().get(
            "mostly", self.default_kwarg_values.get("mostly")
        )

        unexpected_values = {}
        total_stats = 0.0
        failed_stats = 0.0
        for column, value in delta_between_thresholds.items():
            column_unexpected_values = {}
            if not isinstance(value, dict):
                unexpected_values[column] = value
                failed_stats += 1.0
                total_stats += 1.0
                continue
            for stat, val in value.items():
                if val is not True:
                    column_unexpected_values[stat] = val
                    failed_stats += 1.0
                total_stats += 1.0
            if column_unexpected_values != {}:
                unexpected_values[column] = column_unexpected_values

        successful_stats = total_stats - failed_stats
        percent_successful = successful_stats / total_stats

        success = percent_successful >= mostly

        results = {
            "success": success,
            "expectation_config": configuration,
            "result": {
                "unexpected_values": unexpected_values,
            },
        }
        return results
