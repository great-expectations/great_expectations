import os

from ruamel.yaml import YAML

from great_expectations import DataContext
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import BatchRequest
from great_expectations.rule_based_profiler.config.base import (
    RuleBasedProfilerConfig,
    ruleBasedProfilerConfigSchema,
)
from great_expectations.rule_based_profiler.rule_based_profiler import RuleBasedProfiler
from great_expectations.validator.metric_configuration import MetricConfiguration

# TODO: <Alex>ALEX</Alex>
# yaml = YAML()


def test_reconcile_profiler_variables(
    empty_data_context: DataContext,
    profiler_config_with_placeholder_args: RuleBasedProfilerConfig,
):
    profiler_config_dict: dict = profiler_config_with_placeholder_args.to_json_dict()
    profiler_config_dict.pop("class_name")
    profiler_config_dict.pop("module_name")
    profiler: RuleBasedProfiler = RuleBasedProfiler(
        **profiler_config_dict, data_context=empty_data_context
    )
    # effective_variables: Optional[
    #     ParameterContainer
    # ] = self.reconcile_profiler_variables(variables=variables)
    #
    # # TODO: <Alex>ALEX -- Tests for Reconciliation are next immediate action items.</Alex>
    # # TODO: <Alex>ALEX -- Replace "getattr/setattr" with "__dict__" (in a "to_dict()" method on Rule and below).</Alex>
    # effective_rules: List[Rule] = self.reconcile_profiler_rules(rules=rules)
    #
    # if expectation_suite_name is None:
    #     expectation_suite_name = (
    #         f"tmp.profiler_{self.__class__.__name__}_suite_{str(uuid.uuid4())[:8]}"
    #     )
    #
    # expectation_suite: ExpectationSuite = ExpectationSuite(
    #     expectation_suite_name=expectation_suite_name,
    #     data_context=self._data_context,
    # )
