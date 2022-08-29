import logging
from dataclasses import asdict, dataclass
from enum import Enum

from great_expectations.core.util import convert_to_json_serializable, nested_update
from great_expectations.rule_based_profiler.helpers.util import (
    convert_variables_to_dict,
)
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterContainer,
)
from great_expectations.types import SerializableDictDot

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ReconciliationStrategy(Enum):
    NESTED_UPDATE = "nested_update"
    REPLACE = "replace"
    UPDATE = "update"


@dataclass
class ReconciliationDirectives(SerializableDictDot):
    variables: ReconciliationStrategy = ReconciliationStrategy.UPDATE
    domain_builder: ReconciliationStrategy = ReconciliationStrategy.UPDATE
    parameter_builder: ReconciliationStrategy = ReconciliationStrategy.UPDATE
    expectation_configuration_builder: ReconciliationStrategy = (
        ReconciliationStrategy.UPDATE
    )

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json_dict(self) -> dict:
        return convert_to_json_serializable(data=self.to_dict())


DEFAULT_RECONCILATION_DIRECTIVES = ReconciliationDirectives(
    variables=ReconciliationStrategy.UPDATE,
    domain_builder=ReconciliationStrategy.UPDATE,
    parameter_builder=ReconciliationStrategy.UPDATE,
    expectation_configuration_builder=ReconciliationStrategy.UPDATE,
)


def reconcile_rule_variables(
    variables: ParameterContainer,
    variables_config: dict,
    reconciliation_strategy: ReconciliationStrategy = DEFAULT_RECONCILATION_DIRECTIVES.variables,
) -> dict:
    """
    Rule "variables" reconciliation involves combining the variables, instantiated from Rule configuration
    (e.g., stored in a YAML file managed by the Profiler store), with the variables override, possibly supplied
    as part of the candiate override rule configuration.

    The reconciliation logic for "variables" is of the "replace" nature: An override value complements the
    original on key "miss", and replaces the original on key "hit" (or "collision"), because "variables" is a
    unique member for a Rule.

    :param variables: existing variables of a Rule
    :param variables_config: variables configuration override, supplied in dictionary (configuration) form
    :param reconciliation_strategy: one of update, nested_update, or overwrite ways of reconciling overwrites
    :return: reconciled variables configuration, returned in dictionary (configuration) form
    """
    effective_variables_config: dict = convert_variables_to_dict(variables=variables)
    if variables_config:
        if reconciliation_strategy == ReconciliationStrategy.NESTED_UPDATE:
            effective_variables_config = nested_update(
                effective_variables_config,
                variables_config,
            )
        elif reconciliation_strategy == ReconciliationStrategy.REPLACE:
            effective_variables_config = variables_config
        elif reconciliation_strategy == ReconciliationStrategy.UPDATE:
            effective_variables_config.update(variables_config)

    return effective_variables_config
