import logging
from dataclasses import asdict, dataclass
from enum import Enum

from great_expectations.core.util import convert_to_json_serializable, nested_update
from great_expectations.rule_based_profiler.helpers.util import (
    convert_variables_to_dict,
)
from great_expectations.rule_based_profiler.types import ParameterContainer
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
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return asdict(self)

    def to_json_dict(self) -> dict:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return convert_to_json_serializable(data=self.to_dict())


DEFAULT_RECONCILATION_DIRECTIVES: ReconciliationDirectives = ReconciliationDirectives(
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
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    '\n    Rule "variables" reconciliation involves combining the variables, instantiated from Rule configuration\n    (e.g., stored in a YAML file managed by the Profiler store), with the variables override, possibly supplied\n    as part of the candiate override rule configuration.\n\n    The reconciliation logic for "variables" is of the "replace" nature: An override value complements the\n    original on key "miss", and replaces the original on key "hit" (or "collision"), because "variables" is a\n    unique member for a Rule.\n\n    :param variables: existing variables of a Rule\n    :param variables_config: variables configuration override, supplied in dictionary (configuration) form\n    :param reconciliation_strategy: one of update, nested_update, or overwrite ways of reconciling overwrites\n    :return: reconciled variables configuration, returned in dictionary (configuration) form\n    '
    effective_variables_config: dict = convert_variables_to_dict(variables=variables)
    if variables_config:
        if reconciliation_strategy == ReconciliationStrategy.NESTED_UPDATE:
            effective_variables_config = nested_update(
                effective_variables_config, variables_config
            )
        elif reconciliation_strategy == ReconciliationStrategy.REPLACE:
            effective_variables_config = variables_config
        elif reconciliation_strategy == ReconciliationStrategy.UPDATE:
            effective_variables_config.update(variables_config)
    return effective_variables_config
