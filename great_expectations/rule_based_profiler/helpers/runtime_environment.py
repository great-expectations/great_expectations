import logging
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.rule_based_profiler.helpers.util import (
    convert_variables_to_dict,
)
from great_expectations.rule_based_profiler.rule import Rule
from great_expectations.types import SerializableDictDot

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclass
class RuntimeEnvironmentVariablesDirectives(SerializableDictDot):
    rule_name: str
    variables: Optional[Dict[str, Any]] = None

    def to_dict(self) -> dict:
        """
        Returns dictionary equivalent of this object.
        """
        return asdict(self)

    def to_json_dict(self) -> dict:
        """
        Returns JSON dictionary equivalent of this object.
        """
        return convert_to_json_serializable(data=self.to_dict())


@dataclass
class RuntimeEnvironmentDomainTypeDirectives(SerializableDictDot):
    domain_type: MetricDomainTypes
    directives: Dict[str, Any]

    def to_dict(self) -> dict:
        """
        Returns dictionary equivalent of this object.
        """
        return asdict(self)

    def to_json_dict(self) -> dict:
        """
        Returns JSON dictionary equivalent of this object.
        """
        return convert_to_json_serializable(data=self.to_dict())


def build_variables_directives(
    exact_estimation: bool,
    rules: List[Rule],
    **kwargs: dict,
) -> List[RuntimeEnvironmentVariablesDirectives]:
    """
    This method makes best-effort attempt to identify directives, supplied in "kwargs", as "variables", referenced by
    components of "Rule" objects, identified by respective "rule_name" property as indicated, and return each of these
    directives as part of dedicated "RuntimeEnvironmentVariablesDirectives" typed object for every "rule_name" (string).
    """
    # Implementation relies on assumption that "kwargs" contains "variables"-level arguments/directives only.
    directives: Dict[
        str, Dict[str, Any]
    ]  # key is "rule_name"; value is "variables" in corresponding "Rule" object
    if exact_estimation:
        directives = {}
        rule_variables_configs: Optional[Dict[str, Any]]
        rule: Rule
        for rule in rules:
            rule_variables_configs = convert_variables_to_dict(variables=rule.variables)
            if rule.name in kwargs:
                rule_variables_configs.update(kwargs[rule.name])

            # Since "exact_estimation" is True, "estimator" value of "exact" must be set on "variables" of every "Rule".
            rule_variables_configs.update(
                {
                    "estimator": "exact",
                }
            )

            directives[rule.name] = rule_variables_configs
    else:
        directives = kwargs

    # Convert "kwargs" ("dict"-typed) directives into interpretable "RuntimeEnvironmentVariablesDirectives" "Enum" type.
    rule_name: str
    return [
        RuntimeEnvironmentVariablesDirectives(
            rule_name=rule_name,
            variables=variables,
        )
        for rule_name, variables in directives.items()
    ]


def build_domain_type_directives(
    **kwargs: dict,
) -> List[RuntimeEnvironmentDomainTypeDirectives]:
    """
    This method makes best-effort attempt to identify directives, supplied in "kwargs", as supported properties,
    corresponnding to "DomainBuilder" classes, associated with every "MetricDomainTypes", and return each of these
    directives as part of dedicated "RuntimeEnvironmentDomainTypeDirectives" typed object for every "MetricDomainTypes".
    """
    # Implementation relies on assumption that "kwargs" contains "Domain"-level arguments/directives only.
    """
    Currently, only "column_domain_type_directives" are supported; in the future, other "Domain" type directives could
    be envisioned as consideration for support (e.g., "table_domain_type_directives").  To underscore this reasoning,
    "domain_type_directives_list" is declared as "List" and a single "RuntimeEnvironmentDomainTypeDirectives" element
    is appended, instead of setting "domain_type_directives_list" to contain that element explicitly.
    """
    domain_type_directives_list: List[RuntimeEnvironmentDomainTypeDirectives] = []

    column_domain_type_directives: RuntimeEnvironmentDomainTypeDirectives = (
        RuntimeEnvironmentDomainTypeDirectives(
            domain_type=MetricDomainTypes.COLUMN,
            directives=kwargs,
        )
    )
    domain_type_directives_list.append(column_domain_type_directives)

    return domain_type_directives_list
