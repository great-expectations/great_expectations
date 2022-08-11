import logging
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.util import convert_to_json_serializable
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
    exact_estimation: bool = True,
    **kwargs: dict,
) -> List[RuntimeEnvironmentVariablesDirectives]:
    """
    This method makes best-effort attempt to identify directives, supplied in "kwargs", as "variables", referenced by
    components of "Rule" objects, identified by respective "rule_name" property as indicated, and return each of these
    directives as part of dedicated "RuntimeEnvironmentVariablesDirectives" typed object for every "rule_name" (string).
    """
    runtime_environment_variables_directives_list: List[
        RuntimeEnvironmentVariablesDirectives
    ] = []

    rule_name: str
    variables: Optional[Dict[str, Any]]
    for rule_name, variables in kwargs.items():
        if exact_estimation:
            variables.update(
                {
                    "estimator": "exact",
                }
            )

        runtime_environment_variables_directives_list.append(
            RuntimeEnvironmentVariablesDirectives(
                rule_name=rule_name,
                variables=variables,
            )
        )

    return runtime_environment_variables_directives_list


def build_domain_type_directives(
    **kwargs: dict,
) -> List[RuntimeEnvironmentDomainTypeDirectives]:
    """
    This method makes best-effort attempt to identify directives, supplied in "kwargs", as supported properties,
    corresponnding to "DomainBuilder" classes, associated with every "MetricDomainTypes", and return each of these
    directives as part of dedicated "RuntimeEnvironmentDomainTypeDirectives" typed object for every "MetricDomainTypes".
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
