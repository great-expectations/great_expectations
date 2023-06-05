from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Dict, Optional, TypeVar, Union

from great_expectations.core.id_dict import IDDict
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.types import SerializableDictDot, SerializableDotDict
from great_expectations.util import (
    deep_filter_properties_iterable,
    is_candidate_subset_of_target,
)

INFERRED_SEMANTIC_TYPE_KEY: str = "inferred_semantic_domain_type"

T = TypeVar("T")


class SemanticDomainTypes(Enum):
    NUMERIC = "numeric"
    TEXT = "text"
    LOGIC = "logic"
    DATETIME = "datetime"
    BINARY = "binary"
    CURRENCY = "currency"
    IDENTIFIER = "identifier"
    MISCELLANEOUS = "miscellaneous"
    UNKNOWN = "unknown"


@dataclass
class InferredSemanticDomainType(SerializableDictDot):
    semantic_domain_type: Optional[Union[str, SemanticDomainTypes]] = None
    details: Optional[Dict[str, Any]] = None

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json_dict(self) -> dict:
        return convert_to_json_serializable(data=self.to_dict())


class DomainKwargs(SerializableDotDict):
    def to_dict(self) -> dict:
        return dict(self)

    def to_json_dict(self) -> dict:
        return convert_to_json_serializable(data=self.to_dict())


class Domain(SerializableDotDict):
    # Adding an explicit constructor to highlight the specific properties that will be used.
    def __init__(
        self,
        domain_type: Union[str, MetricDomainTypes],
        domain_kwargs: Optional[Union[Dict[str, Any], DomainKwargs]] = None,
        details: Optional[Dict[str, Any]] = None,
        rule_name: Optional[str] = None,
    ) -> None:
        if isinstance(domain_type, str):
            try:
                domain_type = MetricDomainTypes(domain_type.lower())
            except (TypeError, KeyError) as e:
                raise ValueError(
                    f""" {e}: Cannot instantiate Domain (domain_type "{str(domain_type)}" of type \
"{str(type(domain_type))}" is not supported).
"""
                )
        elif not isinstance(domain_type, MetricDomainTypes):
            raise ValueError(
                f"""Cannot instantiate Domain (domain_type "{str(domain_type)}" of type "{str(type(domain_type))}" is \
not supported).
"""
            )

        if domain_kwargs is None:
            domain_kwargs = DomainKwargs({})
        elif isinstance(domain_kwargs, dict):
            domain_kwargs = DomainKwargs(domain_kwargs)

        domain_kwargs_dot_dict: DomainKwargs = (
            deep_convert_properties_iterable_to_domain_kwargs(source=domain_kwargs)
        )

        if details is None:
            details = {}

        inferred_semantic_domain_type: Optional[
            Dict[str, Union[str, SemanticDomainTypes]]
        ] = details.get(INFERRED_SEMANTIC_TYPE_KEY)
        if inferred_semantic_domain_type:
            semantic_domain_key: str
            metric_domain_key: str
            metric_domain_value: Any
            is_consistent: bool
            for semantic_domain_key in inferred_semantic_domain_type:
                is_consistent = False
                for (
                    metric_domain_key,
                    metric_domain_value,
                ) in domain_kwargs_dot_dict.items():
                    if (
                        isinstance(metric_domain_value, (list, set, tuple))
                        and semantic_domain_key in metric_domain_value
                    ) or (semantic_domain_key == metric_domain_value):
                        is_consistent = True
                        break

                if not is_consistent:
                    raise ValueError(
                        f"""Cannot instantiate Domain (domain_type "{str(domain_type)}" of type \
"{str(type(domain_type))}" -- key "{semantic_domain_key}", detected in "{INFERRED_SEMANTIC_TYPE_KEY}" dictionary, does \
not exist as value of appropriate key in "domain_kwargs" dictionary.
"""
                    )

        super().__init__(
            domain_type=domain_type,
            domain_kwargs=domain_kwargs_dot_dict,
            details=details,
            rule_name=rule_name,
        )

    def __repr__(self):
        return json.dumps(self.to_json_dict(), indent=2)

    def __str__(self):
        return self.__repr__()

    def __eq__(self, other):
        return (other is not None) and (
            (
                hasattr(other, "to_json_dict")
                and self.to_json_dict() == other.to_json_dict()
            )
            or (
                isinstance(other, dict)
                and deep_filter_properties_iterable(
                    properties=self.to_json_dict(), clean_falsy=True
                )
                == deep_filter_properties_iterable(properties=other, clean_falsy=True)
            )
            or (self.__str__() == str(other))
        )

    def __ne__(self, other):
        return not self.__eq__(other=other)

    def __hash__(self) -> int:  # type: ignore[override]
        """Overrides the default implementation"""
        _result_hash: int = hash(self.id)
        return _result_hash

    def is_superset(self, other: Domain) -> bool:
        """Determines if other "Domain" object (provided as argument) is contained within this "Domain" object."""
        if other is None:
            return True

        return other.is_subset(other=self)

    def is_subset(self, other: Domain) -> bool:
        """Determines if this "Domain" object is contained within other "Domain" object (provided as argument)."""
        if other is None:
            return False

        this_json_dict: dict = self.to_json_dict()
        other_json_dict: dict = other.to_json_dict()

        return is_candidate_subset_of_target(
            candidate=this_json_dict, target=other_json_dict
        )

    # Adding this property for convenience (also, in the future, arguments may not be all set to their default values).
    @property
    def id(self) -> str:
        return IDDict(self.to_json_dict()).to_id()

    def to_json_dict(self) -> dict:
        details: dict = {}

        key: str
        value: Any
        for key, value in self["details"].items():
            if value:
                if key == INFERRED_SEMANTIC_TYPE_KEY:
                    column_name: str
                    semantic_type: Union[str, SemanticDomainTypes]
                    value = {  # noqa: PLW2901
                        column_name: SemanticDomainTypes(semantic_type.lower()).value
                        if isinstance(semantic_type, str)
                        else semantic_type.value
                        for column_name, semantic_type in value.items()
                    }

            details[key] = convert_to_json_serializable(data=value)

        json_dict: dict = {
            "domain_type": self["domain_type"].value,
            "domain_kwargs": self["domain_kwargs"].to_json_dict(),
            "details": details,
            "rule_name": self["rule_name"],
        }
        json_dict = convert_to_json_serializable(data=json_dict)

        return deep_filter_properties_iterable(properties=json_dict, clean_falsy=True)


def deep_convert_properties_iterable_to_domain_kwargs(
    source: Union[T, dict]
) -> Union[T, DomainKwargs]:
    if isinstance(source, dict):
        return _deep_convert_properties_iterable_to_domain_kwargs(
            source=DomainKwargs(source)
        )

    # Must allow for non-dictionary source types, since their internal nested structures may contain dictionaries.
    if isinstance(source, (list, set, tuple)):
        data_type: type = type(source)

        element: Any
        return data_type(
            [
                deep_convert_properties_iterable_to_domain_kwargs(source=element)
                for element in source
            ]
        )

    return source


def _deep_convert_properties_iterable_to_domain_kwargs(source: dict) -> DomainKwargs:
    key: str
    value: Any
    for key, value in source.items():
        if isinstance(value, dict):
            source[key] = _deep_convert_properties_iterable_to_domain_kwargs(
                source=value
            )
        elif isinstance(value, (list, set, tuple)):
            data_type: type = type(value)

            element: Any
            source[key] = data_type(
                [
                    deep_convert_properties_iterable_to_domain_kwargs(source=element)
                    for element in value
                ]
            )

    return DomainKwargs(source)
