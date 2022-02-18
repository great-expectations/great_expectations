import logging
from abc import ABC, abstractmethod
from typing import Dict, Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.rule_based_profiler.types import (
    Builder,
    Domain,
    ParameterContainer,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ExpectationConfigurationBuilder(Builder, ABC):
    def __init__(self, expectation_type: str, **kwargs):
        self._expectation_type = expectation_type

        """
        Since ExpectationConfigurationBuilderConfigSchema allow arbitrary fields (as ExpectationConfiguration kwargs)
        to be provided, they must be all converted to public property accessors and/or public fields in order for all
        provisions by Builder, SerializableDictDot, and DictDot to operate properly in compliance with their interfaces.
        """
        for k, v in kwargs.items():
            setattr(self, k, v)
            logger.debug(
                'Setting unknown kwarg (%s, %s) provided to constructor as argument in "%s".',
                k,
                v,
                self.__class__.__name__,
            )

    def build_expectation_configuration(
        self,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> ExpectationConfiguration:
        return self._build_expectation_configuration(
            domain=domain, variables=variables, parameters=parameters
        )

    @abstractmethod
    def _build_expectation_configuration(
        self,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> ExpectationConfiguration:
        pass

    @property
    def expectation_type(self) -> str:
        return self._expectation_type
