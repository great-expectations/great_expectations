import logging
from typing import List, Type

logger = logging.getLogger(__name__)

_registered_expectations = dict()


def register_expectation(expectation: Type["Expectation"]) -> None:
    expectation_type = expectation.expectation_type
    if expectation_type in _registered_expectations:
        if _registered_expectations[expectation_type] == expectation:
            logger.info(
                "Multiple declarations of expectation " + expectation_type + " found."
            )
            return
        else:
            logger.warning(
                "Overwriting declaration of expectation " + expectation_type + "."
            )
    logger.debug("Registering expectation: " + expectation_type)
    _registered_expectations[expectation_type] = expectation


def get_expectation_impl(expectation_name):
    return _registered_expectations.get(expectation_name)


def list_registered_expectation_implementations(
    expectation_root: Type["Expectation"],
) -> List[str]:
    registered_expectation_implementations = []
    for (
        expectation_name,
        expectation_implementation,
    ) in _registered_expectations.items():
        if issubclass(expectation_implementation, expectation_root):
            registered_expectation_implementations.append(expectation_name)

    return registered_expectation_implementations
