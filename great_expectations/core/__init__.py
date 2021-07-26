import logging

from .expectation_suite import (
    ExpectationConfiguration,
    ExpectationConfigurationSchema,
    ExpectationSuite,
    ExpectationSuiteSchema,
    expectationSuiteSchema,
)
from .expectation_validation_result import (
    ExpectationSuiteValidationResult,
    ExpectationSuiteValidationResultSchema,
    ExpectationValidationResult,
    ExpectationValidationResultSchema,
    expectationSuiteValidationResultSchema,
    expectationValidationResultSchema,
    get_metric_kwargs_id,
)
from .id_dict import IDDict
from .run_identifier import RunIdentifier, RunIdentifierSchema
from .urn import ge_urn

__all__ = [
    ExpectationConfiguration,
    ExpectationConfigurationSchema,
    ExpectationSuite,
    ExpectationSuiteSchema,
    expectationSuiteSchema,
    ExpectationSuiteValidationResult,
    ExpectationSuiteValidationResultSchema,
    ExpectationValidationResult,
    ExpectationValidationResultSchema,
    expectationSuiteValidationResultSchema,
    expectationValidationResultSchema,
    get_metric_kwargs_id,
    IDDict,
    RunIdentifier,
    RunIdentifierSchema,
    ge_urn,
]

# <WILL> this is the marker
# logging.basicConfig(
#     format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
#     level=logging.DEBUG
# )

logger = logging.getLogger(__name__)

RESULT_FORMATS = ["BOOLEAN_ONLY", "BASIC", "COMPLETE", "SUMMARY"]
