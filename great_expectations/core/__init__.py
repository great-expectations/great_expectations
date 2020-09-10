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

logger = logging.getLogger(__name__)

RESULT_FORMATS = ["BOOLEAN_ONLY", "BASIC", "COMPLETE", "SUMMARY"]

# TODO: re-enable once we can allow arbitrary keys but still add this sort of validation
# class MetaDictSchema(Schema):
#     """The MetaDict """
#
#     # noinspection PyUnusedLocal
#     @validates_schema
#     def validate_json_serializable(self, data, **kwargs):
#         import json
#         try:
#             json.dumps(data)
#         except (TypeError, OverflowError):
#             raise ValidationError("meta information must be json serializable.")
